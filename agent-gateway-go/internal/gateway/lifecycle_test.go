package gateway

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// finalizeRecorder stands in for LobeHub's finalize-abandoned endpoint and
// records the reason of every call it receives.
type finalizeRecorder struct {
	mu      sync.Mutex
	reasons []string
	// statusFor returns the HTTP status for the nth call (0-based).
	statusFor func(call int) int
	srv       *httptest.Server
}

func newFinalizeRecorder(t *testing.T, statusFor func(call int) int) *finalizeRecorder {
	t.Helper()
	rec := &finalizeRecorder{statusFor: statusFor}
	rec.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/agent/finalize-abandoned" {
			t.Errorf("unexpected finalize path: %s", r.URL.Path)
		}
		var body struct {
			OperationID string `json:"operationId"`
			Reason      string `json:"reason"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		rec.mu.Lock()
		call := len(rec.reasons)
		rec.reasons = append(rec.reasons, body.Reason)
		rec.mu.Unlock()
		status := http.StatusOK
		if rec.statusFor != nil {
			status = rec.statusFor(call)
		}
		if status != http.StatusOK {
			http.Error(w, "finalize unavailable", status)
			return
		}
		writeTestJSON(t, w, map[string]any{"abandoned": true, "finalized": true, "found": true})
	}))
	t.Cleanup(rec.srv.Close)
	return rec
}

func (r *finalizeRecorder) calls() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.reasons...)
}

// syncBuffer is a bytes.Buffer safe to write from timer goroutines.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type lifecycleHarness struct {
	finalize *finalizeRecorder
	http     *httptest.Server
	logs     *syncBuffer
	srv      *Server
}

func newLifecycleHarness(t *testing.T, statusFor func(call int) int) *lifecycleHarness {
	t.Helper()
	finalize := newFinalizeRecorder(t, statusFor)
	srv := NewServer(Config{ServiceToken: "service-token", LobeAPIBaseURL: finalize.srv.URL})
	srv.cleanupDelay = 50 * time.Millisecond
	logs := &syncBuffer{}
	srv.logger = slog.New(slog.NewTextHandler(logs, nil))
	httpSrv := httptest.NewServer(srv.Routes())
	t.Cleanup(httpSrv.Close)
	return &lifecycleHarness{finalize: finalize, http: httpSrv, logs: logs, srv: srv}
}

func (h *lifecycleHarness) init(t *testing.T, operationID string) *operation {
	t.Helper()
	postJSON(t, h.http.URL+"/api/operations/init", map[string]any{"operationId": operationID, "userId": "user-1"}, http.StatusOK)
	op := h.srv.getOperation(operationID)
	if op == nil {
		t.Fatalf("operation %s not registered", operationID)
	}
	t.Cleanup(func() {
		op.mu.Lock()
		op.stopWatchdogLocked()
		op.stopCleanupLocked()
		op.mu.Unlock()
	})
	return op
}

// push delivers one event on channelID's channel. eventOperationID is the
// operation the event names: the channel itself, or a mirrored member.
func (h *lifecycleHarness) push(t *testing.T, channelID string, eventOperationID string, eventType string, data map[string]any) {
	t.Helper()
	postJSON(t, h.http.URL+"/api/operations/push-event", map[string]any{
		"event": map[string]any{
			"data": data, "operationId": eventOperationID, "stepIndex": 0,
			"timestamp": time.Now().UnixMilli(), "type": eventType,
		},
		"operationId": channelID,
	}, http.StatusOK)
}

func (h *lifecycleHarness) waitRemoved(t *testing.T, operationID string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for h.srv.getOperation(operationID) != nil {
		if time.Now().After(deadline) {
			t.Fatalf("operation %s was never cleaned up", operationID)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// expireIdleWindow makes the operation look silent past its watchdog window
// and fires the watchdog, as its timer would.
func expireIdleWindow(op *operation) {
	op.mu.Lock()
	op.lastEventAt = time.Now().Add(-op.watchdogTimeoutLocked() - time.Minute)
	op.mu.Unlock()
	op.fireWatchdog()
}

func opState(op *operation) (SessionStatus, bool, bool) {
	op.mu.RLock()
	defer op.mu.RUnlock()
	return op.record.Status, op.watchdog != nil, op.cleanupTimer != nil
}

func assertReasons(t *testing.T, got []string, want ...string) {
	t.Helper()
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("finalize-abandoned reasons = %v, want %v", got, want)
	}
}

func TestCleanupDeliversWatchdogVerdictLobeHubNeverAcknowledged(t *testing.T) {
	// The watchdog's own call fails; the cleanup retry gets through.
	h := newLifecycleHarness(t, func(call int) int {
		if call == 0 {
			return http.StatusBadGateway
		}
		return http.StatusOK
	})
	op := h.init(t, "op-owed")

	expireIdleWindow(op)
	if status, _, _ := opState(op); status != StatusError {
		t.Fatalf("expected error status after an unacknowledged verdict, got %s", status)
	}
	if !strings.Contains(h.logs.String(), "level=WARN") || !strings.Contains(h.logs.String(), "op-owed") {
		t.Fatalf("expected a warning for the failed watchdog call, got %q", h.logs.String())
	}

	h.waitRemoved(t, "op-owed")
	assertReasons(t, h.finalize.calls(), finalizeReasonWatchdog, finalizeReasonCleanup)
	if strings.Contains(h.logs.String(), "level=ERROR") {
		t.Fatalf("a successful retry must not log an error: %q", h.logs.String())
	}
}

func TestCleanupLogsFinalizeItCouldNotDeliver(t *testing.T) {
	h := newLifecycleHarness(t, func(int) int { return http.StatusInternalServerError })
	op := h.init(t, "op-unreachable")

	expireIdleWindow(op)
	h.waitRemoved(t, "op-unreachable")

	assertReasons(t, h.finalize.calls(), finalizeReasonWatchdog, finalizeReasonCleanup)
	logs := h.logs.String()
	for _, want := range []string{"level=ERROR", "operationId=op-unreachable", "reason=gateway_cleanup", "500"} {
		if !strings.Contains(logs, want) {
			t.Fatalf("cleanup failure log missing %q: %q", want, logs)
		}
	}
}

func TestCleanupDoesNotFinalizeSettledOperations(t *testing.T) {
	cases := []struct {
		name string
		end  func(t *testing.T, h *lifecycleHarness, op *operation)
		want []string
	}{
		{
			name: "run finished",
			end: func(t *testing.T, h *lifecycleHarness, _ *operation) {
				h.push(t, "op-settled", "op-settled", "agent_runtime_end", map[string]any{"reason": "done"})
			},
		},
		{
			name: "run failed",
			end: func(t *testing.T, h *lifecycleHarness, _ *operation) {
				h.push(t, "op-settled", "op-settled", "agent_runtime_end", map[string]any{"reason": "error"})
			},
		},
		{
			name: "end without reason",
			end: func(t *testing.T, h *lifecycleHarness, _ *operation) {
				h.push(t, "op-settled", "op-settled", "agent_runtime_end", map[string]any{})
			},
		},
		{
			name: "backend status",
			end: func(t *testing.T, h *lifecycleHarness, _ *operation) {
				postJSON(t, h.http.URL+"/api/operations/update-status", map[string]any{"operationId": "op-settled", "status": "interrupted"}, http.StatusOK)
			},
		},
		{
			name: "acknowledged watchdog verdict",
			end: func(_ *testing.T, _ *lifecycleHarness, op *operation) {
				expireIdleWindow(op)
			},
			want: []string{finalizeReasonWatchdog},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newLifecycleHarness(t, nil)
			op := h.init(t, "op-settled")
			h.push(t, "op-settled", "op-settled", "step_start", nil)

			tc.end(t, h, op)
			h.waitRemoved(t, "op-settled")

			assertReasons(t, h.finalize.calls(), tc.want...)
		})
	}
}

// The step gap from lobehub-gateway#12, as a headless group run hits it: a
// member's terminal is mirrored verbatim onto the supervisor's channel (no
// client declared member_runtime_end), the supervisor resumes after the
// member barrier under its own operationId, and then dies between steps. The
// supervisor must stay watched instead of being dropped by the cleanup the
// mirrored end scheduled.
func TestSupervisorResumedAfterMirroredMemberEndStaysWatched(t *testing.T) {
	h := newLifecycleHarness(t, nil)
	op := h.init(t, "op-supervisor")
	h.push(t, "op-supervisor", "op-supervisor", "step_start", nil)
	h.push(t, "op-supervisor", "op-member", "agent_runtime_end", map[string]any{"reason": "done"})

	if status, watched, cleaning := opState(op); status != StatusCompleted || !watched || cleaning {
		t.Fatalf("a mirrored end ends the session but keeps the run watched: status=%s watchdog=%v cleanup=%v", status, watched, cleaning)
	}

	// The supervisor's next step.
	h.push(t, "op-supervisor", "op-supervisor", "step_start", nil)
	if status, watched, cleaning := opState(op); status != StatusRunning || !watched || cleaning {
		t.Fatalf("resumed supervisor must be live and watched: status=%s watchdog=%v cleanup=%v", status, watched, cleaning)
	}

	// Well past the cleanup delay the mirrored end scheduled, it is still known.
	time.Sleep(4 * h.srv.cleanupDelay)
	if h.srv.getOperation("op-supervisor") != op {
		t.Fatal("cleanup dropped a running supervisor")
	}

	// It dies between steps: the watchdog reports it, exactly once.
	expireIdleWindow(op)
	assertReasons(t, h.finalize.calls(), finalizeReasonWatchdog)
	h.waitRemoved(t, "op-supervisor")
	assertReasons(t, h.finalize.calls(), finalizeReasonWatchdog)
}

func TestMirroredEndIsNotReopenedByBookkeepingEvents(t *testing.T) {
	h := newLifecycleHarness(t, nil)
	op := h.init(t, "op-quiet")
	h.push(t, "op-quiet", "op-member", "agent_runtime_end", map[string]any{"reason": "done"})

	h.push(t, "op-quiet", "op-quiet", "message_patch", map[string]any{"revision": 2})
	h.push(t, "op-quiet", "op-quiet", "notify_update", nil)
	h.push(t, "op-quiet", "op-member", "member_runtime_end", map[string]any{"reason": "done"})

	if status, watched, _ := opState(op); status != StatusCompleted || !watched {
		t.Fatalf("bookkeeping must not reopen the operation or stop its watchdog: status=%s watchdog=%v", status, watched)
	}
	expireIdleWindow(op)
	h.waitRemoved(t, "op-quiet")
	assertReasons(t, h.finalize.calls(), finalizeReasonWatchdog)
}

func TestFinishedRunIsNotReopenedByLateEvents(t *testing.T) {
	for _, reason := range []string{"done", "waiting_for_human"} {
		t.Run(reason, func(t *testing.T) {
			h := newLifecycleHarness(t, nil)
			op := h.init(t, "op-done")
			h.push(t, "op-done", "op-done", "agent_runtime_end", map[string]any{"reason": reason})
			h.push(t, "op-done", "op-done", "step_start", nil)
			h.push(t, "op-done", "op-member", "stream_start", nil)

			if status, watched, _ := opState(op); status != StatusCompleted || watched {
				t.Fatalf("ended run reopened: status=%s watchdog=%v", status, watched)
			}
			h.waitRemoved(t, "op-done")
			assertReasons(t, h.finalize.calls())
		})
	}
}

func TestMirroredMemberEndStillCompletesClientSession(t *testing.T) {
	h := newLifecycleHarness(t, nil)
	h.init(t, "op-supervisor")
	ws := dialWebSocket(t, h.http.URL, "/ws?operationId=op-supervisor")
	defer ws.close()
	ws.writeJSON(t, map[string]any{"type": "auth", "token": "service-token"})
	_ = ws.readJSON(t)

	// Clients still get the mirrored end and the session_complete they always
	// got for it; only the gateway's own bookkeeping changed.
	h.push(t, "op-supervisor", "op-member", "agent_runtime_end", map[string]any{"reason": "done"})
	if msg := ws.readJSON(t); msg["type"] != "agent_event" {
		t.Fatalf("expected the mirrored end, got %+v", msg)
	}
	if msg := ws.readJSON(t); msg["type"] != "session_complete" {
		t.Fatalf("expected session_complete, got %+v", msg)
	}
}

func TestToolAndLLMRequestsReopenSupervisor(t *testing.T) {
	t.Run("tool execute", func(t *testing.T) {
		h := newLifecycleHarness(t, nil)
		op := h.init(t, "op-tool")
		h.push(t, "op-tool", "op-member", "agent_runtime_end", map[string]any{"reason": "done"})
		postJSON(t, h.http.URL+"/api/operations/tool-execute", map[string]any{"operationId": "op-tool", "data": map[string]any{"toolCallId": "call-1"}}, http.StatusOK)
		if status, watched, cleaning := opState(op); status != StatusRunning || !watched || cleaning {
			t.Fatalf("tool execute must reopen: status=%s watchdog=%v cleanup=%v", status, watched, cleaning)
		}
	})
	t.Run("llm execute", func(t *testing.T) {
		h := newLifecycleHarness(t, nil)
		op := h.init(t, "op-llm")
		h.push(t, "op-llm", "op-member", "agent_runtime_end", map[string]any{"reason": "done"})
		postJSON(t, h.http.URL+"/api/operations/llm-execute", map[string]any{"operationId": "op-llm", "data": map[string]any{"callId": "call-1"}}, http.StatusOK)
		if status, watched, cleaning := opState(op); status != StatusRunning || !watched || cleaning {
			t.Fatalf("llm execute must reopen: status=%s watchdog=%v cleanup=%v", status, watched, cleaning)
		}
	})
}

func TestCleanupFiringAfterReviveKeepsOperation(t *testing.T) {
	h := newLifecycleHarness(t, nil)
	op := h.init(t, "op-race")
	h.push(t, "op-race", "op-member", "agent_runtime_end", map[string]any{"reason": "done"})
	h.push(t, "op-race", "op-race", "step_start", nil)

	// The cleanup timer had already fired and waited for the lock while the
	// revive ran; its late call must leave the live run alone.
	op.cleanup()

	if h.srv.getOperation("op-race") != op {
		t.Fatal("stale cleanup dropped a revived run")
	}
	if status, watched, _ := opState(op); status != StatusRunning || !watched {
		t.Fatalf("revived run lost its watchdog: status=%s watchdog=%v", status, watched)
	}
	assertReasons(t, h.finalize.calls())
}

func TestRunEndSettles(t *testing.T) {
	cases := []struct {
		ended string
		want  bool
	}{
		{ended: "op", want: true},
		{ended: "", want: true},
		{ended: "member", want: false},
	}
	for _, tc := range cases {
		if got := runEndSettles("op", tc.ended); got != tc.want {
			t.Errorf("runEndSettles(op, %q) = %v, want %v", tc.ended, got, tc.want)
		}
	}
}

// The step gap at its narrowest: the channel's own run dies right after a
// mirrored member end, before any later step could reopen the operation.
func TestQuietMirroredEndIsFinalizedAfterIdleWindow(t *testing.T) {
	h := newLifecycleHarness(t, nil)
	op := h.init(t, "op-quiet-end")
	h.push(t, "op-quiet-end", "op-member", "agent_runtime_end", map[string]any{"reason": "done"})

	// Past the cleanup delay it is still known, watched and not yet judged:
	// the 30-minute idle window is unchanged.
	time.Sleep(4 * h.srv.cleanupDelay)
	if h.srv.getOperation("op-quiet-end") != op {
		t.Fatal("cleanup dropped a run that only saw a mirrored end")
	}
	if status, watched, cleaning := opState(op); status != StatusCompleted || !watched || cleaning {
		t.Fatalf("quiet mirrored end must stay watched: status=%s watchdog=%v cleanup=%v", status, watched, cleaning)
	}
	op.fireWatchdog()
	assertReasons(t, h.finalize.calls())

	expireIdleWindow(op)
	h.waitRemoved(t, "op-quiet-end")
	assertReasons(t, h.finalize.calls(), finalizeReasonWatchdog)
	if status, _, _ := opState(op); status != StatusCompleted {
		t.Fatalf("clients already got session_complete; status must not change, got %s", status)
	}
}

func TestQuietMirroredEndRetriesUnacknowledgedVerdict(t *testing.T) {
	h := newLifecycleHarness(t, func(call int) int {
		if call == 0 {
			return http.StatusBadGateway
		}
		return http.StatusOK
	})
	op := h.init(t, "op-quiet-owed")
	h.push(t, "op-quiet-owed", "op-member", "agent_runtime_end", map[string]any{"reason": "done"})

	expireIdleWindow(op)
	h.waitRemoved(t, "op-quiet-owed")

	assertReasons(t, h.finalize.calls(), finalizeReasonWatchdog, finalizeReasonCleanup)
	if !strings.Contains(h.logs.String(), "level=WARN") {
		t.Fatalf("expected a warning for the failed watchdog call, got %q", h.logs.String())
	}
}

func TestLateMirroredEndDoesNotUnsettleFinishedRun(t *testing.T) {
	h := newLifecycleHarness(t, nil)
	op := h.init(t, "op-finished")
	h.push(t, "op-finished", "op-finished", "agent_runtime_end", map[string]any{"reason": "done"})
	h.push(t, "op-finished", "op-member", "agent_runtime_end", map[string]any{"reason": "done"})
	h.push(t, "op-finished", "op-finished", "step_start", nil)

	if status, watched, _ := opState(op); status != StatusCompleted || watched {
		t.Fatalf("finished run reopened by a late mirrored end: status=%s watchdog=%v", status, watched)
	}
	h.waitRemoved(t, "op-finished")
	assertReasons(t, h.finalize.calls())
}

func TestMirroredEndKeepsOwedVerdict(t *testing.T) {
	h := newLifecycleHarness(t, func(call int) int {
		if call == 0 {
			return http.StatusBadGateway
		}
		return http.StatusOK
	})
	h.srv.cleanupDelay = 300 * time.Millisecond
	op := h.init(t, "op-owed-mirror")

	expireIdleWindow(op)
	h.push(t, "op-owed-mirror", "op-member", "agent_runtime_end", map[string]any{"reason": "done"})

	h.waitRemoved(t, "op-owed-mirror")
	assertReasons(t, h.finalize.calls(), finalizeReasonWatchdog, finalizeReasonCleanup)
}

func TestRetiredOperationIsNotRevived(t *testing.T) {
	h := newLifecycleHarness(t, nil)
	op := h.init(t, "op-retired")
	h.push(t, "op-retired", "op-member", "agent_runtime_end", map[string]any{"reason": "done"})

	op.mu.Lock()
	op.expired = true
	op.stopWatchdogLocked()
	revived := op.reviveLocked()
	op.scheduleWatchdogLocked()
	watched := op.watchdog != nil
	status := op.record.Status
	op.mu.Unlock()

	if revived || watched || status != StatusCompleted {
		t.Fatalf("retired operation came back: revived=%v watchdog=%v status=%s", revived, watched, status)
	}
}

// A late v1 client replays a run that went on past a mirrored end. It must
// reach the run's real end instead of stopping at the superseded terminal.
func TestTerminalReplayContinuesPastMirroredSessionComplete(t *testing.T) {
	h := newLifecycleHarness(t, nil)
	h.srv.cleanupDelay = 5 * time.Second
	h.init(t, "op-replay")
	h.push(t, "op-replay", "op-replay", "step_start", nil)                                     // 1
	h.push(t, "op-replay", "op-member", "agent_runtime_end", map[string]any{"reason": "done"}) // 2, session_complete 3
	h.push(t, "op-replay", "op-replay", "step_start", nil)                                     // 4, reopens
	h.push(t, "op-replay", "op-replay", "agent_runtime_end", map[string]any{"reason": "done"}) // 5, session_complete 6

	ws := dialWebSocket(t, h.http.URL, "/ws?operationId=op-replay")
	defer ws.close()
	ws.writeJSON(t, map[string]any{"type": "auth", "token": "service-token"})
	if msg := ws.readJSON(t); msg["type"] != "auth_success" {
		t.Fatalf("expected auth_success, got %+v", msg)
	}
	for _, want := range []struct{ id, typ string }{
		{"1", "agent_event"}, {"2", "agent_event"}, {"4", "agent_event"}, {"5", "agent_event"}, {"6", "session_complete"},
	} {
		if msg := ws.readJSON(t); msg["id"] != want.id || msg["type"] != want.typ {
			t.Fatalf("expected %s %s, got %+v", want.typ, want.id, msg)
		}
	}
}
