package gateway

import (
	"bufio"
	"errors"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestReplayBufferDropsOldEventsByBytes(t *testing.T) {
	op := newOperation(nil, "op")
	content := strings.Repeat("a", 3<<20)
	for i := 1; i <= 6; i++ {
		id := op.nextEventIDLocked()
		op.bufferLocked(id, map[string]any{"id": id, "content": content})
	}
	events, gap, _ := op.collectReplay("")
	totalBytes := 0
	for _, event := range events {
		totalBytes += len(event)
	}
	if !gap || totalBytes > 16<<20 || len(events) == 0 || messageID(events[len(events)-1]) != 6 {
		t.Fatalf("expected a bounded suffix ending at event 6, got gap=%t, bytes=%d, events=%d", gap, totalBytes, len(events))
	}
}

func TestOversizedEventReportsReplayGap(t *testing.T) {
	op := newOperation(nil, "op")
	for _, content := range []string{"earlier", strings.Repeat("a", 16<<20), "later"} {
		id := op.nextEventIDLocked()
		payload := op.bufferLocked(id, map[string]any{"id": id, "content": content})
		if len(payload) == 0 {
			t.Fatal("expected a payload for live delivery")
		}
	}
	events, gap, _ := op.collectReplay("1")
	if !gap || len(events) != 1 || messageID(events[0]) != 3 {
		t.Fatalf("expected a gap followed by event 3, got gap=%t, events=%d", gap, len(events))
	}
}

func TestLateWebSocketReplaysCompletedOperation(t *testing.T) {
	for _, final := range []struct {
		status string
		want   string
	}{{"completed", "session_complete"}, {"error", "resume_complete"}} {
		t.Run(final.status, func(t *testing.T) {
			srv, ts := testServer()
			defer ts.Close()
			srv.cleanupDelay = 5 * time.Second
			srv.heartbeatTimeout = 5 * time.Second
			postJSON(t, ts.URL+"/api/operations/init", map[string]any{"operationId": "late", "userId": "user"}, http.StatusOK)
			for _, eventType := range []string{"stream_chunk", "tool_execute"} {
				postJSON(t, ts.URL+"/api/operations/push-event", map[string]any{
					"operationId": "late",
					"event":       map[string]any{"type": eventType},
				}, http.StatusOK)
			}
			postJSON(t, ts.URL+"/api/operations/update-status", map[string]any{"operationId": "late", "status": final.status}, http.StatusOK)

			ws := dialWebSocket(t, ts.URL, "/ws?operationId=late")
			defer ws.close()
			ws.writeJSON(t, map[string]any{"type": "auth", "token": "service-token"})
			if msg := ws.readJSON(t); msg["type"] != "auth_success" {
				t.Fatalf("expected auth_success, got %+v", msg)
			}
			if msg := ws.readJSON(t); msg["type"] != "agent_event" || msg["id"] != "1" {
				t.Fatalf("expected stream history before completion, got %+v", msg)
			}
			msg := ws.readJSON(t)
			if msg["type"] != final.want {
				t.Fatalf("expected %s without a resume request, got %+v", final.want, msg)
			}
			if final.want == "resume_complete" && msg["status"] != final.status {
				t.Fatalf("expected status %s, got %+v", final.status, msg)
			}

			ws.writeJSON(t, map[string]any{"type": "resume", "lastEventId": "1", "wantStatus": true})
			if msg := ws.readJSON(t); msg["type"] != "resume_complete" || msg["gap"] != false || msg["status"] != final.status {
				t.Fatalf("expected only the requested status after fallback, got %+v", msg)
			}
		})
	}
}

func TestTerminalReconnectRespectsResumeCursor(t *testing.T) {
	srv, ts := testServer()
	defer ts.Close()
	const operationID = "terminal-resume"
	op := srv.getOrCreateOperation(operationID)
	t.Cleanup(op.cleanup)
	op.init(operationID, "user", nil)
	for range 2 {
		op.pushEvent(agentStreamEvent{OperationID: operationID, Type: "stream_chunk"})
	}
	op.updateStatus(StatusCompleted, "")

	ws := dialWebSocket(t, ts.URL, "/ws?operationId="+operationID)
	defer ws.close()
	ws.writeJSON(t, map[string]any{"type": "auth", "token": "service-token"})
	if msg := ws.readJSON(t); msg["type"] != "auth_success" {
		t.Fatalf("expected auth_success, got %+v", msg)
	}
	ws.writeJSON(t, map[string]any{"type": "resume", "lastEventId": "1", "wantStatus": true})
	for _, want := range []struct{ eventType, id string }{
		{"agent_event", "2"}, {"session_complete", "3"}, {"resume_complete", ""},
	} {
		msg := ws.readJSON(t)
		id, _ := msg["id"].(string)
		if msg["type"] != want.eventType || id != want.id {
			t.Fatalf("expected %s (id %q), got %+v", want.eventType, want.id, msg)
		}
		if want.eventType == "resume_complete" && (msg["gap"] != false || msg["status"] != "completed") {
			t.Fatalf("unexpected resume status: %+v", msg)
		}
	}
}

func TestLateTerminalReplayReportsGapBeforeCompletion(t *testing.T) {
	srv, ts := testServer()
	defer ts.Close()
	srv.cleanupDelay = 5 * time.Second
	srv.heartbeatTimeout = 5 * time.Second
	const operationID = "terminal-gap"
	op := srv.getOrCreateOperation(operationID)
	t.Cleanup(op.cleanup)
	op.init(operationID, "user", nil)
	for range eventBufferMax - 1 {
		op.pushEvent(agentStreamEvent{OperationID: operationID, Type: "stream_chunk"})
	}
	op.pushEvent(agentStreamEvent{OperationID: operationID, Type: "agent_runtime_end"})

	ws := dialWebSocket(t, ts.URL, "/ws?operationId="+operationID)
	defer ws.close()
	ws.writeJSON(t, map[string]any{"type": "auth", "token": "service-token"})
	if msg := ws.readJSON(t); msg["type"] != "auth_success" {
		t.Fatalf("expected auth_success, got %+v", msg)
	}
	sawGap, sawChunk, sawRuntimeEnd := false, false, false
	for range eventBufferTrim + 2 {
		msg := ws.readJSON(t)
		switch msg["type"] {
		case "agent_event":
			event, _ := msg["event"].(map[string]any)
			if event["type"] == "agent_runtime_end" {
				if !sawGap {
					t.Fatal("runtime ended before the gap notice")
				}
				sawRuntimeEnd = true
			} else {
				sawChunk = true
			}
		case "resume_complete":
			if msg["gap"] != true || msg["status"] != "completed" {
				t.Fatalf("expected terminal replay gap, got %+v", msg)
			}
			sawGap = true
		case "session_complete":
			if !sawChunk || !sawGap || !sawRuntimeEnd {
				t.Fatalf("incomplete terminal replay: chunks=%v gap=%v runtimeEnd=%v", sawChunk, sawGap, sawRuntimeEnd)
			}
			return
		}
	}
	t.Fatal("terminal replay did not complete")
}

type failingWriteConn struct{ net.Conn }

func (f failingWriteConn) Write([]byte) (int, error) {
	return 0, errors.New("write failed")
}

func TestBroadcastWriteFailureClosesConnection(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	op := newOperation(nil, "op")
	conn := newOperationConnection(op, &wsConn{br: bufio.NewReader(server), conn: failingWriteConn{server}})
	conn.markAuthenticated("user", false, false)
	op.register(conn)
	done := make(chan struct{})
	go func() {
		conn.readLoop()
		close(done)
	}()

	broadcast([]*operationConnection{conn}, []byte(`{"type":"session_complete"}`))
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("broadcast failure left the socket open")
	}
	if connected, _ := op.statusSnapshot(); connected {
		t.Fatal("failed socket is still registered as connected")
	}
}
