package gateway

import (
	"bufio"
	"bytes"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func testServer() (*Server, *httptest.Server) {
	srv := NewServer(Config{ServiceToken: "service-token", LobeAPIBaseURL: "http://127.0.0.1:1"})
	srv.authTimeout = time.Second
	srv.heartbeatTimeout = time.Second
	srv.cleanupDelay = time.Second
	httpSrv := httptest.NewServer(srv.Routes())
	return srv, httpSrv
}

func TestHealth(t *testing.T) {
	_, ts := testServer()
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || string(body) != "OK" {
		t.Fatalf("unexpected health response: %d %q", resp.StatusCode, body)
	}
}

func TestOperationsAuthAndStatus(t *testing.T) {
	_, ts := testServer()
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/operations/status?operationId=op-auth")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	postJSON(t, ts.URL+"/api/operations/init", map[string]any{"operationId": "op-auth", "userId": "user-1"}, http.StatusOK)

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/api/operations/status?operationId=op-auth", nil)
	req.Header.Set("Authorization", "Bearer service-token")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var status struct {
		ClientConnected bool   `json:"clientConnected"`
		Status          string `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		t.Fatal(err)
	}
	if status.ClientConnected || status.Status != "running" {
		t.Fatalf("unexpected status: %+v", status)
	}
}

func TestWebSocketPushAndResume(t *testing.T) {
	_, ts := testServer()
	defer ts.Close()
	postJSON(t, ts.URL+"/api/operations/init", map[string]any{"operationId": "op-ws", "userId": "user-1"}, http.StatusOK)

	ws := dialWebSocket(t, ts.URL, "/ws?operationId=op-ws")
	defer ws.close()
	ws.writeJSON(t, map[string]any{"type": "auth", "token": "service-token"})
	if msg := ws.readJSON(t); msg["type"] != "auth_success" {
		t.Fatalf("expected auth_success, got %+v", msg)
	}

	postJSON(t, ts.URL+"/api/operations/push-event", map[string]any{
		"operationId": "op-ws",
		"event": map[string]any{
			"data":        map[string]any{"chunkType": "text", "content": "hello"},
			"operationId": "op-ws",
			"stepIndex":   0,
			"timestamp":   time.Now().UnixMilli(),
			"type":        "stream_chunk",
		},
	}, http.StatusOK)

	msg := ws.readJSON(t)
	if msg["type"] != "agent_event" || msg["id"] != "1" {
		t.Fatalf("unexpected event: %+v", msg)
	}
	ws.close()

	postJSON(t, ts.URL+"/api/operations/push-event", map[string]any{
		"operationId": "op-ws",
		"event": map[string]any{
			"data":        map[string]any{"chunkType": "text", "content": "again"},
			"operationId": "op-ws",
			"stepIndex":   0,
			"timestamp":   time.Now().UnixMilli(),
			"type":        "stream_chunk",
		},
	}, http.StatusOK)

	ws2 := dialWebSocket(t, ts.URL, "/ws?operationId=op-ws")
	defer ws2.close()
	ws2.writeJSON(t, map[string]any{"type": "auth", "token": "service-token"})
	_ = ws2.readJSON(t)
	ws2.writeJSON(t, map[string]any{"type": "resume", "lastEventId": "1"})
	resumed := ws2.readJSON(t)
	if resumed["type"] != "agent_event" || resumed["id"] != "2" {
		t.Fatalf("unexpected resumed event: %+v", resumed)
	}
	wsExpectNoMessage(t, ws2, 100*time.Millisecond)

	ws2.writeJSON(t, map[string]any{"type": "resume", "lastEventId": "1", "wantStatus": true})
	resumed = ws2.readJSON(t)
	if resumed["type"] != "agent_event" || resumed["id"] != "2" {
		t.Fatalf("unexpected resumed event with status requested: %+v", resumed)
	}
	resumeComplete := ws2.readJSON(t)
	if resumeComplete["type"] != "resume_complete" || resumeComplete["status"] != "running" {
		t.Fatalf("unexpected resume_complete: %+v", resumeComplete)
	}
}

func TestWebSocketResumeCompleteWithoutBufferedEvents(t *testing.T) {
	_, ts := testServer()
	defer ts.Close()
	postJSON(t, ts.URL+"/api/operations/init", map[string]any{"operationId": "op-resume-empty", "userId": "user-1"}, http.StatusOK)

	ws := dialWebSocket(t, ts.URL, "/ws?operationId=op-resume-empty")
	defer ws.close()
	ws.writeJSON(t, map[string]any{"type": "auth", "token": "service-token"})
	if msg := ws.readJSON(t); msg["type"] != "auth_success" {
		t.Fatalf("expected auth_success, got %+v", msg)
	}

	ws.writeJSON(t, map[string]any{"type": "resume", "lastEventId": "", "wantStatus": true})
	resumeComplete := ws.readJSON(t)
	if resumeComplete["type"] != "resume_complete" || resumeComplete["status"] != "running" {
		t.Fatalf("unexpected resume_complete: %+v", resumeComplete)
	}
}

// TestWebSocketResumeCompleteSkippedWhenStatusMissing mirrors the upstream
// AgentOperationDO.handleResume safety contract (LOBE-10443): when the
// operation has no stored status (e.g. the init/resume race for
// trigger-started runs, or a freshly created operation that never received
// /api/operations/init), the gateway MUST NOT synthesize a resume_complete.
// The opt-in client treats "no resume_complete" as "keep waiting" — live
// events still stream and heartbeat loss still forces reconnect — which is
// safe and recoverable. Emitting an out-of-enum placeholder or a guessed
// terminal status would either break the SessionStatus contract or abort a
// run that is only just starting.
func TestWebSocketResumeCompleteSkippedWhenStatusMissing(t *testing.T) {
	srv, ts := testServer()
	defer ts.Close()
	// init lands the userId but we then clear the stored status to simulate
	// the init/resume race window where a trigger-started run's WS resume
	// arrives before init has set the status.
	postJSON(t, ts.URL+"/api/operations/init", map[string]any{"operationId": "op-resume-no-status", "userId": "user-1"}, http.StatusOK)
	op := srv.getOperation("op-resume-no-status")
	if op == nil {
		t.Fatal("expected operation to exist after init")
	}
	op.mu.Lock()
	op.record.Status = ""
	op.mu.Unlock()

	ws := dialWebSocket(t, ts.URL, "/ws?operationId=op-resume-no-status")
	defer ws.close()
	ws.writeJSON(t, map[string]any{"type": "auth", "token": "service-token"})
	if msg := ws.readJSON(t); msg["type"] != "auth_success" {
		t.Fatalf("expected auth_success, got %+v", msg)
	}

	ws.writeJSON(t, map[string]any{"type": "resume", "lastEventId": "", "wantStatus": true})
	wsExpectNoMessage(t, ws, 200*time.Millisecond)
}

func TestWebSocketJWTClaimValidation(t *testing.T) {
	jwks, signJWT := testJWTSigner(t)
	srv := NewServer(Config{JWKSPublicKey: jwks, LobeAPIBaseURL: "http://127.0.0.1:1", ServiceToken: "service-token"})
	srv.authTimeout = time.Second
	httpSrv := httptest.NewServer(srv.Routes())
	defer httpSrv.Close()
	postJSON(t, httpSrv.URL+"/api/operations/init", map[string]any{"operationId": "op-jwt", "userId": "jwt-user"}, http.StatusOK)

	fresh := dialWebSocket(t, httpSrv.URL, "/ws?operationId=op-jwt")
	defer fresh.close()
	fresh.writeJSON(t, map[string]any{"type": "auth", "token": signJWT("jwt-user", time.Now().Add(time.Minute), time.Now().Add(-time.Minute)), "tokenType": "jwt"})
	if msg := fresh.readJSON(t); msg["type"] != "auth_success" {
		t.Fatalf("expected fresh jwt auth_success, got %+v", msg)
	}

	expired := dialWebSocket(t, httpSrv.URL, "/ws?operationId=op-jwt")
	defer expired.close()
	expired.writeJSON(t, map[string]any{"type": "auth", "token": signJWT("jwt-user", time.Now().Add(-time.Minute), time.Now().Add(-2*time.Minute)), "tokenType": "jwt"})
	if msg := expired.readJSON(t); msg["type"] != "auth_expired" {
		t.Fatalf("expected expired jwt auth_expired, got %+v", msg)
	}
	wsExpectNoMessage(t, expired, 100*time.Millisecond)

	notYetActive := dialWebSocket(t, httpSrv.URL, "/ws?operationId=op-jwt")
	defer notYetActive.close()
	notYetActive.writeJSON(t, map[string]any{"type": "auth", "token": signJWT("jwt-user", time.Now().Add(time.Minute), time.Now().Add(time.Minute)), "tokenType": "jwt"})
	msg := notYetActive.readJSON(t)
	if msg["type"] != "auth_failed" || msg["reason"] != `"nbf" claim timestamp check failed` {
		t.Fatalf("expected nbf auth_failed, got %+v", msg)
	}
}

func TestConfirmationAndInput(t *testing.T) {
	_, ts := testServer()
	defer ts.Close()
	postJSON(t, ts.URL+"/api/operations/init", map[string]any{"operationId": "op-pending", "userId": "user-1"}, http.StatusOK)

	ws := dialWebSocket(t, ts.URL, "/ws?operationId=op-pending")
	defer ws.close()
	ws.writeJSON(t, map[string]any{"type": "auth", "token": "service-token"})
	_ = ws.readJSON(t)

	confirmationDone := make(chan map[string]any, 1)
	go func() {
		confirmationDone <- postJSONBody(t, ts.URL+"/api/operations/request-confirmation", map[string]any{
			"operationId": "op-pending",
			"timeout":     1000,
			"toolCallId":  "tool-1",
			"tool": map[string]any{
				"apiName":    "file.delete",
				"arguments":  "{}",
				"identifier": "file.delete",
				"name":       "delete",
			},
		}, http.StatusOK)
	}()
	request := ws.readJSON(t)
	if request["type"] != "tool_confirmation_request" || request["toolCallId"] != "tool-1" {
		t.Fatalf("unexpected confirmation request: %+v", request)
	}
	ws.writeJSON(t, map[string]any{"type": "tool_confirmation", "toolCallId": "tool-1", "approved": true})
	if result := <-confirmationDone; result["approved"] != true {
		t.Fatalf("unexpected confirmation result: %+v", result)
	}

	inputDone := make(chan map[string]any, 1)
	go func() {
		inputDone <- postJSONBody(t, ts.URL+"/api/operations/request-input", map[string]any{"operationId": "op-pending", "prompt": "name?", "timeout": 1000}, http.StatusOK)
	}()
	inputReq := ws.readJSON(t)
	requestID, _ := inputReq["requestId"].(string)
	if inputReq["type"] != "input_request" || requestID == "" {
		t.Fatalf("unexpected input request: %+v", inputReq)
	}
	ws.writeJSON(t, map[string]any{"type": "user_input", "requestId": requestID, "content": "alice"})
	if result := <-inputDone; result["content"] != "alice" {
		t.Fatalf("unexpected input result: %+v", result)
	}
}

func TestInactivityWatchdogReconcilesPhantomTimeout(t *testing.T) {
	finalizeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/agent/finalize-abandoned" {
			t.Fatalf("unexpected finalize path: %s", r.URL.Path)
		}
		writeTestJSON(t, w, map[string]any{"finalized": false, "found": true})
	}))
	defer finalizeSrv.Close()

	srv := NewServer(Config{ServiceToken: "service-token", LobeAPIBaseURL: finalizeSrv.URL})
	srv.cleanupDelay = time.Second
	httpSrv := httptest.NewServer(srv.Routes())
	defer httpSrv.Close()
	postJSON(t, httpSrv.URL+"/api/operations/init", map[string]any{"operationId": "op-watchdog-phantom", "userId": "user-1"}, http.StatusOK)

	ws := dialWebSocket(t, httpSrv.URL, "/ws?operationId=op-watchdog-phantom")
	defer ws.close()
	ws.writeJSON(t, map[string]any{"type": "auth", "token": "service-token"})
	_ = ws.readJSON(t)

	op := srv.getOperation("op-watchdog-phantom")
	op.mu.Lock()
	op.lastEventAt = time.Now().Add(-11 * time.Minute)
	op.lastEventTyp = "step_complete"
	op.mu.Unlock()
	op.fireWatchdog()

	msg := ws.readJSON(t)
	if msg["type"] != "session_complete" {
		t.Fatalf("expected session_complete for phantom timeout, got %+v", msg)
	}
	_, status := op.statusSnapshot()
	if status != StatusCompleted {
		t.Fatalf("expected completed status, got %s", status)
	}
}

func TestInactivityWatchdogReportsAbandonedTimeout(t *testing.T) {
	finalizeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeTestJSON(t, w, map[string]any{"abandoned": true, "finalized": true, "found": true})
	}))
	defer finalizeSrv.Close()

	srv := NewServer(Config{ServiceToken: "service-token", LobeAPIBaseURL: finalizeSrv.URL})
	srv.cleanupDelay = time.Second
	httpSrv := httptest.NewServer(srv.Routes())
	defer httpSrv.Close()
	postJSON(t, httpSrv.URL+"/api/operations/init", map[string]any{"operationId": "op-watchdog-abandoned", "userId": "user-1"}, http.StatusOK)

	ws := dialWebSocket(t, httpSrv.URL, "/ws?operationId=op-watchdog-abandoned")
	defer ws.close()
	ws.writeJSON(t, map[string]any{"type": "auth", "token": "service-token"})
	_ = ws.readJSON(t)

	op := srv.getOperation("op-watchdog-abandoned")
	op.mu.Lock()
	op.lastEventAt = time.Now().Add(-11 * time.Minute)
	op.lastEventTyp = "step_complete"
	op.mu.Unlock()
	op.fireWatchdog()

	msg := ws.readJSON(t)
	if msg["type"] != "status_change" || msg["status"] != string(StatusError) {
		t.Fatalf("expected error status_change for abandoned timeout, got %+v", msg)
	}
	_, status := op.statusSnapshot()
	if status != StatusError {
		t.Fatalf("expected error status, got %s", status)
	}
}

func TestInactivityWatchdogRechecksRecentEventsAfterFinalize(t *testing.T) {
	finalizeStarted := make(chan struct{})
	allowFinalize := make(chan struct{})
	finalizeSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(finalizeStarted)
		<-allowFinalize
		writeTestJSON(t, w, map[string]any{"abandoned": true, "finalized": true, "found": true})
	}))
	defer finalizeSrv.Close()

	srv := NewServer(Config{ServiceToken: "service-token", LobeAPIBaseURL: finalizeSrv.URL})
	srv.cleanupDelay = time.Second
	httpSrv := httptest.NewServer(srv.Routes())
	defer httpSrv.Close()
	postJSON(t, httpSrv.URL+"/api/operations/init", map[string]any{"operationId": "op-watchdog-race", "userId": "user-1"}, http.StatusOK)

	ws := dialWebSocket(t, httpSrv.URL, "/ws?operationId=op-watchdog-race")
	defer ws.close()
	ws.writeJSON(t, map[string]any{"type": "auth", "token": "service-token"})
	_ = ws.readJSON(t)

	op := srv.getOperation("op-watchdog-race")
	op.mu.Lock()
	op.lastEventAt = time.Now().Add(-11 * time.Minute)
	op.lastEventTyp = "step_complete"
	op.mu.Unlock()

	done := make(chan struct{})
	go func() {
		op.fireWatchdog()
		close(done)
	}()
	<-finalizeStarted

	op.pushEvent(agentStreamEvent{
		Data:        json.RawMessage(`{"content":"still alive"}`),
		OperationID: "op-watchdog-race",
		StepIndex:   0,
		Timestamp:   time.Now().UnixMilli(),
		Type:        "stream_chunk",
	})
	if msg := ws.readJSON(t); msg["type"] != "agent_event" {
		t.Fatalf("expected recent agent_event, got %+v", msg)
	}

	close(allowFinalize)
	<-done
	wsExpectNoMessage(t, ws, 100*time.Millisecond)
	_, status := op.statusSnapshot()
	if status != StatusRunning {
		t.Fatalf("expected running status after recent event, got %s", status)
	}
}

func postJSON(t *testing.T, url string, body any, expected int) {
	t.Helper()
	_ = postJSONBody(t, url, body, expected)
}

func writeTestJSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Fatal(err)
	}
}

func postJSONBody(t *testing.T, url string, body any, expected int) map[string]any {
	t.Helper()
	payload, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(payload))
	req.Header.Set("Authorization", "Bearer service-token")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != expected {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected %d, got %d: %s", expected, resp.StatusCode, data)
	}
	var result map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&result)
	return result
}

type testWS struct {
	br   *bufio.Reader
	conn net.Conn
}

func dialWebSocket(t *testing.T, serverURL string, path string) *testWS {
	t.Helper()
	addr := strings.TrimPrefix(serverURL, "http://")
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	key := randomWSKey(t)
	request := "GET " + path + " HTTP/1.1\r\n" +
		"Host: " + addr + "\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Key: " + key + "\r\n" +
		"Sec-WebSocket-Version: 13\r\n\r\n"
	if _, err := conn.Write([]byte(request)); err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(conn)
	status, err := br.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(status, "101") {
		t.Fatalf("unexpected websocket status: %s", status)
	}
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if line == "\r\n" {
			break
		}
	}
	return &testWS{br: br, conn: conn}
}

func (w *testWS) writeJSON(t *testing.T, value any) {
	t.Helper()
	payload, _ := json.Marshal(value)
	frame := []byte{0x81}
	mask := []byte{1, 2, 3, 4}
	if len(payload) < 126 {
		frame = append(frame, 0x80|byte(len(payload)))
	} else if len(payload) <= 0xffff {
		frame = append(frame, 0x80|126, byte(len(payload)>>8), byte(len(payload)))
	} else {
		t.Fatalf("test payload too large")
	}
	frame = append(frame, mask...)
	for i, b := range payload {
		frame = append(frame, b^mask[i%4])
	}
	if _, err := w.conn.Write(frame); err != nil {
		t.Fatal(err)
	}
}

func (w *testWS) readJSON(t *testing.T) map[string]any {
	t.Helper()
	_ = w.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	first, err := w.br.ReadByte()
	if err != nil {
		t.Fatal(err)
	}
	if first&0x0f != wsTextMessage {
		t.Fatalf("unexpected opcode %d", first&0x0f)
	}
	second, err := w.br.ReadByte()
	if err != nil {
		t.Fatal(err)
	}
	length := int(second & 0x7f)
	if length == 126 {
		buf := make([]byte, 2)
		if _, err := io.ReadFull(w.br, buf); err != nil {
			t.Fatal(err)
		}
		length = int(buf[0])<<8 | int(buf[1])
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(w.br, payload); err != nil {
		t.Fatal(err)
	}
	var msg map[string]any
	if err := json.Unmarshal(payload, &msg); err != nil {
		t.Fatalf("invalid json %q: %v", payload, err)
	}
	return msg
}

func wsExpectNoMessage(t *testing.T, ws *testWS, timeout time.Duration) {
	t.Helper()
	_ = ws.conn.SetReadDeadline(time.Now().Add(timeout))
	if _, err := ws.br.ReadByte(); err == nil {
		t.Fatal("expected no websocket message")
	} else if netErr, ok := err.(net.Error); !ok || !netErr.Timeout() {
		t.Fatalf("expected read timeout while waiting for no message, got %v", err)
	}
}

func (w *testWS) close() {
	_ = w.conn.Close()
}

func randomWSKey(t *testing.T) string {
	t.Helper()
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(buf)
}

func expectedAccept(key string) string {
	sum := sha1.Sum([]byte(key + wsGUID))
	return base64.StdEncoding.EncodeToString(sum[:])
}

func testJWTSigner(t *testing.T) (string, func(string, time.Time, time.Time) string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	exponent := big.NewInt(int64(key.PublicKey.E)).Bytes()
	jwksBody := map[string]any{
		"keys": []map[string]string{{
			"alg": "RS256",
			"e":   base64.RawURLEncoding.EncodeToString(exponent),
			"kty": "RSA",
			"n":   base64.RawURLEncoding.EncodeToString(key.PublicKey.N.Bytes()),
		}},
	}
	jwks, err := json.Marshal(jwksBody)
	if err != nil {
		t.Fatal(err)
	}

	return string(jwks), func(sub string, exp time.Time, nbf time.Time) string {
		t.Helper()
		header, err := json.Marshal(map[string]any{"alg": "RS256", "typ": "JWT"})
		if err != nil {
			t.Fatal(err)
		}
		payload, err := json.Marshal(map[string]any{
			"exp": exp.Unix(),
			"iat": time.Now().Unix(),
			"nbf": nbf.Unix(),
			"sub": sub,
		})
		if err != nil {
			t.Fatal(err)
		}
		signingInput := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
		sum := sha256.Sum256([]byte(signingInput))
		sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, sum[:])
		if err != nil {
			t.Fatal(err)
		}
		return signingInput + "." + base64.RawURLEncoding.EncodeToString(sig)
	}
}
