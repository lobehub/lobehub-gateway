package gateway

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	defaultAuthTimeout      = 10 * time.Second
	defaultHeartbeatTimeout = 90 * time.Second
)

type Server struct {
	auth             *authResolver
	authTimeout      time.Duration
	cfg              Config
	heartbeatTimeout time.Duration
	hubs             map[string]*hub
	hubsMu           sync.Mutex
}

func NewServer(cfg Config) *Server {
	return &Server{
		auth:             newAuthResolver(cfg),
		authTimeout:      defaultAuthTimeout,
		cfg:              cfg,
		heartbeatTimeout: defaultHeartbeatTimeout,
		hubs:             map[string]*hub{},
	}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("OK"))
	})
	mux.HandleFunc("GET /ws", s.handleWebSocket)
	mux.HandleFunc("POST /api/device/status", s.withServiceAuth(s.handleStatus))
	mux.HandleFunc("POST /api/device/devices", s.withServiceAuth(s.handleDevices))
	mux.HandleFunc("POST /api/device/tool-call", s.withServiceAuth(s.handleToolCall))
	mux.HandleFunc("POST /api/device/system-info", s.withServiceAuth(s.handleSystemInfo))
	mux.HandleFunc("POST /api/device/agent/run", s.withServiceAuth(s.handleAgentRun))
	mux.HandleFunc("POST /api/device/files/list", s.withServiceAuth(s.handleFileList))
	mux.HandleFunc("POST /api/device/files/read", s.withServiceAuth(s.handleFileRead))
	mux.HandleFunc("POST /api/device/files/write", s.withServiceAuth(s.handleFileWrite))
	mux.HandleFunc("POST /api/device/files/delete", s.withServiceAuth(s.handleFileDelete))
	mux.HandleFunc("POST /api/device/files/mkdir", s.withServiceAuth(s.handleFileMkdir))
	mux.HandleFunc("POST /api/device/files/move", s.withServiceAuth(s.handleFileMove))
	return mux
}

func (s *Server) hub(userID string) *hub {
	s.hubsMu.Lock()
	defer s.hubsMu.Unlock()
	if existing := s.hubs[userID]; existing != nil {
		return existing
	}
	h := newHub(userID)
	s.hubs[userID] = h
	return h
}

func (s *Server) withServiceAuth(next func(http.ResponseWriter, *http.Request, deviceHTTPBody)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.cfg.ServiceToken == "" || r.Header.Get("Authorization") != "Bearer "+s.cfg.ServiceToken {
			writeText(w, http.StatusUnauthorized, "Unauthorized")
			return
		}

		payload, err := io.ReadAll(r.Body)
		if err != nil {
			writeText(w, http.StatusBadRequest, err.Error())
			return
		}
		var body deviceHTTPBody
		if len(payload) > 0 {
			if err := json.Unmarshal(payload, &body); err != nil {
				writeText(w, http.StatusBadRequest, err.Error())
				return
			}
		}
		if body.UserID == "" {
			writeText(w, http.StatusBadRequest, "Missing userId")
			return
		}
		next(w, r, body)
	}
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("userId")
	if userID == "" {
		writeText(w, http.StatusBadRequest, "Missing userId")
		return
	}

	ws, err := upgradeWebSocket(w, r)
	if err != nil {
		return
	}

	now := time.Now().UnixMilli()
	h := s.hub(userID)
	conn := &connection{
		att: DeviceAttachment{
			Authenticated: false,
			ConnectedAt:   now,
			DeviceID:      defaultString(r.URL.Query().Get("deviceId"), "unknown"),
			Hostname:      r.URL.Query().Get("hostname"),
			LastHeartbeat: now,
			Platform:      r.URL.Query().Get("platform"),
		},
		authTimeout: s.authTimeout,
		hub:         h,
		ws:          ws,
	}
	h.register(conn)
	conn.startAuthTimer()
	go conn.readLoop(s.auth, s.heartbeatTimeout)
}

func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request, body deviceHTTPBody) {
	connections := s.hub(body.UserID).authenticatedConnections()
	writeJSON(w, http.StatusOK, map[string]any{"deviceCount": len(connections), "online": len(connections) > 0})
}

func (s *Server) handleDevices(w http.ResponseWriter, _ *http.Request, body deviceHTTPBody) {
	writeJSON(w, http.StatusOK, map[string]any{"devices": s.hub(body.UserID).devices()})
}

func (s *Server) handleToolCall(w http.ResponseWriter, _ *http.Request, body deviceHTTPBody) {
	h := s.hub(body.UserID)
	if len(h.authenticatedConnections()) == 0 {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"content": "Desktop device offline", "error": "DEVICE_OFFLINE", "success": false})
		return
	}
	target := h.target(body.DeviceID)
	if target == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "DEVICE_NOT_FOUND", "success": false})
		return
	}
	timeout := timeoutOrDefault(body.Timeout, 30*time.Second)
	requestID := randomID()

	resultCh := make(chan rpcEnvelope, 1)
	timeoutCh := make(chan struct{}, 1)
	h.setPending(requestID, timeout, func(msg rpcEnvelope) { resultCh <- msg }, func() { timeoutCh <- struct{}{} })
	_ = target.writeJSON(map[string]any{"requestId": requestID, "toolCall": json.RawMessage(body.ToolCall), "type": "tool_call_request"})

	select {
	case msg := <-resultCh:
		writeMergedResult(w, http.StatusOK, true, msg.Result)
	case <-timeoutCh:
		writeJSON(w, http.StatusGatewayTimeout, map[string]any{"content": "Tool call timed out (" + formatSeconds(timeout) + "s)", "error": "TIMEOUT", "success": false})
	}
}

func (s *Server) handleSystemInfo(w http.ResponseWriter, _ *http.Request, body deviceHTTPBody) {
	h := s.hub(body.UserID)
	if len(h.authenticatedConnections()) == 0 {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "DEVICE_OFFLINE", "success": false})
		return
	}
	target := h.target(body.DeviceID)
	if target == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "DEVICE_NOT_FOUND", "success": false})
		return
	}
	timeout := timeoutOrDefault(body.Timeout, 10*time.Second)
	requestID := randomID()

	resultCh := make(chan rpcEnvelope, 1)
	timeoutCh := make(chan struct{}, 1)
	h.setPending(requestID, timeout, func(msg rpcEnvelope) { resultCh <- msg }, func() { timeoutCh <- struct{}{} })
	_ = target.writeJSON(map[string]any{"requestId": requestID, "type": "system_info_request"})

	select {
	case msg := <-resultCh:
		writeMergedResult(w, http.StatusOK, true, msg.Result)
	case <-timeoutCh:
		writeJSON(w, http.StatusGatewayTimeout, map[string]any{"error": "TIMEOUT", "success": false})
	}
}

func (s *Server) handleAgentRun(w http.ResponseWriter, _ *http.Request, body deviceHTTPBody) {
	h := s.hub(body.UserID)
	if len(h.authenticatedConnections()) == 0 {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "DEVICE_OFFLINE", "success": false})
		return
	}
	target := h.target(body.DeviceID)
	if target == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "DEVICE_NOT_FOUND", "success": false})
		return
	}
	timeout := timeoutOrDefault(body.Timeout, 10*time.Second)
	key := body.OperationID

	resultCh := make(chan rpcEnvelope, 1)
	timeoutCh := make(chan struct{}, 1)
	h.setPending(key, timeout, func(msg rpcEnvelope) { resultCh <- msg }, func() { timeoutCh <- struct{}{} })
	msg := map[string]any{
		"agentType":   body.AgentType,
		"jwt":         body.JWT,
		"operationId": body.OperationID,
		"prompt":      body.Prompt,
		"topicId":     body.TopicID,
		"type":        "agent_run_request",
	}
	if body.CWD != "" {
		msg["cwd"] = body.CWD
	}
	if body.ResumeSessionID != "" {
		msg["resumeSessionId"] = body.ResumeSessionID
	}
	_ = target.writeJSON(msg)

	select {
	case msg := <-resultCh:
		if msg.Status == "rejected" {
			writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"error": "DEVICE_REJECTED", "success": false})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"success": true})
	case <-timeoutCh:
		writeJSON(w, http.StatusGatewayTimeout, map[string]any{"error": "TIMEOUT", "success": false})
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeText(w http.ResponseWriter, status int, value string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(value))
}

func writeMergedResult(w http.ResponseWriter, status int, success bool, result json.RawMessage) {
	merged := map[string]any{"success": success}
	if len(result) > 0 {
		var resultMap map[string]any
		if err := json.Unmarshal(result, &resultMap); err == nil {
			for k, v := range resultMap {
				merged[k] = v
			}
		}
	}
	writeJSON(w, status, merged)
}

func timeoutOrDefault(ms int, fallback time.Duration) time.Duration {
	if ms <= 0 {
		return fallback
	}
	return time.Duration(ms) * time.Millisecond
}

func randomID() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return time.Now().Format(time.RFC3339Nano)
	}
	return hex.EncodeToString(buf)
}

func defaultString(value string, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func formatSeconds(d time.Duration) string {
	seconds := d.Seconds()
	text := strings.TrimRight(strings.TrimRight(fmt.Sprintf("%.3f", seconds), "0"), ".")
	return text
}
