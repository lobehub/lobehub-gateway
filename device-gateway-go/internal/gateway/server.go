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
	defaultAuthTimeout          = 10 * time.Second
	defaultHeartbeatTimeout     = 90 * time.Second
	defaultToolCallTimeout      = 30 * time.Second
	minToolCallTimeout          = 5 * time.Second
	toolCallTimeoutPadding      = 15 * time.Second
	defaultSystemInfoTimeout    = 10 * time.Second
	defaultDeviceRPCTimeout     = 10 * time.Second
	defaultDeviceMessageTimeout = 30 * time.Second
	defaultAgentRunTimeout      = 10 * time.Second
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
	mux.HandleFunc("/api/device/", s.withServiceAuth(s.handleDeviceAPI))
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

func hubKeyFromBody(body deviceHTTPBody) string {
	if body.WorkspaceID != "" {
		return "workspace:" + body.WorkspaceID
	}
	return body.UserID
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
	workspaceID := r.URL.Query().Get("workspaceId")
	hubKey := userID
	if workspaceID != "" {
		hubKey = "workspace:" + workspaceID
	}
	if hubKey == "" {
		writeText(w, http.StatusBadRequest, "Missing userId or workspaceId")
		return
	}

	ws, err := upgradeWebSocket(w, r)
	if err != nil {
		return
	}

	now := time.Now().UnixMilli()
	h := s.hub(hubKey)
	conn := &connection{
		att: DeviceAttachment{
			Authenticated: false,
			Channel:       r.URL.Query().Get("channel"),
			ConnectedAt:   now,
			ConnectionID:  defaultString(r.URL.Query().Get("connectionId"), defaultString(r.URL.Query().Get("deviceId"), "unknown")),
			DeviceID:      defaultString(r.URL.Query().Get("deviceId"), "unknown"),
			Hostname:      r.URL.Query().Get("hostname"),
			LastHeartbeat: now,
			Platform:      r.URL.Query().Get("platform"),
			UserID:        userID,
			WorkspaceID:   workspaceID,
		},
		authTimeout: s.authTimeout,
		hub:         h,
		ws:          ws,
	}
	h.register(conn)
	conn.startAuthTimer()
	go conn.readLoop(s.auth, s.heartbeatTimeout)
}

func (s *Server) handleDeviceAPI(w http.ResponseWriter, r *http.Request, body deviceHTTPBody) {
	switch r.URL.Path {
	case "/api/device/status":
		s.handleStatus(w, r, body)
	case "/api/device/devices":
		s.handleDevices(w, r, body)
	case "/api/device/message-api":
		if r.Method == http.MethodPost {
			s.handleMessageAPI(w, r, body)
			return
		}
		writeText(w, http.StatusNotFound, "404 page not found")
	case "/api/device/tool-call":
		if r.Method == http.MethodPost {
			s.handleToolCall(w, r, body)
			return
		}
		writeText(w, http.StatusNotFound, "404 page not found")
	case "/api/device/system-info":
		if r.Method == http.MethodPost {
			s.handleSystemInfo(w, r, body)
			return
		}
		writeText(w, http.StatusNotFound, "404 page not found")
	case "/api/device/rpc":
		if r.Method == http.MethodPost {
			s.handleRPC(w, r, body)
			return
		}
		writeText(w, http.StatusNotFound, "404 page not found")
	case "/api/device/agent/run":
		if r.Method == http.MethodPost {
			s.handleAgentRun(w, r, body)
			return
		}
		writeText(w, http.StatusNotFound, "404 page not found")
	default:
		writeText(w, http.StatusNotFound, "404 page not found")
	}
}

func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request, body deviceHTTPBody) {
	hubKey := hubKeyFromBody(body)
	connections := s.hub(hubKey).authenticatedConnections()
	writeJSON(w, http.StatusOK, map[string]any{"deviceCount": s.hub(hubKey).deviceCount(), "online": len(connections) > 0})
}

func (s *Server) handleDevices(w http.ResponseWriter, _ *http.Request, body deviceHTTPBody) {
	writeJSON(w, http.StatusOK, map[string]any{"devices": s.hub(hubKeyFromBody(body)).devices()})
}

func (s *Server) handleToolCall(w http.ResponseWriter, _ *http.Request, body deviceHTTPBody) {
	h := s.hub(hubKeyFromBody(body))
	if len(h.authenticatedConnections()) == 0 {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"content": "桌面设备不在线", "error": "DEVICE_OFFLINE", "success": false})
		return
	}
	target := h.target(body.DeviceID)
	if target == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "DEVICE_NOT_FOUND", "success": false})
		return
	}
	timeout := normalizeToolCallTimeout(body.Timeout)
	requestID := randomID()

	request := map[string]any{
		"requestId": requestID,
		"timeout":   int(timeout / time.Millisecond),
		"toolCall":  json.RawMessage(body.ToolCall),
		"type":      "tool_call_request",
	}
	if body.OperationID != "" {
		request["operationId"] = body.OperationID
	}

	msg, status := h.dispatch(target, requestID, timeout+toolCallTimeoutPadding, request)
	switch status {
	case dispatchOK:
		writeMergedResult(w, http.StatusOK, true, msg.Result)
	case dispatchTimeout:
		writeJSON(w, http.StatusGatewayTimeout, map[string]any{"content": "工具调用超时（" + formatSeconds(timeout) + "s）", "error": "TIMEOUT", "success": false})
	case dispatchOffline:
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"content": "桌面设备不在线", "error": "DEVICE_OFFLINE", "success": false})
	}
}

func (s *Server) handleSystemInfo(w http.ResponseWriter, _ *http.Request, body deviceHTTPBody) {
	h := s.hub(hubKeyFromBody(body))
	if len(h.authenticatedConnections()) == 0 {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "DEVICE_OFFLINE", "success": false})
		return
	}
	target := h.target(body.DeviceID)
	if target == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "DEVICE_NOT_FOUND", "success": false})
		return
	}
	timeout := timeoutOrDefault(body.Timeout, defaultSystemInfoTimeout)
	requestID := randomID()

	msg, status := h.dispatch(target, requestID, timeout, map[string]any{"requestId": requestID, "type": "system_info_request"})
	switch status {
	case dispatchOK:
		writeMergedResult(w, http.StatusOK, true, msg.Result)
	case dispatchTimeout:
		writeJSON(w, http.StatusGatewayTimeout, map[string]any{"error": "TIMEOUT", "success": false})
	case dispatchOffline:
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "DEVICE_OFFLINE", "success": false})
	}
}

func (s *Server) handleRPC(w http.ResponseWriter, _ *http.Request, body deviceHTTPBody) {
	if strings.TrimSpace(body.Method) == "" {
		writeText(w, http.StatusBadRequest, "Missing method")
		return
	}
	h := s.hub(hubKeyFromBody(body))
	if len(h.authenticatedConnections()) == 0 {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "DEVICE_OFFLINE", "success": false})
		return
	}
	target := h.target(body.DeviceID)
	if target == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "DEVICE_NOT_FOUND", "success": false})
		return
	}
	timeout := timeoutOrDefault(body.Timeout, defaultDeviceRPCTimeout)
	requestID := randomID()

	request := map[string]any{
		"method":    body.Method,
		"requestId": requestID,
		"timeout":   int(timeout / time.Millisecond),
		"type":      "rpc_request",
	}
	if len(body.Params) > 0 {
		request["params"] = json.RawMessage(body.Params)
	}

	msg, status := h.dispatch(target, requestID, timeout, request)
	switch status {
	case dispatchOK:
		writeRawResult(w, http.StatusOK, msg.Result)
	case dispatchTimeout:
		writeJSON(w, http.StatusGatewayTimeout, map[string]any{"error": "TIMEOUT", "success": false})
	case dispatchOffline:
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "DEVICE_OFFLINE", "success": false})
	}
}

func (s *Server) handleMessageAPI(w http.ResponseWriter, _ *http.Request, body deviceHTTPBody) {
	h := s.hub(hubKeyFromBody(body))
	if len(h.authenticatedConnections()) == 0 {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"content": "桌面设备不在线", "error": "DEVICE_OFFLINE", "success": false})
		return
	}
	target := h.target(body.DeviceID)
	if target == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "DEVICE_NOT_FOUND", "success": false})
		return
	}
	timeout := timeoutOrDefault(body.Timeout, defaultDeviceMessageTimeout)
	requestID := randomID()

	request := map[string]any{
		"requestId": requestID,
		"type":      "message_api_request",
	}
	if len(body.API) > 0 {
		request["api"] = json.RawMessage(body.API)
	}

	msg, status := h.dispatch(target, requestID, timeout, request)
	switch status {
	case dispatchOK:
		writeMergedResult(w, http.StatusOK, true, msg.Result)
	case dispatchTimeout:
		writeJSON(w, http.StatusGatewayTimeout, map[string]any{"content": "消息 API 调用超时（" + formatSeconds(timeout) + "s）", "error": "TIMEOUT", "success": false})
	case dispatchOffline:
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"content": "桌面设备不在线", "error": "DEVICE_OFFLINE", "success": false})
	}
}

func (s *Server) handleAgentRun(w http.ResponseWriter, _ *http.Request, body deviceHTTPBody) {
	if strings.TrimSpace(body.OperationID) == "" {
		writeText(w, http.StatusBadRequest, "Missing operationId")
		return
	}
	h := s.hub(hubKeyFromBody(body))
	if len(h.authenticatedConnections()) == 0 {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "DEVICE_OFFLINE", "success": false})
		return
	}
	target := h.target(body.DeviceID)
	if target == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "DEVICE_NOT_FOUND", "success": false})
		return
	}
	timeout := timeoutOrDefault(body.Timeout, defaultAgentRunTimeout)
	key := body.OperationID

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
	if body.SystemContext != "" {
		msg["systemContext"] = body.SystemContext
	}
	if len(body.Args) > 0 {
		msg["args"] = body.Args
	}
	if len(body.ImageList) > 0 {
		msg["imageList"] = body.ImageList
	}

	result, status := h.dispatch(target, key, timeout, msg)
	switch status {
	case dispatchOK:
		msg := result
		if msg.Status == "rejected" {
			errorText := defaultString(msg.Reason, "DEVICE_REJECTED")
			writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"error": errorText, "success": false})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"success": true})
	case dispatchTimeout:
		writeJSON(w, http.StatusGatewayTimeout, map[string]any{"error": "TIMEOUT", "success": false})
	case dispatchOffline:
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "DEVICE_OFFLINE", "success": false})
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

func writeRawResult(w http.ResponseWriter, status int, result json.RawMessage) {
	if len(result) == 0 {
		writeJSON(w, status, map[string]any{"success": false})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(result)
}

func timeoutOrDefault(ms int, fallback time.Duration) time.Duration {
	if ms <= 0 {
		return fallback
	}
	return time.Duration(ms) * time.Millisecond
}

func normalizeToolCallTimeout(ms int) time.Duration {
	timeout := timeoutOrDefault(ms, defaultToolCallTimeout)
	if timeout < minToolCallTimeout {
		return minToolCallTimeout
	}
	return timeout
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
