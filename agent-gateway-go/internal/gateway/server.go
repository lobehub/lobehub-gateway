package gateway

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	defaultAuthTimeout      = 10 * time.Second
	defaultHeartbeatTimeout = 90 * time.Second
	defaultCleanupDelay     = 10 * time.Minute
	defaultIdleWatchdog     = 30 * time.Minute
	defaultHeteroWatchdog   = 60 * time.Minute
	defaultConfirmationWait = 60 * time.Second
	defaultInputWait        = 60 * time.Second
)

type Server struct {
	auth             *authResolver
	authTimeout      time.Duration
	cfg              Config
	cleanupDelay     time.Duration
	heartbeatTimeout time.Duration
	logger           *slog.Logger
	operations       map[string]*operation
	operationsMu     sync.Mutex
	v2Connections    map[*hubConnection]struct{}
	v2Mu             sync.RWMutex
}

func NewServer(cfg Config) *Server {
	return &Server{
		auth:             newAuthResolver(cfg),
		authTimeout:      defaultAuthTimeout,
		cfg:              cfg,
		cleanupDelay:     defaultCleanupDelay,
		heartbeatTimeout: defaultHeartbeatTimeout,
		logger:           slog.Default(),
		operations:       map[string]*operation{},
		v2Connections:    map[*hubConnection]struct{}{},
	}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("OK"))
	})
	mux.HandleFunc("GET /ws", s.handleWebSocket)
	mux.HandleFunc("GET /v2/ws", s.handleV2WebSocket)
	mux.HandleFunc("/api/admin/", func(w http.ResponseWriter, r *http.Request) {
		writeText(w, http.StatusNotFound, "404 page not found")
	})
	mux.HandleFunc("/api/operations/", s.handleOperations)
	return mux
}

func (s *Server) getOrCreateOperation(operationID string) *operation {
	s.operationsMu.Lock()
	defer s.operationsMu.Unlock()
	if existing := s.operations[operationID]; existing != nil {
		return existing
	}
	op := newOperation(s, operationID)
	s.operations[operationID] = op
	return op
}

func (s *Server) getOperation(operationID string) *operation {
	s.operationsMu.Lock()
	defer s.operationsMu.Unlock()
	return s.operations[operationID]
}

func (s *Server) removeOperation(operationID string, op *operation) {
	s.operationsMu.Lock()
	defer s.operationsMu.Unlock()
	if s.operations[operationID] == op {
		delete(s.operations, operationID)
	}
}

func (s *Server) withServiceAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.cfg.ServiceToken == "" || r.Header.Get("Authorization") != "Bearer "+s.cfg.ServiceToken {
			writeText(w, http.StatusUnauthorized, "Unauthorized")
			return
		}
		next(w, r)
	}
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	operationID := r.URL.Query().Get("operationId")
	if operationID == "" {
		writeText(w, http.StatusBadRequest, "Missing operationId")
		return
	}
	ws, err := upgradeWebSocket(w, r)
	if err != nil {
		return
	}
	op := s.getOrCreateOperation(operationID)
	conn := newOperationConnection(op, ws)
	op.register(conn)
	conn.startAuthTimer(s.authTimeout)
	go conn.readLoop()
}

func (s *Server) handleOperations(w http.ResponseWriter, r *http.Request) {
	if s.cfg.ServiceToken == "" || r.Header.Get("Authorization") != "Bearer "+s.cfg.ServiceToken {
		writeText(w, http.StatusUnauthorized, "Unauthorized")
		return
	}

	path := r.URL.Path
	if path == "/api/operations/init" && r.Method == http.MethodPost {
		s.handleInit(w, r)
		return
	}
	if path == "/api/operations/status" && r.Method == http.MethodGet {
		s.handleStatus(w, r)
		return
	}

	if r.Method != http.MethodPost {
		writeText(w, http.StatusNotFound, "404 page not found")
		return
	}

	body, ok := readOperationBody(w, r)
	if !ok {
		return
	}
	if path == "/api/operations/llm-execute" || path == "/api/operations/llm-cancel" || path == "/api/operations/llm-close" {
		if body.OperationID == "" {
			message := "Missing operationId or data.callId"
			if path == "/api/operations/llm-close" {
				message = "Missing operationId or callId"
			}
			writeText(w, 400, message)
			return
		}
		s.handleLLM(w, r, body)
		return
	}
	if body.OperationID == "" {
		writeText(w, http.StatusBadRequest, "Missing operationId")
		return
	}

	switch path {
	case "/api/operations/push-event":
		s.handlePushEvent(w, r, body)
	case "/api/operations/push-events":
		s.handlePushEvents(w, r, body)
	case "/api/operations/request-confirmation":
		s.handleRequestConfirmation(w, r, body)
	case "/api/operations/request-input":
		s.handleRequestInput(w, r, body)
	case "/api/operations/tool-execute":
		s.handleToolExecute(w, r, body)
	case "/api/operations/update-status":
		s.handleUpdateStatus(w, r, body)
	default:
		writeText(w, http.StatusNotFound, "404 page not found")
	}
}

func readOperationBody(w http.ResponseWriter, r *http.Request) (operationHTTPBody, bool) {
	payload, err := io.ReadAll(r.Body)
	if err != nil {
		writeText(w, http.StatusBadRequest, err.Error())
		return operationHTTPBody{}, false
	}
	var body operationHTTPBody
	if len(payload) > 0 {
		if err := json.Unmarshal(payload, &body); err != nil {
			writeText(w, http.StatusBadRequest, err.Error())
			return operationHTTPBody{}, false
		}
	}
	return body, true
}

func (s *Server) handleInit(w http.ResponseWriter, r *http.Request) {
	var body operationHTTPBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeText(w, http.StatusBadRequest, err.Error())
		return
	}
	if body.OperationID == "" || body.UserID == "" {
		writeText(w, http.StatusBadRequest, "Missing operationId or userId")
		return
	}
	op := s.getOrCreateOperation(body.OperationID)
	op.init(body.OperationID, body.UserID, body.Meta)
	writeJSON(w, http.StatusOK, map[string]any{"success": true})
}

func (s *Server) handlePushEvent(w http.ResponseWriter, _ *http.Request, body operationHTTPBody) {
	if body.Event == nil {
		writeText(w, http.StatusBadRequest, "Missing event")
		return
	}
	s.getOrCreateOperation(body.OperationID).pushEvent(*body.Event)
	writeJSON(w, http.StatusOK, map[string]any{"success": true})
}

func (s *Server) handlePushEvents(w http.ResponseWriter, _ *http.Request, body operationHTTPBody) {
	if len(body.Events) == 0 {
		writeText(w, http.StatusBadRequest, "Missing events")
		return
	}
	op := s.getOrCreateOperation(body.OperationID)
	for _, event := range body.Events {
		op.pushEvent(event)
	}
	writeJSON(w, http.StatusOK, map[string]any{"success": true})
}

func (s *Server) handleRequestConfirmation(w http.ResponseWriter, _ *http.Request, body operationHTTPBody) {
	if body.ToolCallID == "" || body.Tool == nil {
		writeText(w, http.StatusBadRequest, "Missing toolCallId or tool")
		return
	}
	timeout := durationFromMS(body.Timeout, defaultConfirmationWait)
	approved, ok := s.getOrCreateOperation(body.OperationID).requestConfirmation(body.ToolCallID, *body.Tool, timeout)
	if !ok {
		writeJSON(w, http.StatusGatewayTimeout, map[string]any{"approved": false, "error": "TIMEOUT"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"approved": approved})
}

func (s *Server) handleRequestInput(w http.ResponseWriter, _ *http.Request, body operationHTTPBody) {
	if body.Prompt == "" {
		writeText(w, http.StatusBadRequest, "Missing prompt")
		return
	}
	timeout := durationFromMS(body.Timeout, defaultInputWait)
	content, ok := s.getOrCreateOperation(body.OperationID).requestInput(body.Prompt, timeout)
	if !ok {
		writeJSON(w, http.StatusGatewayTimeout, map[string]any{"content": "", "error": "TIMEOUT"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"content": content})
}

func (s *Server) handleToolExecute(w http.ResponseWriter, _ *http.Request, body operationHTTPBody) {
	if len(body.Data) == 0 || body.OperationID == "" {
		writeText(w, http.StatusBadRequest, "Missing operationId or data")
		return
	}
	event := agentStreamEvent{
		Data:        body.Data,
		OperationID: body.OperationID,
		StepIndex:   0,
		Timestamp:   time.Now().UnixMilli(),
		Type:        "tool_execute",
	}
	s.getOrCreateOperation(body.OperationID).broadcastToolExecute(event)
	writeJSON(w, http.StatusOK, map[string]any{"success": true})
}

func (s *Server) handleUpdateStatus(w http.ResponseWriter, _ *http.Request, body operationHTTPBody) {
	if body.Status == "" {
		writeText(w, http.StatusBadRequest, "Missing status")
		return
	}
	s.getOrCreateOperation(body.OperationID).updateStatus(body.Status, body.Summary)
	writeJSON(w, http.StatusOK, map[string]any{"success": true})
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	operationID := r.URL.Query().Get("operationId")
	if operationID == "" {
		writeText(w, http.StatusBadRequest, "Missing operationId")
		return
	}
	op := s.getOperation(operationID)
	if op == nil {
		writeJSON(w, http.StatusOK, map[string]any{"clientConnected": false, "status": "unknown"})
		return
	}
	clientConnected, status := op.statusSnapshot()
	writeJSON(w, http.StatusOK, map[string]any{"clientConnected": clientConnected, "status": status})
}

func durationFromMS(ms int, fallback time.Duration) time.Duration {
	if ms <= 0 {
		return fallback
	}
	return time.Duration(ms) * time.Millisecond
}

func isTerminalStatus(status SessionStatus) bool {
	return status == StatusCompleted || status == StatusError || status == StatusInterrupted
}

func normalizeBaseURL(value string) string {
	return strings.TrimRight(value, "/")
}
