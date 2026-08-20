package gateway

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

const (
	eventBufferMax  = 1000
	eventBufferTrim = 500
)

var inflightEventTypes = map[string]bool{
	"stream_start": true,
	"stream_chunk": true,
	"tool_start":   true,
	"step_start":   true,
}

type pendingConfirmation struct {
	ch    chan bool
	timer *time.Timer
}

type pendingInput struct {
	ch    chan string
	timer *time.Timer
}

type operation struct {
	server *Server

	cleanupTimer *time.Timer
	connections  map[*operationConnection]struct{}
	eventBuffer  []bufferedEvent
	eventCounter int
	lastEventAt  time.Time
	lastEventTyp string
	mu           sync.RWMutex
	pendingConf  map[string]pendingConfirmation
	pendingInput map[string]pendingInput
	record       operationRecord
	watchdog     *time.Timer
}

func newOperation(server *Server, operationID string) *operation {
	return &operation{
		connections:  map[*operationConnection]struct{}{},
		pendingConf:  map[string]pendingConfirmation{},
		pendingInput: map[string]pendingInput{},
		record: operationRecord{
			CreatedAt:   time.Now(),
			OperationID: operationID,
			// Status intentionally left empty to mirror upstream DO storage,
			// which starts undefined and is only set when /api/operations/init
			// lands. This makes the init/resume race (LOBE-10443) observable:
			// a WS resume that arrives before init sees no stored status and
			// gets no resume_complete, so the opt-in client keeps waiting
			// instead of false-completing.
			Status: "",
		},
		server: server,
	}
}

func (o *operation) init(operationID string, userID string) {
	o.mu.Lock()
	o.record.OperationID = operationID
	o.record.UserID = userID
	o.record.Status = StatusRunning
	if o.record.CreatedAt.IsZero() {
		o.record.CreatedAt = time.Now()
	}
	o.stopCleanupLocked()
	o.mu.Unlock()
}

func (o *operation) register(conn *operationConnection) {
	o.mu.Lock()
	o.connections[conn] = struct{}{}
	o.mu.Unlock()
}

func (o *operation) remove(conn *operationConnection) {
	o.mu.Lock()
	delete(o.connections, conn)
	o.mu.Unlock()
	conn.stopTimers()
}

func (o *operation) storedUserID() string {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.record.UserID
}

func (o *operation) statusSnapshot() (bool, SessionStatus) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	connected := false
	for conn := range o.connections {
		if conn.isAuthenticated() && !conn.isAdmin() {
			connected = true
			break
		}
	}
	status := o.record.Status
	if status == "" {
		status = "unknown"
	}
	return connected, status
}

func (o *operation) nextEventIDLocked() string {
	o.eventCounter++
	return jsonNumberString(o.eventCounter)
}

func (o *operation) pushEvent(event agentStreamEvent) {
	msg := map[string]any{"event": event, "type": "agent_event"}
	o.mu.Lock()
	id := o.nextEventIDLocked()
	msg["id"] = id
	o.bufferLocked(id, msg)
	o.lastEventAt = time.Now()
	o.lastEventTyp = event.Type
	o.scheduleWatchdogLocked()
	connections := o.authenticatedConnectionsLocked()
	o.mu.Unlock()
	broadcast(connections, msg)

	if event.Type == "agent_runtime_end" {
		o.handleAgentRuntimeEnd(event)
	}
}

func (o *operation) broadcastToolExecute(event agentStreamEvent) {
	msg := map[string]any{"event": event, "type": "agent_event"}
	o.mu.Lock()
	id := o.nextEventIDLocked()
	msg["id"] = id
	o.bufferLocked(id, msg)
	connections := o.authenticatedConnectionsLocked()
	o.mu.Unlock()
	broadcast(connections, msg)
}

func (o *operation) handleAgentRuntimeEnd(event agentStreamEvent) {
	var data agentRuntimeEndData
	_ = json.Unmarshal(event.Data, &data)
	status := StatusCompleted
	if data.Reason == "error" {
		status = StatusError
	} else if data.Reason == "interrupted" {
		status = StatusInterrupted
	}
	o.handleSessionEnd(status, data.ReasonDetail)
}

func (o *operation) handleSessionEnd(status SessionStatus, summary string) {
	msg := map[string]any{"summary": summary, "type": "session_complete"}
	o.mu.Lock()
	o.record.Status = status
	id := o.nextEventIDLocked()
	msg["id"] = id
	o.bufferLocked(id, msg)
	connections := o.authenticatedConnectionsLocked()
	o.scheduleCleanupLocked()
	o.mu.Unlock()
	broadcast(connections, msg)
}

func (o *operation) updateStatus(status SessionStatus, summary string) {
	o.mu.Lock()
	o.record.Status = status
	id := o.nextEventIDLocked()
	var msg map[string]any
	if status == StatusCompleted {
		msg = map[string]any{"id": id, "summary": summary, "type": "session_complete"}
	} else {
		msg = map[string]any{"id": id, "status": status, "type": "status_change"}
	}
	o.bufferLocked(id, msg)
	connections := o.authenticatedConnectionsLocked()
	if isTerminalStatus(status) {
		o.scheduleCleanupLocked()
	}
	o.mu.Unlock()
	broadcast(connections, msg)
}

func (o *operation) requestConfirmation(toolCallID string, tool toolCallInfo, timeout time.Duration) (bool, bool) {
	msg := map[string]any{"tool": tool, "toolCallId": toolCallID, "type": "tool_confirmation_request"}
	ch := make(chan bool, 1)
	timer := time.NewTimer(timeout)
	o.mu.Lock()
	id := o.nextEventIDLocked()
	msg["id"] = id
	o.bufferLocked(id, msg)
	o.pendingConf[toolCallID] = pendingConfirmation{ch: ch, timer: timer}
	connections := o.authenticatedConnectionsLocked()
	o.mu.Unlock()
	broadcast(connections, msg)

	select {
	case approved := <-ch:
		return approved, true
	case <-timer.C:
		o.mu.Lock()
		delete(o.pendingConf, toolCallID)
		o.mu.Unlock()
		return false, false
	}
}

func (o *operation) requestInput(prompt string, timeout time.Duration) (string, bool) {
	requestID := randomID()
	msg := map[string]any{"prompt": prompt, "requestId": requestID, "type": "input_request"}
	ch := make(chan string, 1)
	timer := time.NewTimer(timeout)
	o.mu.Lock()
	id := o.nextEventIDLocked()
	msg["id"] = id
	o.bufferLocked(id, msg)
	o.pendingInput[requestID] = pendingInput{ch: ch, timer: timer}
	connections := o.authenticatedConnectionsLocked()
	o.mu.Unlock()
	broadcast(connections, msg)

	select {
	case content := <-ch:
		return content, true
	case <-timer.C:
		o.mu.Lock()
		delete(o.pendingInput, requestID)
		o.mu.Unlock()
		return "", false
	}
}

func (o *operation) resolveConfirmation(toolCallID string, approved bool) {
	o.mu.Lock()
	pending, ok := o.pendingConf[toolCallID]
	if ok {
		delete(o.pendingConf, toolCallID)
	}
	o.mu.Unlock()
	if !ok {
		return
	}
	if pending.timer.Stop() {
		pending.ch <- approved
	}
}

func (o *operation) resolveInput(requestID string, content string) {
	o.mu.Lock()
	pending, ok := o.pendingInput[requestID]
	if ok {
		delete(o.pendingInput, requestID)
	}
	o.mu.Unlock()
	if !ok {
		return
	}
	if pending.timer.Stop() {
		pending.ch <- content
	}
}

func (o *operation) handleResume(conn *operationConnection, lastEventID string, wantStatus bool) {
	o.mu.RLock()
	idx := -1
	for i, event := range o.eventBuffer {
		if event.ID == lastEventID {
			idx = i
			break
		}
	}
	missed := o.eventBuffer
	if idx != -1 {
		missed = o.eventBuffer[idx+1:]
	}
	payloads := make([]json.RawMessage, 0, len(missed))
	for _, event := range missed {
		payloads = append(payloads, append(json.RawMessage(nil), event.Data...))
	}
	// Mirror upstream AgentOperationDO.handleResume: only confirm a status we
	// actually hold. A missing status can mean "init hasn't landed yet" (the
	// init/resume race) just as much as "already cleaned up" — synthesizing a
	// terminal signal here would abort a run that is only just starting, and
	// emitting an out-of-enum value breaks the SessionStatus contract. The
	// opt-in client treats "no resume_complete" as "keep waiting" — live events
	// still stream and heartbeat loss still forces reconnect — which is safe
	// and recoverable. See LOBE-10443.
	status := o.record.Status
	o.mu.RUnlock()
	for _, payload := range payloads {
		_ = conn.writeRaw(payload)
	}
	if wantStatus && status != "" {
		_ = conn.writeJSON(map[string]any{
			"status": status,
			"type":   "resume_complete",
		})
	}
}

func (o *operation) bufferLocked(id string, msg map[string]any) {
	payload, _ := json.Marshal(msg)
	o.eventBuffer = append(o.eventBuffer, bufferedEvent{Data: payload, ID: id})
	if len(o.eventBuffer) > eventBufferMax {
		o.eventBuffer = append([]bufferedEvent(nil), o.eventBuffer[len(o.eventBuffer)-eventBufferTrim:]...)
	}
}

func (o *operation) authenticatedConnectionsLocked() []*operationConnection {
	connections := make([]*operationConnection, 0, len(o.connections))
	for conn := range o.connections {
		if conn.isAuthenticated() {
			connections = append(connections, conn)
		}
	}
	return connections
}

func (o *operation) scheduleWatchdogLocked() {
	if o.lastEventAt.IsZero() || isTerminalStatus(o.record.Status) {
		return
	}
	timeout := defaultIdleWatchdog
	if inflightEventTypes[o.lastEventTyp] {
		timeout = defaultInflightWatchdog
	}
	due := o.lastEventAt.Add(timeout)
	delay := time.Until(due)
	if delay < 0 {
		delay = 0
	}
	if o.watchdog == nil {
		o.watchdog = time.AfterFunc(delay, o.fireWatchdog)
		return
	}
	o.watchdog.Reset(delay)
}

func (o *operation) fireWatchdog() {
	o.mu.Lock()
	if o.lastEventAt.IsZero() || isTerminalStatus(o.record.Status) {
		o.mu.Unlock()
		return
	}
	timeout := defaultIdleWatchdog
	if inflightEventTypes[o.lastEventTyp] {
		timeout = defaultInflightWatchdog
	}
	idle := time.Since(o.lastEventAt)
	if idle < timeout {
		o.scheduleWatchdogLocked()
		o.mu.Unlock()
		return
	}
	operationID := o.record.OperationID
	o.mu.Unlock()

	result, ok := o.callFinalizeAbandoned(operationID, "inactivity_watchdog")
	abandoned := true
	if ok {
		if result.Abandoned != nil {
			abandoned = *result.Abandoned
		} else {
			abandoned = boolValue(result.Found) && boolValue(result.Finalized)
		}
	}

	o.mu.Lock()
	if isTerminalStatus(o.record.Status) {
		o.mu.Unlock()
		return
	}
	if o.lastEventAt.IsZero() {
		o.mu.Unlock()
		return
	}
	currentTimeout := defaultIdleWatchdog
	if inflightEventTypes[o.lastEventTyp] {
		currentTimeout = defaultInflightWatchdog
	}
	if time.Since(o.lastEventAt) < currentTimeout {
		o.scheduleWatchdogLocked()
		o.mu.Unlock()
		return
	}
	id := o.nextEventIDLocked()
	var msg map[string]any
	if abandoned {
		o.record.Status = StatusError
		msg = map[string]any{"id": id, "status": StatusError, "type": "status_change"}
	} else {
		o.record.Status = StatusCompleted
		msg = map[string]any{"id": id, "summary": watchdogSummary(idle), "type": "session_complete"}
	}
	o.bufferLocked(id, msg)
	connections := o.authenticatedConnectionsLocked()
	o.scheduleCleanupLocked()
	o.mu.Unlock()
	broadcast(connections, msg)
}

func (o *operation) callFinalizeAbandoned(operationID string, reason string) (finalizeAbandonedResult, bool) {
	if operationID == "" {
		return finalizeAbandonedResult{}, false
	}
	body, _ := json.Marshal(map[string]string{"operationId": operationID, "reason": reason})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, normalizeBaseURL(o.server.cfg.LobeAPIBaseURL)+"/api/agent/finalize-abandoned", bytes.NewReader(body))
	if err != nil {
		return finalizeAbandonedResult{}, false
	}
	req.Header.Set("Authorization", "Bearer "+o.server.cfg.ServiceToken)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return finalizeAbandonedResult{}, false
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, resp.Body)
		return finalizeAbandonedResult{}, false
	}
	var result finalizeAbandonedResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return finalizeAbandonedResult{}, false
	}
	return result, true
}

func boolValue(value *bool) bool {
	return value != nil && *value
}

func watchdogSummary(idle time.Duration) string {
	return fmt.Sprintf("Operation idle for %ds with no events from agent-runtime — presumed killed mid-flight", int(idle.Seconds()+0.5))
}

func (o *operation) forwardToolResult(msg toolResultMessage) {
	body, _ := json.Marshal(map[string]any{
		"content":    msg.Content,
		"error":      jsonRawOrNil(msg.Error),
		"success":    msg.Success,
		"toolCallId": msg.ToolCallID,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, normalizeBaseURL(o.server.cfg.LobeAPIBaseURL)+"/api/agent/tool-result", bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Authorization", "Bearer "+o.server.cfg.ServiceToken)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
}

func (o *operation) scheduleCleanupLocked() {
	o.stopWatchdogLocked()
	if o.cleanupTimer == nil {
		o.cleanupTimer = time.AfterFunc(o.server.cleanupDelay, o.cleanup)
		return
	}
	o.cleanupTimer.Reset(o.server.cleanupDelay)
}

func (o *operation) cleanup() {
	o.mu.Lock()
	connections := make([]*operationConnection, 0, len(o.connections))
	for conn := range o.connections {
		connections = append(connections, conn)
	}
	o.connections = map[*operationConnection]struct{}{}
	o.stopWatchdogLocked()
	o.stopCleanupLocked()
	operationID := o.record.OperationID
	o.mu.Unlock()
	for _, conn := range connections {
		conn.close(wsCloseNormal, "Session expired")
	}
	o.server.removeOperation(operationID, o)
}

func (o *operation) stopCleanupLocked() {
	if o.cleanupTimer != nil {
		o.cleanupTimer.Stop()
		o.cleanupTimer = nil
	}
}

func (o *operation) stopWatchdogLocked() {
	if o.watchdog != nil {
		o.watchdog.Stop()
		o.watchdog = nil
	}
}

type operationConnection struct {
	att            clientAttachment
	authTimer      *time.Timer
	heartbeatTimer *time.Timer
	mu             sync.RWMutex
	operation      *operation
	ws             *wsConn
}

func newOperationConnection(op *operation, ws *wsConn) *operationConnection {
	now := time.Now().UnixMilli()
	return &operationConnection{
		att: clientAttachment{
			ConnectedAt:   now,
			LastHeartbeat: now,
		},
		operation: op,
		ws:        ws,
	}
}

func (c *operationConnection) isAuthenticated() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.att.Authenticated
}

func (c *operationConnection) isAdmin() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.att.IsAdmin
}

func (c *operationConnection) markAuthenticated(userID string, isAdmin bool) {
	c.mu.Lock()
	c.att.Authenticated = true
	c.att.IsAdmin = isAdmin
	c.att.LastHeartbeat = time.Now().UnixMilli()
	c.att.UserID = userID
	c.mu.Unlock()
}

func (c *operationConnection) recordHeartbeat() {
	c.mu.Lock()
	c.att.LastHeartbeat = time.Now().UnixMilli()
	c.mu.Unlock()
}

func (c *operationConnection) writeJSON(v any) error {
	payload, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return c.writeRaw(payload)
}

func (c *operationConnection) writeRaw(payload []byte) error {
	return c.ws.writeJSON(payload)
}

func (c *operationConnection) close(code int, reason string) {
	_ = c.ws.writeClose(code, reason)
	_ = c.ws.close()
}

func (c *operationConnection) startAuthTimer(timeout time.Duration) {
	c.authTimer = time.AfterFunc(timeout, func() {
		if c.isAuthenticated() {
			return
		}
		_ = c.writeJSON(map[string]string{"reason": "Authentication timeout", "type": "auth_failed"})
		c.close(wsClosePolicy, "Authentication timeout")
		c.operation.remove(c)
	})
}

func (c *operationConnection) startHeartbeatTimer(timeout time.Duration) {
	c.heartbeatTimer = time.AfterFunc(timeout, func() {
		c.close(wsCloseNormal, "Heartbeat timeout")
		c.operation.remove(c)
	})
}

func (c *operationConnection) resetHeartbeatTimer(timeout time.Duration) {
	if c.heartbeatTimer != nil {
		c.heartbeatTimer.Reset(timeout)
	}
}

func (c *operationConnection) stopTimers() {
	if c.authTimer != nil {
		c.authTimer.Stop()
	}
	if c.heartbeatTimer != nil {
		c.heartbeatTimer.Stop()
	}
}

func (c *operationConnection) readLoop() {
	defer c.operation.remove(c)

	for {
		payload, err := c.ws.readMessage()
		if err != nil {
			return
		}
		var envelope struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(payload, &envelope); err != nil {
			continue
		}
		if envelope.Type == "auth" {
			c.handleAuth(payload)
			continue
		}
		if !c.isAuthenticated() {
			continue
		}
		c.handleAuthenticatedMessage(envelope.Type, payload)
	}
}

func (c *operationConnection) handleAuth(payload []byte) {
	if c.isAuthenticated() {
		return
	}
	var msg authMessage
	if err := json.Unmarshal(payload, &msg); err != nil {
		_ = c.writeJSON(map[string]string{"reason": err.Error(), "type": "auth_failed"})
		c.close(wsClosePolicy, err.Error())
		return
	}
	storedUserID := c.operation.storedUserID()
	verifiedUserID, err := c.operation.server.auth.resolve(context.Background(), storedUserID, msg)
	if err == nil && storedUserID != "" && verifiedUserID != storedUserID {
		err = errors.New("userId mismatch")
	}
	if err != nil {
		if errors.Is(err, errTokenExpired) {
			_ = c.writeJSON(map[string]string{"type": "auth_expired"})
			return
		}
		reason := err.Error()
		_ = c.writeJSON(map[string]string{"reason": reason, "type": "auth_failed"})
		c.close(wsClosePolicy, reason)
		return
	}
	if c.authTimer != nil {
		c.authTimer.Stop()
	}
	c.markAuthenticated(verifiedUserID, false)
	_ = c.writeJSON(map[string]string{"type": "auth_success"})
	c.startHeartbeatTimer(c.operation.server.heartbeatTimeout)
}

func (c *operationConnection) handleAuthenticatedMessage(messageType string, payload []byte) {
	switch messageType {
	case "resume":
		var msg struct {
			LastEventID string `json:"lastEventId"`
			WantStatus  bool   `json:"wantStatus"`
		}
		if json.Unmarshal(payload, &msg) == nil {
			c.operation.handleResume(c, msg.LastEventID, msg.WantStatus)
		}
	case "heartbeat":
		c.recordHeartbeat()
		c.resetHeartbeatTimer(c.operation.server.heartbeatTimeout)
		_ = c.writeJSON(map[string]string{"type": "heartbeat_ack"})
	case "interrupt":
		// Kept for protocol compatibility. The reference gateway has no backend
		// HTTP waiter for interrupt in the current API surface.
	case "tool_confirmation":
		var msg struct {
			Approved   bool   `json:"approved"`
			ToolCallID string `json:"toolCallId"`
		}
		if json.Unmarshal(payload, &msg) == nil {
			c.operation.resolveConfirmation(msg.ToolCallID, msg.Approved)
		}
	case "user_input":
		var msg struct {
			Content   string `json:"content"`
			RequestID string `json:"requestId"`
		}
		if json.Unmarshal(payload, &msg) == nil {
			c.operation.resolveInput(msg.RequestID, msg.Content)
		}
	case "tool_result":
		var msg toolResultMessage
		if json.Unmarshal(payload, &msg) == nil {
			go c.operation.forwardToolResult(msg)
		}
	}
}

func broadcast(connections []*operationConnection, msg any) {
	payload, err := json.Marshal(msg)
	if err != nil {
		return
	}
	for _, conn := range connections {
		_ = conn.writeRaw(payload)
	}
}

func randomID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return jsonNumberString(time.Now().UnixNano())
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return hex.EncodeToString(b[0:4]) + "-" + hex.EncodeToString(b[4:6]) + "-" + hex.EncodeToString(b[6:8]) + "-" + hex.EncodeToString(b[8:10]) + "-" + hex.EncodeToString(b[10:16])
}

func jsonNumberString[T ~int | ~int64](value T) string {
	return string(mustMarshal(value))
}

func mustMarshal(value any) []byte {
	payload, _ := json.Marshal(value)
	return payload
}

func jsonRawOrNil(raw json.RawMessage) any {
	if len(raw) == 0 {
		return nil
	}
	return raw
}
