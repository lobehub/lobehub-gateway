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
	"sort"
	"sync"
	"time"
)

const (
	eventBufferMax       = 1000
	eventBufferTrim      = 500
	eventBufferMaxBytes  = 16 << 20
	eventBufferTrimBytes = eventBufferMaxBytes / 2

	terminalResumeGrace = 750 * time.Millisecond
)

type queuedHubEvent struct {
	delivered     chan int
	isToolExecute bool
	payload       json.RawMessage
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

	cleanupTimer     *time.Timer
	connections      map[*operationConnection]struct{}
	eventBuffer      []bufferedEvent
	eventBufferBytes int
	eventCounter     int
	hubDispatch      bool
	hubOverflow      bool
	hubQueue         []queuedHubEvent
	lastEventAt      time.Time
	lastEventTyp     string
	mu               sync.RWMutex
	pendingConf      map[string]pendingConfirmation
	pendingInput     map[string]pendingInput
	record           operationRecord
	watchdog         *time.Timer
	openLLM          map[string]openLLMCall
}

func newOperation(server *Server, operationID string) *operation {
	return &operation{
		connections:  map[*operationConnection]struct{}{},
		openLLM:      map[string]openLLMCall{},
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

func (o *operation) init(operationID string, userID string, meta *operationMeta) {
	o.mu.Lock()
	o.record.OperationID = operationID
	o.record.UserID = userID
	o.record.Status = StatusRunning
	if meta != nil {
		o.record.Meta = cloneMeta(meta)
	}
	if o.record.CreatedAt.IsZero() {
		o.record.CreatedAt = time.Now()
	}
	o.stopCleanupLocked()
	o.lastEventAt = time.Now()
	o.lastEventTyp = "operation_init"
	o.scheduleWatchdogLocked()
	connections := make([]*operationConnection, 0, len(o.connections))
	for conn := range o.connections {
		connections = append(connections, conn)
	}
	o.mu.Unlock()
	for _, conn := range connections {
		conn.verifyPendingOwner(userID)
	}
	o.server.notifyLifecycle(o, StatusRunning, "")
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
	payload := o.bufferLocked(id, msg)
	o.lastEventAt = time.Now()
	o.lastEventTyp = event.Type
	o.scheduleWatchdogLocked()
	connections := o.authenticatedConnectionsLocked()
	o.queueHubEventLocked(msg)
	o.mu.Unlock()
	if event.Type != "message_patch" {
		broadcast(connections, payload)
	}

	if event.Type == "agent_runtime_end" {
		o.handleAgentRuntimeEnd(event)
	}
}

func (o *operation) broadcastToolExecute(event agentStreamEvent) {
	msg := map[string]any{"event": event, "type": "agent_event"}
	o.mu.Lock()
	id := o.nextEventIDLocked()
	msg["id"] = id
	payload := o.bufferLocked(id, msg)
	o.lastEventAt = time.Now()
	o.lastEventTyp = event.Type
	o.scheduleWatchdogLocked()
	connections := o.authenticatedConnectionsLocked()
	o.queueHubEventLocked(msg)
	o.mu.Unlock()
	broadcast(connections, payload)
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
	payload := o.bufferLocked(id, msg)
	connections := o.authenticatedConnectionsLocked()
	o.queueHubEventLocked(msg)
	o.scheduleCleanupLocked()
	o.mu.Unlock()
	broadcast(connections, payload)
	o.server.notifyLifecycle(o, status, summary)
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
	payload := o.bufferLocked(id, msg)
	o.queueHubEventLocked(msg)
	o.lastEventAt = time.Now()
	o.lastEventTyp = "status_change"
	o.scheduleWatchdogLocked()
	connections := o.authenticatedConnectionsLocked()
	if isTerminalStatus(status) {
		o.scheduleCleanupLocked()
	}
	o.mu.Unlock()
	broadcast(connections, payload)
	o.server.notifyLifecycle(o, status, summary)
}

func (o *operation) requestConfirmation(toolCallID string, tool toolCallInfo, timeout time.Duration) (bool, bool) {
	msg := map[string]any{"tool": tool, "toolCallId": toolCallID, "type": "tool_confirmation_request"}
	ch := make(chan bool, 1)
	timer := time.NewTimer(timeout)
	o.mu.Lock()
	id := o.nextEventIDLocked()
	msg["id"] = id
	payload := o.bufferLocked(id, msg)
	o.pendingConf[toolCallID] = pendingConfirmation{ch: ch, timer: timer}
	connections := o.authenticatedConnectionsLocked()
	o.queueHubEventLocked(msg)
	o.mu.Unlock()
	broadcast(connections, payload)

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
	payload := o.bufferLocked(id, msg)
	o.pendingInput[requestID] = pendingInput{ch: ch, timer: timer}
	connections := o.authenticatedConnectionsLocked()
	o.queueHubEventLocked(msg)
	o.mu.Unlock()
	broadcast(connections, payload)

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
	payloads, gap, status := o.collectReplay(lastEventID)
	// Mirror upstream AgentOperationDO.handleResume: only confirm a status we
	// actually hold. A missing status can mean "init hasn't landed yet" (the
	// init/resume race) just as much as "already cleaned up" — synthesizing a
	// terminal signal here would abort a run that is only just starting, and
	// emitting an out-of-enum value breaks the SessionStatus contract. The
	// opt-in client treats "no resume_complete" as "keep waiting" — live events
	// still stream and heartbeat loss still forces reconnect — which is safe
	// and recoverable. See LOBE-10443.
	for _, payload := range payloads {
		if !isMuxOnlyPayload(payload) && o.llmRecipient(conn, payload) && !conn.writeOrClose(payload) {
			return
		}
	}
	if wantStatus && status != "" {
		conn.writeResumeComplete(gap, status)
	}
}

// A v1 client that connects after completion may never send a resume request.
func (o *operation) replayFinished(conn *operationConnection, userID string) bool {
	o.mu.Lock()
	if _, attached := o.connections[conn]; !attached || o.record.UserID != userID || !isTerminalStatus(o.record.Status) {
		o.mu.Unlock()
		return false
	}
	payloads, gap, status := o.collectReplayLocked(0)
	operationID := o.record.OperationID
	o.mu.Unlock()

	gapReported := false
	for _, payload := range payloads {
		var message struct {
			Type  string `json:"type"`
			Event struct {
				Type        string `json:"type"`
				OperationID string `json:"operationId"`
			} `json:"event"`
		}
		if json.Unmarshal(payload, &message) != nil {
			continue
		}
		switch message.Type {
		case "agent_event":
			switch message.Event.Type {
			case "message_patch", "tool_execute":
				continue
			}
		case "session_complete":
		default:
			continue
		}

		terminal := message.Type == "session_complete" ||
			(message.Event.Type == "agent_runtime_end" &&
				(message.Event.OperationID == "" || message.Event.OperationID == operationID))
		if terminal && gap && !gapReported {
			if !conn.writeResumeComplete(true, status) {
				return false
			}
			gapReported = true
		}
		if !conn.writeOrClose(payload) {
			return false
		}
		if message.Type == "session_complete" {
			return true
		}
	}
	return gapReported || conn.writeResumeComplete(gap, status)
}

func (o *operation) collectReplay(lastEventID string) ([]json.RawMessage, bool, SessionStatus) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.collectReplayLocked(parseEventID(lastEventID))
}

func (o *operation) collectReplayLocked(since int) ([]json.RawMessage, bool, SessionStatus) {
	o.pruneLLMLocked()
	payloads := make([]json.RawMessage, 0, len(o.eventBuffer))
	for _, event := range o.eventBuffer {
		if parseEventID(event.ID) > since && o.replayableLLMLocked(event.Data) {
			payloads = append(payloads, event.Data)
		}
	}
	seen := map[int]bool{}
	for _, p := range payloads {
		seen[messageID(p)] = true
	}
	for _, call := range o.openLLM {
		id := messageID(call.payload)
		if id > since && !seen[id] {
			payloads = append(payloads, call.payload)
		}
	}
	sort.SliceStable(payloads, func(i, j int) bool { return messageID(payloads[i]) < messageID(payloads[j]) })
	return payloads, o.replayGapLocked(since), o.record.Status
}

func (o *operation) replayGapLocked(since int) bool {
	if len(o.eventBuffer) == 0 {
		return o.eventCounter > since
	}
	return since < parseEventID(o.eventBuffer[0].ID)-1
}

func (o *operation) hubSnapshot() (string, SessionStatus, *operationMeta) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.record.UserID, o.record.Status, cloneMeta(o.record.Meta)
}

func (o *operation) queueHubEventLocked(msg map[string]any) <-chan int {
	delivered := make(chan int, 1)
	payload, err := json.Marshal(msg)
	if err != nil {
		delivered <- 0
		return delivered
	}
	isToolExecute := false
	if event, ok := msg["event"].(agentStreamEvent); ok {
		isToolExecute = event.Type == "tool_execute"
	}
	if len(o.hubQueue) >= eventBufferMax {
		// A subscriber that cannot keep up must reconnect and replay from the
		// bounded operation buffer. Keeping an unbounded delivery queue here
		// would let one stalled socket consume memory indefinitely.
		for _, queued := range o.hubQueue {
			queued.delivered <- 0
		}
		o.hubQueue = nil
		o.hubOverflow = true
	}
	o.hubQueue = append(o.hubQueue, queuedHubEvent{isToolExecute: isToolExecute, payload: payload, delivered: delivered})
	if o.hubDispatch {
		return delivered
	}
	o.hubDispatch = true
	go o.drainHubEvents()
	return delivered
}

func (o *operation) drainHubEvents() {
	for {
		o.mu.Lock()
		if o.hubOverflow {
			o.hubOverflow = false
			for _, queued := range o.hubQueue {
				queued.delivered <- 0
			}
			o.hubQueue = nil
			operationID := o.record.OperationID
			o.mu.Unlock()
			o.server.closeOperationHubSubscribers(operationID)
			continue
		}
		if len(o.hubQueue) == 0 {
			o.hubQueue = nil
			o.hubDispatch = false
			o.mu.Unlock()
			return
		}
		event := o.hubQueue[0]
		o.hubQueue[0] = queuedHubEvent{}
		o.hubQueue = o.hubQueue[1:]
		o.mu.Unlock()
		event.delivered <- o.server.deliverOperationEvent(o, event.payload, event.isToolExecute)
	}
}

// bufferLocked stores immutable payloads and returns the same bytes for live delivery.
func (o *operation) bufferLocked(id string, msg map[string]any) []byte {
	payload, err := json.Marshal(msg)
	if err != nil {
		return nil
	}
	if len(payload) > eventBufferMaxBytes {
		// Keep a contiguous suffix for gap detection. Deliver this event live
		// even though it cannot be retained for replay.
		o.eventBuffer = nil
		o.eventBufferBytes = 0
		return payload
	}
	o.eventBuffer = append(o.eventBuffer, bufferedEvent{Data: payload, ID: id})
	o.eventBufferBytes += len(payload)
	if len(o.eventBuffer) > eventBufferMax || o.eventBufferBytes > eventBufferMaxBytes {
		drop := 0
		for drop < len(o.eventBuffer)-1 &&
			(len(o.eventBuffer)-drop > eventBufferTrim || o.eventBufferBytes > eventBufferTrimBytes) {
			o.eventBufferBytes -= len(o.eventBuffer[drop].Data)
			drop++
		}
		o.eventBuffer = append([]bufferedEvent(nil), o.eventBuffer[drop:]...)
	}
	return payload
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
	timeout := o.watchdogTimeoutLocked()
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
	timeout := o.watchdogTimeoutLocked()
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
	currentTimeout := o.watchdogTimeoutLocked()
	if time.Since(o.lastEventAt) < currentTimeout {
		o.scheduleWatchdogLocked()
		o.mu.Unlock()
		return
	}
	summary := watchdogSummary(idle)
	id := o.nextEventIDLocked()
	var msg map[string]any
	var status SessionStatus
	if abandoned {
		status = StatusError
		o.record.Status = status
		msg = map[string]any{"id": id, "status": StatusError, "type": "status_change"}
	} else {
		status = StatusCompleted
		o.record.Status = status
		msg = map[string]any{"id": id, "summary": summary, "type": "session_complete"}
	}
	payload := o.bufferLocked(id, msg)
	connections := o.authenticatedConnectionsLocked()
	o.queueHubEventLocked(msg)
	o.scheduleCleanupLocked()
	o.mu.Unlock()
	broadcast(connections, payload)
	o.server.notifyLifecycle(o, status, summary)
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
	msg.Type = ""
	body, _ := json.Marshal(msg)
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
	o.server.notifyLifecycle(o, "gone", "")
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

	// Owned by the read loop. The timer sends its result before a late
	// resume can decide whether any history still needs to be replayed.
	terminalReplayTimer *time.Timer
	terminalReplayDone  chan bool
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

func (c *operationConnection) markAuthenticated(userID string, isAdmin bool, pendingOwner bool, clientID ...string) {
	c.mu.Lock()
	c.att.Authenticated = true
	if len(clientID) > 0 {
		c.att.ClientID = clientID[0]
	}
	c.att.IsAdmin = isAdmin
	c.att.LastHeartbeat = time.Now().UnixMilli()
	c.att.PendingOwner = pendingOwner
	c.att.UserID = userID
	c.mu.Unlock()
}

func (o *operation) authenticateConnection(c *operationConnection, userID string, clientID ...string) (bool, error) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	storedUserID := o.record.UserID
	if storedUserID != "" && storedUserID != userID {
		return false, errors.New("userId mismatch")
	}
	c.markAuthenticated(userID, false, storedUserID == "", clientID...)
	return storedUserID != "" && isTerminalStatus(o.record.Status), nil
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

func (c *operationConnection) writeResumeComplete(gap bool, status SessionStatus) bool {
	return c.writeOrClose(mustMarshal(map[string]any{
		"gap": gap, "status": status, "type": "resume_complete",
	}))
}

func (c *operationConnection) writeRaw(payload []byte) error {
	return c.ws.writeJSON(payload)
}

func (c *operationConnection) writeOrClose(payload []byte) bool {
	if err := c.writeRaw(payload); err != nil {
		_ = c.ws.close()
		return false
	}
	return true
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
	defer func() {
		if c.terminalReplayTimer != nil {
			c.terminalReplayTimer.Stop()
		}
	}()

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
	var finished bool
	if err == nil {
		finished, err = c.operation.authenticateConnection(c, verifiedUserID, msg.ClientID)
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
	if !c.writeOrClose(mustMarshal(map[string]string{"type": "auth_success"})) {
		return
	}
	c.startHeartbeatTimer(c.operation.server.heartbeatTimeout)
	if finished {
		done := make(chan bool, 1)
		c.terminalReplayDone = done
		c.terminalReplayTimer = time.AfterFunc(terminalResumeGrace, func() {
			done <- c.operation.replayFinished(c, verifiedUserID)
		})
	}
}

func (c *operationConnection) verifyPendingOwner(userID string) {
	c.mu.Lock()
	if !c.att.PendingOwner {
		c.mu.Unlock()
		return
	}
	if c.att.UserID == userID {
		c.att.PendingOwner = false
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()
	c.close(wsClosePolicy, "userId mismatch")
	c.operation.remove(c)
}

func (c *operationConnection) handleAuthenticatedMessage(messageType string, payload []byte) {
	switch messageType {
	case "resume":
		var msg struct {
			LastEventID string `json:"lastEventId"`
			WantStatus  bool   `json:"wantStatus"`
		}
		if json.Unmarshal(payload, &msg) != nil {
			return
		}
		if timer := c.terminalReplayTimer; timer != nil {
			c.terminalReplayTimer = nil
			if !timer.Stop() && <-c.terminalReplayDone {
				if msg.WantStatus {
					c.operation.mu.RLock()
					gap := c.operation.replayGapLocked(parseEventID(msg.LastEventID))
					status := c.operation.record.Status
					c.operation.mu.RUnlock()
					if status != "" {
						c.writeResumeComplete(gap, status)
					}
				}
				return
			}
		}
		c.operation.handleResume(c, msg.LastEventID, msg.WantStatus)
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

func broadcast(connections []*operationConnection, payload []byte) {
	if payload == nil {
		return
	}
	for _, conn := range connections {
		conn.writeOrClose(payload)
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

func isMuxOnlyPayload(payload json.RawMessage) bool {
	var message struct {
		Event *struct {
			Type string `json:"type"`
		} `json:"event"`
		Type string `json:"type"`
	}
	return json.Unmarshal(payload, &message) == nil && message.Type == "agent_event" && message.Event != nil && message.Event.Type == "message_patch"
}

func parseEventID(value string) int {
	var parsed int
	_, _ = fmt.Sscanf(value, "%d", &parsed)
	return parsed
}

func cloneMeta(meta *operationMeta) *operationMeta {
	if meta == nil {
		return nil
	}
	cloned := *meta
	return &cloned
}
