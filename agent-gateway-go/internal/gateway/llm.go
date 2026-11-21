package gateway

import (
	"encoding/json"
	"net/http"
	"time"
)

type llmData struct {
	CallID            string `json:"callId"`
	PreferredClientID string `json:"preferredClientId"`
	StepIndex         int    `json:"stepIndex"`
	Deadlines         struct {
		TotalMS float64 `json:"totalMs"`
	} `json:"deadlines"`
}

type llmEvent struct {
	Type string  `json:"type"`
	Data llmData `json:"data"`
}

type openLLMCall struct {
	expiresAt time.Time
	payload   json.RawMessage
}

func llmPayload(payload json.RawMessage) llmEvent {
	var message struct {
		Type  string   `json:"type"`
		Event llmEvent `json:"event"`
	}
	if json.Unmarshal(payload, &message) != nil || message.Type != "agent_event" {
		return llmEvent{}
	}
	return message.Event
}

func (o *operation) watchdogTimeoutLocked() time.Duration {
	if o.record.Meta != nil && o.record.Meta.HeteroType != "" {
		return defaultHeteroWatchdog
	}
	return defaultIdleWatchdog
}

func (o *operation) pruneLLMLocked() {
	for key, call := range o.openLLM {
		if isTerminalStatus(o.record.Status) || !call.expiresAt.After(time.Now()) {
			delete(o.openLLM, key)
		}
	}
}

func (o *operation) replayableLLMLocked(payload json.RawMessage) bool {
	event := llmPayload(payload)
	if event.Type != "llm_execute" {
		return true
	}
	_, ok := o.openLLM[event.Data.CallID]
	return ok
}

func (o *operation) llmConnectionsLocked(preferred string) []*operationConnection {
	all := o.authenticatedConnectionsLocked()
	if preferred == "" {
		return all
	}
	found := false
	for _, conn := range all {
		conn.mu.RLock()
		found = found || (!conn.att.IsAdmin && conn.att.ClientID == preferred)
		conn.mu.RUnlock()
	}
	if !found {
		return all
	}
	chosen := make([]*operationConnection, 0, len(all))
	for _, conn := range all {
		conn.mu.RLock()
		if conn.att.IsAdmin || conn.att.ClientID == preferred {
			chosen = append(chosen, conn)
		}
		conn.mu.RUnlock()
	}
	return chosen
}

func (o *operation) llmRecipient(conn *operationConnection, payload json.RawMessage) bool {
	event := llmPayload(payload)
	if event.Type != "llm_execute" {
		return true
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	for _, target := range o.llmConnectionsLocked(event.Data.PreferredClientID) {
		if target == conn {
			return true
		}
	}
	return false
}

func (s *Server) handleLLM(w http.ResponseWriter, r *http.Request, body operationHTTPBody) {
	var data llmData
	_ = json.Unmarshal(body.Data, &data)
	closing := r.URL.Path == "/api/operations/llm-close"
	callID := data.CallID
	if closing {
		callID = body.CallID
	}
	if callID == "" {
		message := "Missing operationId or data.callId"
		if closing {
			message = "Missing operationId or callId"
		}
		writeText(w, 400, message)
		return
	}
	op := s.getOrCreateOperation(body.OperationID)
	op.mu.Lock()
	revived := false
	if r.URL.Path == "/api/operations/llm-execute" {
		// An LLM attempt is a step running: it reopens a run whose session
		// ended on a mirrored terminal (see reviveLocked). A finished run
		// still refuses it.
		revived = op.reviveLocked()
		if isTerminalStatus(op.record.Status) {
			op.mu.Unlock()
			writeJSON(w, 409, map[string]any{"delivered": 0, "error": "OPERATION_ENDED"})
			return
		}
	}
	delete(op.openLLM, callID)
	if closing {
		op.mu.Unlock()
		writeJSON(w, 200, map[string]any{"success": true})
		return
	}
	eventType := "llm_cancel"
	if r.URL.Path == "/api/operations/llm-execute" {
		eventType = "llm_execute"
	}
	event := agentStreamEvent{Data: body.Data, OperationID: body.OperationID, StepIndex: data.StepIndex, Timestamp: time.Now().UnixMilli(), Type: eventType}
	id := op.nextEventIDLocked()
	message := map[string]any{"event": event, "id": id, "type": "agent_event"}
	if eventType == "llm_execute" {
		ttl := 10 * time.Minute
		if data.Deadlines.TotalMS > 0 {
			ttl = time.Duration(data.Deadlines.TotalMS * float64(time.Millisecond))
		}
		op.openLLM[callID] = openLLMCall{expiresAt: time.Now().Add(ttl), payload: mustMarshal(message)}
	}
	op.bufferLocked(id, message)
	op.lastEventAt, op.lastEventTyp = time.Now(), eventType
	op.scheduleWatchdogLocked()
	connections := op.authenticatedConnectionsLocked()
	if eventType == "llm_execute" {
		connections = op.llmConnectionsLocked(data.PreferredClientID)
	}
	hubDelivered := op.queueHubEventLocked(message)
	op.mu.Unlock()
	if revived {
		s.notifyLifecycle(op, StatusRunning, "")
	}
	delivered := 0
	for _, conn := range connections {
		if conn.writeJSON(message) == nil && !conn.isAdmin() {
			delivered++
		}
	}
	if eventType == "llm_execute" {
		timer := time.NewTimer(5 * time.Second)
		defer timer.Stop()
		select {
		case count := <-hubDelivered:
			delivered += count
		case <-timer.C:
		}
		writeJSON(w, 200, map[string]any{"success": true, "delivered": delivered})
	} else {
		writeJSON(w, 200, map[string]any{"success": true})
	}
}
