package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"
)

type connection struct {
	att            DeviceAttachment
	authTimeout    time.Duration
	heartbeatTimer *time.Timer
	hub            *hub
	writeMu        sync.Mutex
	ws             *wsConn
}

func (c *connection) writeJSON(v any) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	payload, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return c.ws.writeJSON(payload)
}

func (c *connection) writeText(payload []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.ws.writeJSON(payload)
}

func (c *connection) close(code int, reason string) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_ = c.ws.writeClose(code, reason)
	_ = c.ws.close()
}

func (c *connection) startAuthTimer() {
	time.AfterFunc(c.authTimeout, func() {
		if c.hub.isAuthenticated(c) {
			return
		}
		_ = c.writeJSON(map[string]string{"reason": "Authentication timeout", "type": "auth_failed"})
		c.close(wsClosePolicy, "Authentication timeout")
		c.hub.remove(c)
	})
}

func (c *connection) startHeartbeatTimer(timeout time.Duration) {
	c.heartbeatTimer = time.AfterFunc(timeout, func() {
		c.close(wsCloseNormal, "Heartbeat timeout")
		c.hub.remove(c)
	})
}

func (c *connection) resetHeartbeatTimer(timeout time.Duration) {
	if c.heartbeatTimer != nil {
		c.heartbeatTimer.Reset(timeout)
	}
}

func (c *connection) readLoop(auth *authResolver, heartbeatTimeout time.Duration) {
	defer c.hub.remove(c)

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
			if c.hub.isAuthenticated(c) {
				continue
			}
			var msg authMessage
			if err := json.Unmarshal(payload, &msg); err != nil {
				_ = c.writeJSON(map[string]string{"reason": err.Error(), "type": "auth_failed"})
				c.close(wsClosePolicy, err.Error())
				return
			}
			storedUserID := c.hub.userID
			if c.att.WorkspaceID != "" {
				storedUserID = c.att.UserID
			}
			verifiedUserID, err := auth.resolve(context.Background(), storedUserID, msg)
			if err == nil && storedUserID != "" && verifiedUserID != storedUserID {
				err = errUserIDMismatch
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

			c.hub.markAuthenticated(c)
			_ = c.writeJSON(map[string]string{"type": "auth_success"})
			c.startHeartbeatTimer(heartbeatTimeout)
			continue
		}

		if !c.hub.isAuthenticated(c) {
			continue
		}

		switch envelope.Type {
		case "heartbeat":
			c.hub.recordHeartbeat(c)
			c.resetHeartbeatTimer(heartbeatTimeout)
			_ = c.writeJSON(map[string]string{"type": "heartbeat_ack"})
		case "tool_call_response", "message_api_response", "system_info_response", "rpc_response", "agent_run_ack":
			var msg rpcEnvelope
			if err := json.Unmarshal(payload, &msg); err == nil {
				c.hub.resolvePending(msg)
			}
		}
	}
}
