package gateway

import "encoding/json"

type DeviceAttachment struct {
	Authenticated bool   `json:"authenticated"`
	ConnectedAt   int64  `json:"connectedAt"`
	DeviceID      string `json:"deviceId"`
	Hostname      string `json:"hostname"`
	LastHeartbeat int64  `json:"lastHeartbeat"`
	Platform      string `json:"platform"`
}

type authMessage struct {
	ServerURL string `json:"serverUrl,omitempty"`
	Token     string `json:"token"`
	TokenType string `json:"tokenType,omitempty"`
	Type      string `json:"type"`
}

type rpcEnvelope struct {
	OperationID string          `json:"operationId,omitempty"`
	Reason      string          `json:"reason,omitempty"`
	RequestID   string          `json:"requestId,omitempty"`
	Result      json.RawMessage `json:"result,omitempty"`
	Status      string          `json:"status,omitempty"`
	Type        string          `json:"type"`
}

type deviceHTTPBody struct {
	AgentType       string          `json:"agentType,omitempty"`
	CWD             string          `json:"cwd,omitempty"`
	DeviceID        string          `json:"deviceId,omitempty"`
	JWT             string          `json:"jwt,omitempty"`
	OperationID     string          `json:"operationId,omitempty"`
	Path            string          `json:"path,omitempty"`
	Content         string          `json:"content,omitempty"`
	Prompt          string          `json:"prompt,omitempty"`
	ResumeSessionID string          `json:"resumeSessionId,omitempty"`
	Timeout         int             `json:"timeout,omitempty"`
	ToolCall        json.RawMessage `json:"toolCall,omitempty"`
	TopicID         string          `json:"topicId,omitempty"`
	UserID          string          `json:"userId"`
}
