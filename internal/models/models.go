package models

import "encoding/json"

// OpenAI API Models
type ChatMessage struct {
	Role    string      `json:"role"`
	Content interface{} `json:"content"` // can be string or []map[string]interface{}
}

type ChatCompletionRequest struct {
	Model       string        `json:"model"`
	Messages    []ChatMessage `json:"messages"`
	Stream      bool          `json:"stream,omitempty"`
	Temperature *float64      `json:"temperature,omitempty"`
	TopP        *float64      `json:"top_p,omitempty"`
	MaxTokens   *int          `json:"max_tokens,omitempty"`
}

// Claw API WebSocket Models
type ClawWSMessage struct {
	Type   string          `json:"type"`             // "event", "req", "res"
	Event  string          `json:"event,omitempty"`  // "connect.challenge", etc.
	ID     string          `json:"id,omitempty"`
	Method string          `json:"method,omitempty"`
	Params json.RawMessage `json:"params,omitempty"`
	OK     *bool           `json:"ok,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

type BridgeWSPayload struct {
	Type   string `json:"type,omitempty"`
	ReqID  string `json:"req_id,omitempty"`
	Method string `json:"method,omitempty"`
	Path   string `json:"path,omitempty"`
	Body   string `json:"body,omitempty"`
}

// User Record for WebUI and Manager
type UserRecord struct {
	UserID         string  `json:"userId"`
	Name           string  `json:"name"`
	ServiceToken   string  `json:"serviceToken"`
	SessionKey     string  `json:"sessionKey"`
	PH             string  `json:"xiaomichatbot_ph"`
	AddedAt        float64 `json:"addedAt"`
	Status         string  `json:"claw_status"`
	RemainSec      float64 `json:"remain_sec"`
	LastRefresh    float64 `json:"lastRefresh"`
}
