package models

import (
	"encoding/json"
	"strings"
)

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

type ModelDef struct {
	ID        string
	Name      string
	Context   int
	MaxTokens int
}

var Models = []ModelDef{
	{"mimo-v2.5-pro", "MiMo V2.5 Pro", 1048576, 131072},
	{"mimo-v2.5", "MiMo V2.5", 1048576, 131072},
	{"mimo-v2.5-tts", "MiMo V2.5 TTS", 8192, 8192},
	{"mimo-v2.5-tts-voicedesign", "MiMo V2.5 TTS VoiceDesign", 8192, 8192},
	{"mimo-v2.5-tts-voiceclone", "MiMo V2.5 TTS VoiceClone", 8192, 8192},
	{"mimo-v2.5-pro-ultraspeed", "MiMo V2.5 Pro UltraSpeed", 1048576, 131072},
}

var modelIDsByLower = buildModelIDsByLower()

func buildModelIDsByLower() map[string]string {
	result := make(map[string]string, len(Models))
	for _, model := range Models {
		result[strings.ToLower(model.ID)] = model.ID
	}
	return result
}

func CanonicalModelID(modelID string) (string, bool) {
	canonical, ok := modelIDsByLower[strings.ToLower(strings.TrimSpace(modelID))]
	return canonical, ok
}

// Claw API WebSocket Models
type ClawWSMessage struct {
	Type    string          `json:"type"`            // "event", "req", "res"
	Event   string          `json:"event,omitempty"` // "connect.challenge", etc.
	ID      string          `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	OK      *bool           `json:"ok,omitempty"`
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
	UserID       string  `json:"userId"`
	Name         string  `json:"name"`
	ServiceToken string  `json:"serviceToken"`
	SessionKey   string  `json:"sessionKey"`
	PH           string  `json:"xiaomichatbot_ph"`
	AddedAt      float64 `json:"addedAt"`
	Status       string  `json:"claw_status"`
	RemainSec    float64 `json:"remain_sec"`
	LastRefresh  float64 `json:"lastRefresh"`
	DailyLimitAt float64 `json:"dailyLimitAt,omitempty"` // 北京时间当日触发429限额的时间戳，0点重置后自动清零
}
