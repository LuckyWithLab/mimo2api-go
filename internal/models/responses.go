package models

// Responses API Output Items
type RespMessageItem struct {
	ID      string        `json:"id,omitempty"`
	Type    string        `json:"type"` // "message"
	Role    string        `json:"role"`
	Status  string        `json:"status"` // "in_progress", "completed"
	Content []interface{} `json:"content"`
}

type RespReasoningItem struct {
	ID               string   `json:"id,omitempty"`
	Type             string   `json:"type"` // "reasoning"
	Status           string   `json:"status"`
	Summary          []string `json:"summary"`
	ReasoningContent string   `json:"reasoning_content,omitempty"`
	EncryptedContent string   `json:"encrypted_content,omitempty"`
}

type RespFunctionCallItem struct {
	ID        string `json:"id,omitempty"`
	Type      string `json:"type"` // "function_call"
	CallID    string `json:"call_id"`
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type ResponseUsage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
	TotalTokens  int `json:"total_tokens"`
}

type ResponsesAPIResponse struct {
	ID        string         `json:"id"`
	Object    string         `json:"object,omitempty"`
	Model     string         `json:"model"`
	CreatedAt int64          `json:"created_at"`
	Status    string         `json:"status"`
	Output    []interface{}  `json:"output"`
	Usage     *ResponseUsage `json:"usage,omitempty"`
}
