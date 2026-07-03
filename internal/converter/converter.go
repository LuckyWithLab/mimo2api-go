package converter

import (
	"encoding/json"
	"fmt"
)

// ConvertRequest transforms an OpenAI-compatible request body to the internal bridge format.
func ConvertRequest(body []byte) (map[string]interface{}, error) {
	var req map[string]interface{}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	if _, ok := req["stream"]; !ok {
		req["stream"] = true
	}

	return req, nil
}

func ResponsesConvertRequest(req map[string]interface{}) (map[string]interface{}, error) {
	chatReq := cloneMap(req)
	var chatMessages []map[string]interface{}

	if instructions, ok := req["instructions"].(string); ok && instructions != "" {
		chatMessages = append(chatMessages, map[string]interface{}{
			"role":    "system",
			"content": instructions,
		})
	}

	switch input := req["input"].(type) {
	case string:
		if input != "" {
			chatMessages = append(chatMessages, map[string]interface{}{
				"role":    "user",
				"content": input,
			})
		}
	case []interface{}:
		chatMessages = append(chatMessages, convertResponseInputItems(input)...)
	}

	if len(chatMessages) == 0 {
		if existing, ok := req["messages"].([]interface{}); ok {
			chatReq["messages"] = existing
		}
	} else {
		chatReq["messages"] = chatMessages
	}

	if maxOutput, ok := chatReq["max_output_tokens"]; ok {
		chatReq["max_tokens"] = maxOutput
		delete(chatReq, "max_output_tokens")
	}

	for _, key := range []string{"instructions", "input", "store", "previous_response_id"} {
		delete(chatReq, key)
	}

	if tools, ok := req["tools"].([]interface{}); ok {
		var convertedTools []map[string]interface{}
		for _, raw := range tools {
			tool, ok := raw.(map[string]interface{})
			if !ok {
				continue
			}
			if toolType, _ := tool["type"].(string); toolType != "function" {
				continue
			}
			convertedTools = append(convertedTools, map[string]interface{}{
				"type": "function",
				"function": map[string]interface{}{
					"name":        asString(tool["name"]),
					"description": asString(tool["description"]),
					"parameters":  defaultMap(tool["parameters"]),
				},
			})
		}
		chatReq["tools"] = convertedTools
	}

	return chatReq, nil
}

func ConvertResponsesResponse(chatResp map[string]interface{}) map[string]interface{} {
	respID := generateID("resp")
	now := nowUnix()
	output := make([]interface{}, 0)

	choices, _ := chatResp["choices"].([]interface{})
	var message map[string]interface{}
	if len(choices) > 0 {
		if choice, ok := choices[0].(map[string]interface{}); ok {
			message, _ = choice["message"].(map[string]interface{})
		}
	}

	if reasoning := asString(message["reasoning_content"]); reasoning != "" {
		output = append(output, map[string]interface{}{
			"id":                generateID("rs"),
			"type":              "reasoning",
			"status":            "completed",
			"summary":           []interface{}{},
			"encrypted_content": reasoning,
		})
	}

	contentParts := make([]interface{}, 0)
	if content := asString(message["content"]); content != "" {
		contentParts = append(contentParts, map[string]interface{}{
			"type":        "output_text",
			"text":        content,
			"annotations": []interface{}{},
		})
	}
	if refusal := asString(message["refusal"]); refusal != "" {
		contentParts = append(contentParts, map[string]interface{}{
			"type":    "refusal",
			"refusal": refusal,
		})
	}
	if len(contentParts) > 0 {
		output = append(output, map[string]interface{}{
			"id":      generateID("msg"),
			"type":    "message",
			"role":    "assistant",
			"status":  "completed",
			"content": contentParts,
		})
	}

	if toolCalls, ok := message["tool_calls"].([]interface{}); ok {
		for _, raw := range toolCalls {
			tc, ok := raw.(map[string]interface{})
			if !ok {
				continue
			}
			function, _ := tc["function"].(map[string]interface{})
			callID := asString(tc["id"])
			if callID == "" {
				callID = generateID("call")
			}
			output = append(output, map[string]interface{}{
				"id":        generateID("fc"),
				"type":      "function_call",
				"call_id":   callID,
				"name":      asString(function["name"]),
				"arguments": stringifyToolPayload(function["arguments"]),
			})
		}
	}

	resp := map[string]interface{}{
		"id":         respID,
		"object":     "response",
		"created_at": now,
		"model":      asString(chatResp["model"]),
		"output":     output,
		"status":     "completed",
	}

	if usage, ok := chatResp["usage"].(map[string]interface{}); ok && len(usage) > 0 {
		resp["usage"] = map[string]interface{}{
			"input_tokens":  asInt(usage["prompt_tokens"]),
			"output_tokens": asInt(usage["completion_tokens"]),
			"total_tokens":  asInt(usage["total_tokens"]),
		}
	}

	return resp
}

// StreamConverter processes SSE chunks.
type StreamConverter struct {
	Model string
}

func NewStreamConverter(model string) *StreamConverter {
	return &StreamConverter{Model: model}
}

func (s *StreamConverter) ProcessChunk(chunk []byte) ([]byte, error) {
	return chunk, nil
}

func convertResponseInputItems(items []interface{}) []map[string]interface{} {
	result := make([]map[string]interface{}, 0)
	var pendingReasoning string

	for _, raw := range items {
		item, ok := raw.(map[string]interface{})
		if !ok {
			result = append(result, map[string]interface{}{
				"role":    "user",
				"content": fmt.Sprint(raw),
			})
			continue
		}

		itemType := asString(item["type"])
		if itemType == "" && (item["role"] != nil || item["content"] != nil) {
			role := normalizeRole(asString(item["role"]))
			msg := map[string]interface{}{
				"role":    role,
				"content": extractMessageContent(item["content"]),
			}
			if role == "assistant" && pendingReasoning != "" {
				msg["reasoning_content"] = pendingReasoning
				pendingReasoning = ""
			}
			result = append(result, msg)
			continue
		}

		switch itemType {
		case "reasoning":
			pendingReasoning += firstNonEmptyString(asString(item["reasoning_content"]), asString(item["encrypted_content"]))
		case "message":
			role := normalizeRole(asString(item["role"]))
			msg := map[string]interface{}{
				"role":    role,
				"content": extractMessageContent(item["content"]),
			}
			if role == "assistant" && pendingReasoning != "" {
				msg["reasoning_content"] = pendingReasoning
				pendingReasoning = ""
			}
			result = append(result, msg)
		case "function_call", "custom_tool_call":
			callID := firstNonEmptyString(asString(item["call_id"]), asString(item["id"]), generateID("call"))
			toolCall := map[string]interface{}{
				"id":   callID,
				"type": "function",
				"function": map[string]interface{}{
					"name":      firstNonEmptyString(asString(item["name"]), "function_call"),
					"arguments": stringifyToolPayload(firstNonNil(item["arguments"], item["input"], item["content"])),
				},
			}
			if len(result) > 0 && asString(result[len(result)-1]["role"]) == "assistant" {
				last := result[len(result)-1]
				toolCalls, _ := last["tool_calls"].([]interface{})
				last["tool_calls"] = append(toolCalls, toolCall)
				if pendingReasoning != "" {
					last["reasoning_content"] = asString(last["reasoning_content"]) + pendingReasoning
					pendingReasoning = ""
				}
			} else {
				msg := map[string]interface{}{
					"role":       "assistant",
					"tool_calls": []interface{}{toolCall},
				}
				if pendingReasoning != "" {
					msg["reasoning_content"] = pendingReasoning
					pendingReasoning = ""
				}
				result = append(result, msg)
			}
		case "function_call_output", "custom_tool_call_output":
			result = append(result, map[string]interface{}{
				"role":         "tool",
				"tool_call_id": firstNonEmptyString(asString(item["call_id"]), asString(item["id"]), generateID("call")),
				"content":      stringifyToolPayload(firstNonNil(item["output"], item["content"])),
			})
		}
	}

	return result
}

func extractMessageContent(content interface{}) interface{} {
	switch v := content.(type) {
	case string:
		return v
	case []interface{}:
		parts := make([]map[string]interface{}, 0)
		for _, raw := range v {
			switch part := raw.(type) {
			case string:
				parts = append(parts, map[string]interface{}{"type": "text", "text": part})
			case map[string]interface{}:
				partType := asString(part["type"])
				switch partType {
				case "input_text", "output_text", "text":
					parts = append(parts, map[string]interface{}{"type": "text", "text": asString(part["text"])})
				case "input_image":
					url := firstNonEmptyString(
						asNestedString(part["image_url"], "url"),
						asString(part["url"]),
						asString(part["image_url"]),
					)
					if url != "" {
						parts = append(parts, map[string]interface{}{
							"type": "image_url",
							"image_url": map[string]interface{}{
								"url": url,
							},
						})
					}
				case "input_file":
					parts = append(parts, map[string]interface{}{
						"type": "text",
						"text": fmt.Sprintf("[Attached File: %s]", firstNonEmptyString(asString(part["filename"]), "unknown")),
					})
				default:
					encoded, _ := json.Marshal(part)
					parts = append(parts, map[string]interface{}{"type": "text", "text": string(encoded)})
				}
			}
		}
		if len(parts) == 1 && asString(parts[0]["type"]) == "text" {
			return asString(parts[0]["text"])
		}
		return parts
	default:
		if v == nil {
			return ""
		}
		return fmt.Sprint(v)
	}
}

func cloneMap(src map[string]interface{}) map[string]interface{} {
	dst := make(map[string]interface{}, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func stringifyToolPayload(value interface{}) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	default:
		encoded, _ := json.Marshal(v)
		return string(encoded)
	}
}

func defaultMap(value interface{}) map[string]interface{} {
	if m, ok := value.(map[string]interface{}); ok {
		return m
	}
	return map[string]interface{}{}
}

func normalizeRole(role string) string {
	switch role {
	case "system", "user", "assistant", "developer":
		return role
	default:
		return "user"
	}
}

func asNestedString(value interface{}, key string) string {
	if m, ok := value.(map[string]interface{}); ok {
		return asString(m[key])
	}
	return ""
}

func asString(value interface{}) string {
	if s, ok := value.(string); ok {
		return s
	}
	return ""
}

func asInt(value interface{}) int {
	switch v := value.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	default:
		return 0
	}
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func firstNonNil(values ...interface{}) interface{} {
	for _, value := range values {
		if value != nil {
			return value
		}
	}
	return nil
}
