package converter

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestResponsesStreamConverterReassemblesSSEChunksAndEmitsItemIDs(t *testing.T) {
	converter := NewResponsesStreamConverter("mimo-test")

	firstEvent := encodeChatSSEChunk(t, map[string]interface{}{
		"choices": []interface{}{
			map[string]interface{}{
				"delta": map[string]interface{}{
					"role":    "assistant",
					"content": "Hello",
				},
			},
		},
	})
	finishEvent := encodeChatSSEChunk(t, map[string]interface{}{
		"choices": []interface{}{
			map[string]interface{}{
				"finish_reason": "stop",
			},
		},
	})

	splitAt := strings.Index(firstEvent, "Hello") + 2
	if splitAt <= 0 || splitAt >= len(firstEvent) {
		t.Fatal("failed to create split test fixture")
	}

	if events := converter.ProcessChunk(firstEvent[:splitAt]); len(events) != 0 {
		t.Fatalf("expected no events from partial SSE frame, got %d", len(events))
	}

	events := converter.ProcessChunk(firstEvent[splitAt:] + finishEvent)
	events = append(events, converter.ProcessChunk("data: [DONE]\n\n")...)

	parsed := parseSSEEvents(t, events)
	assertSequenceNumbers(t, parsed)

	textDelta := findEvent(t, parsed, "response.output_text.delta")
	textDone := findEvent(t, parsed, "response.output_text.done")
	partAdded := findEvent(t, parsed, "response.content_part.added")
	partDone := findEvent(t, parsed, "response.content_part.done")
	completed := findEvent(t, parsed, "response.completed")

	itemID := mustStringField(t, textDelta.Payload, "item_id")
	if itemID == "" {
		t.Fatal("expected output text delta to include item_id")
	}
	if got := mustStringField(t, textDone.Payload, "item_id"); got != itemID {
		t.Fatalf("expected output text done item_id %q, got %q", itemID, got)
	}
	if got := mustStringField(t, partAdded.Payload, "item_id"); got != itemID {
		t.Fatalf("expected content_part.added item_id %q, got %q", itemID, got)
	}
	if got := mustStringField(t, partDone.Payload, "item_id"); got != itemID {
		t.Fatalf("expected content_part.done item_id %q, got %q", itemID, got)
	}

	response := mustMapField(t, completed.Payload, "response")
	output := mustSliceField(t, response, "output")
	if len(output) != 1 {
		t.Fatalf("expected completed response to contain one output item, got %d", len(output))
	}
	message := mustMap(t, output[0])
	if got := mustStringField(t, message, "id"); got != itemID {
		t.Fatalf("expected completed message id %q, got %q", itemID, got)
	}
}

func TestResponsesStreamConverterKeepsReasoningIDStable(t *testing.T) {
	converter := NewResponsesStreamConverter("mimo-test")

	events := converter.ProcessChunk(encodeChatSSEChunk(t, map[string]interface{}{
		"choices": []interface{}{
			map[string]interface{}{
				"delta": map[string]interface{}{
					"role":              "assistant",
					"reasoning_content": "pondering",
				},
			},
		},
	}))
	events = append(events, converter.ProcessChunk(encodeChatSSEChunk(t, map[string]interface{}{
		"choices": []interface{}{
			map[string]interface{}{
				"delta": map[string]interface{}{
					"content": "answer",
				},
				"finish_reason": "stop",
			},
		},
	}))...)
	events = append(events, converter.ProcessChunk("data: [DONE]\n\n")...)

	parsed := parseSSEEvents(t, events)
	assertSequenceNumbers(t, parsed)

	reasoningAdded := findOutputItemEvent(t, parsed, "response.output_item.added", "reasoning")
	reasoningDone := findOutputItemEvent(t, parsed, "response.output_item.done", "reasoning")
	completed := findEvent(t, parsed, "response.completed")

	addedID := mustStringField(t, mustMapField(t, reasoningAdded.Payload, "item"), "id")
	doneID := mustStringField(t, mustMapField(t, reasoningDone.Payload, "item"), "id")
	if addedID != doneID {
		t.Fatalf("expected stable reasoning id, added=%q done=%q", addedID, doneID)
	}

	response := mustMapField(t, completed.Payload, "response")
	output := mustSliceField(t, response, "output")
	if len(output) < 1 {
		t.Fatal("expected completed response output to contain reasoning item")
	}
	reasoning := mustMap(t, output[0])
	if got := mustStringField(t, reasoning, "id"); got != addedID {
		t.Fatalf("expected completed reasoning id %q, got %q", addedID, got)
	}
}

func TestResponsesStreamConverterEmitsToolCallArgumentLifecycle(t *testing.T) {
	converter := NewResponsesStreamConverter("mimo-test")

	events := converter.ProcessChunk(encodeChatSSEChunk(t, map[string]interface{}{
		"choices": []interface{}{
			map[string]interface{}{
				"delta": map[string]interface{}{
					"role": "assistant",
					"tool_calls": []interface{}{
						map[string]interface{}{
							"index": 0,
							"id":    "call_123",
							"function": map[string]interface{}{
								"name":      "lookup",
								"arguments": "{\"q\":\"hel",
							},
						},
					},
				},
			},
		},
	}))
	events = append(events, converter.ProcessChunk(encodeChatSSEChunk(t, map[string]interface{}{
		"choices": []interface{}{
			map[string]interface{}{
				"delta": map[string]interface{}{
					"tool_calls": []interface{}{
						map[string]interface{}{
							"index": 0,
							"function": map[string]interface{}{
								"arguments": "lo\"}",
							},
						},
					},
				},
				"finish_reason": "tool_calls",
			},
		},
	}))...)

	parsed := parseSSEEvents(t, events)
	assertSequenceNumbers(t, parsed)

	itemAdded := findOutputItemEvent(t, parsed, "response.output_item.added", "function_call")
	item := mustMapField(t, itemAdded.Payload, "item")
	itemID := mustStringField(t, item, "id")

	argsDelta := findEvent(t, parsed, "response.function_call_arguments.delta")
	if got := mustStringField(t, argsDelta.Payload, "item_id"); got != itemID {
		t.Fatalf("expected arguments delta item_id %q, got %q", itemID, got)
	}

	argsDone := findEvent(t, parsed, "response.function_call_arguments.done")
	if got := mustStringField(t, argsDone.Payload, "item_id"); got != itemID {
		t.Fatalf("expected arguments done item_id %q, got %q", itemID, got)
	}
	if got := mustStringField(t, argsDone.Payload, "name"); got != "lookup" {
		t.Fatalf("expected tool call name lookup, got %q", got)
	}
	if got := mustStringField(t, argsDone.Payload, "arguments"); got != "{\"q\":\"hello\"}" {
		t.Fatalf("expected tool call arguments to be reassembled, got %q", got)
	}

	itemDone := findOutputItemEvent(t, parsed, "response.output_item.done", "function_call")
	doneItem := mustMapField(t, itemDone.Payload, "item")
	if got := mustStringField(t, doneItem, "status"); got != "completed" {
		t.Fatalf("expected completed tool call status, got %q", got)
	}
}

func TestResponsesStreamConverterEmitsFailedTerminalEvent(t *testing.T) {
	converter := NewResponsesStreamConverter("mimo-test")

	events := converter.Fail("Gateway Error: stream timed out", "stream_idle_timeout")
	parsed := parseSSEEvents(t, events)
	assertSequenceNumbers(t, parsed)

	errEvent := findEvent(t, parsed, "error")
	errObj := mustMapField(t, errEvent.Payload, "error")
	if got := mustStringField(t, errObj, "code"); got != "stream_idle_timeout" {
		t.Fatalf("expected error code stream_idle_timeout, got %q", got)
	}

	failed := findEvent(t, parsed, "response.failed")
	response := mustMapField(t, failed.Payload, "response")
	if got := mustStringField(t, response, "status"); got != "failed" {
		t.Fatalf("expected failed response status, got %q", got)
	}
	respErr := mustMapField(t, response, "error")
	if got := mustStringField(t, respErr, "message"); got != "Gateway Error: stream timed out" {
		t.Fatalf("expected failed response message, got %q", got)
	}

	if again := converter.Finalize(); len(again) != 0 {
		t.Fatalf("expected no completion after failure, got %d events", len(again))
	}
}

func TestResponsesStreamConverterKeepAliveUsesResponsesEvents(t *testing.T) {
	converter := NewResponsesStreamConverter("mimo-test")

	events := converter.KeepAlive()
	events = append(events, converter.KeepAlive()...)

	parsed := parseSSEEvents(t, events)
	assertSequenceNumbers(t, parsed)

	created := findEvent(t, parsed, "response.created")
	createdResp := mustMapField(t, created.Payload, "response")
	if got := mustStringField(t, createdResp, "status"); got != "in_progress" {
		t.Fatalf("expected created response to be in_progress, got %q", got)
	}

	inProgress := findEvent(t, parsed, "response.in_progress")
	progressResp := mustMapField(t, inProgress.Payload, "response")
	if got := mustStringField(t, progressResp, "id"); got != mustStringField(t, createdResp, "id") {
		t.Fatalf("expected heartbeat response id to stay stable, got %q", got)
	}
}

func TestResponsesConvertRequestHandlesCompactInstructions(t *testing.T) {
	req := map[string]interface{}{
		"model":        "mimo-v2.5-pro",
		"instructions": "Summarize the following conversation.",
		"input": []interface{}{
			map[string]interface{}{
				"type":    "message",
				"role":    "user",
				"content": "Hello",
			},
			map[string]interface{}{
				"type":    "message",
				"role":    "assistant",
				"content": "Hi there!",
			},
		},
		"stream": false,
	}

	result, err := ResponsesConvertRequest(req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	messages, ok := result["messages"].([]map[string]interface{})
	if !ok {
		t.Fatalf("expected messages to be []map[string]interface{}, got %T", result["messages"])
	}
	if len(messages) != 3 {
		t.Fatalf("expected 3 messages (system + 2 from input), got %d", len(messages))
	}
	if messages[0]["role"] != "system" {
		t.Fatalf("expected first message to be system, got %q", messages[0]["role"])
	}
	if messages[0]["content"] != "Summarize the following conversation." {
		t.Fatalf("expected system message content to be the instructions, got %q", messages[0]["content"])
	}
	if result["stream"] != false {
		t.Fatalf("expected stream to be false, got %v", result["stream"])
	}
}

func TestConvertResponsesResponseProducesValidCompactFormat(t *testing.T) {
	chatResp := map[string]interface{}{
		"model": "mimo-v2.5-pro",
		"choices": []interface{}{
			map[string]interface{}{
				"message": map[string]interface{}{
					"role":    "assistant",
					"content": "Compacted summary of the conversation.",
				},
			},
		},
		"usage": map[string]interface{}{
			"prompt_tokens":     100,
			"completion_tokens": 50,
			"total_tokens":      150,
		},
	}

	resp := ConvertResponsesResponse(chatResp)

	if resp["object"] != "response" {
		t.Fatalf("expected object=response, got %v", resp["object"])
	}
	if resp["status"] != "completed" {
		t.Fatalf("expected status=completed, got %v", resp["status"])
	}
	if resp["model"] != "mimo-v2.5-pro" {
		t.Fatalf("expected model=mimo-v2.5-pro, got %v", resp["model"])
	}

	output, ok := resp["output"].([]interface{})
	if !ok || len(output) == 0 {
		t.Fatalf("expected non-empty output array, got %v", resp["output"])
	}

	usage, ok := resp["usage"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected usage map, got %T", resp["usage"])
	}
	if usage["input_tokens"] != 100 {
		t.Fatalf("expected input_tokens=100, got %v", usage["input_tokens"])
	}
	if usage["output_tokens"] != 50 {
		t.Fatalf("expected output_tokens=50, got %v", usage["output_tokens"])
	}
}

func TestResponsesConvertRequestDropsPreviousResponseID(t *testing.T) {
	result, err := ResponsesConvertRequest(map[string]interface{}{
		"model":                "mimo-test",
		"previous_response_id": "resp_123",
		"input":                "continue",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := result["previous_response_id"]; ok {
		t.Fatal("did not expect previous_response_id to be forwarded to chat completions")
	}
	messages, ok := result["messages"].([]map[string]interface{})
	if !ok || len(messages) != 1 {
		t.Fatalf("expected one converted message, got %T %v", result["messages"], result["messages"])
	}
	if messages[0]["role"] != "user" || messages[0]["content"] != "continue" {
		t.Fatalf("unexpected converted message: %v", messages[0])
	}
}

func TestConvertResponseInputItemsAppendsPendingReasoningToToolCallMessage(t *testing.T) {
	items := []interface{}{
		map[string]interface{}{
			"type":              "reasoning",
			"encrypted_content": "alpha",
			"reasoning_content": "",
		},
		map[string]interface{}{
			"type":    "message",
			"role":    "assistant",
			"content": "hello",
		},
		map[string]interface{}{
			"type":              "reasoning",
			"reasoning_content": "beta",
		},
		map[string]interface{}{
			"type":      "function_call",
			"call_id":   "call_456",
			"name":      "lookup",
			"arguments": map[string]interface{}{"q": "world"},
		},
	}

	messages := convertResponseInputItems(items)
	if len(messages) != 1 {
		t.Fatalf("expected one assistant message, got %d", len(messages))
	}
	if got := mustStringField(t, messages[0], "reasoning_content"); got != "alphabeta" {
		t.Fatalf("expected reasoning_content to be appended, got %q", got)
	}
}

type parsedSSEEvent struct {
	Name    string
	Payload map[string]interface{}
}

func encodeChatSSEChunk(t *testing.T, payload map[string]interface{}) string {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	return "data: " + string(body) + "\n\n"
}

func parseSSEEvents(t *testing.T, rawEvents []string) []parsedSSEEvent {
	t.Helper()
	parsed := make([]parsedSSEEvent, 0, len(rawEvents))
	for _, raw := range rawEvents {
		lines := strings.Split(strings.TrimSpace(raw), "\n")
		eventName := ""
		data := ""
		for _, line := range lines {
			switch {
			case strings.HasPrefix(line, "event:"):
				eventName = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
			case strings.HasPrefix(line, "data:"):
				data = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			}
		}
		if eventName == "" || data == "" {
			t.Fatalf("invalid SSE event: %q", raw)
		}

		var payload map[string]interface{}
		if err := json.Unmarshal([]byte(data), &payload); err != nil {
			t.Fatalf("unmarshal SSE payload %q: %v", data, err)
		}
		parsed = append(parsed, parsedSSEEvent{Name: eventName, Payload: payload})
	}
	return parsed
}

func assertSequenceNumbers(t *testing.T, events []parsedSSEEvent) {
	t.Helper()
	for i, event := range events {
		got := int(mustFloatField(t, event.Payload, "sequence_number"))
		want := i + 1
		if got != want {
			t.Fatalf("expected sequence_number %d for %s, got %d", want, event.Name, got)
		}
	}
}

func findEvent(t *testing.T, events []parsedSSEEvent, name string) parsedSSEEvent {
	t.Helper()
	for _, event := range events {
		if event.Name == name {
			return event
		}
	}
	t.Fatalf("missing event %s", name)
	return parsedSSEEvent{}
}

func findOutputItemEvent(t *testing.T, events []parsedSSEEvent, name, itemType string) parsedSSEEvent {
	t.Helper()
	for _, event := range events {
		if event.Name != name {
			continue
		}
		item := mustMapField(t, event.Payload, "item")
		if mustStringField(t, item, "type") == itemType {
			return event
		}
	}
	t.Fatalf("missing %s event for item type %s", name, itemType)
	return parsedSSEEvent{}
}

func mustMap(t *testing.T, value interface{}) map[string]interface{} {
	t.Helper()
	m, ok := value.(map[string]interface{})
	if !ok {
		t.Fatalf("expected object, got %T", value)
	}
	return m
}

func mustMapField(t *testing.T, data map[string]interface{}, key string) map[string]interface{} {
	t.Helper()
	value, ok := data[key].(map[string]interface{})
	if !ok {
		t.Fatalf("expected %s to be an object, got %T", key, data[key])
	}
	return value
}

func mustSliceField(t *testing.T, data map[string]interface{}, key string) []interface{} {
	t.Helper()
	value, ok := data[key].([]interface{})
	if !ok {
		t.Fatalf("expected %s to be an array, got %T", key, data[key])
	}
	return value
}

func mustStringField(t *testing.T, data map[string]interface{}, key string) string {
	t.Helper()
	value, ok := data[key].(string)
	if !ok {
		t.Fatalf("expected %s to be a string, got %T", key, data[key])
	}
	return value
}

func mustFloatField(t *testing.T, data map[string]interface{}, key string) float64 {
	t.Helper()
	value, ok := data[key].(float64)
	if !ok {
		t.Fatalf("expected %s to be a number, got %T", key, data[key])
	}
	return value
}
