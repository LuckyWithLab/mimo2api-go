package converter

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"mimo2api/internal/models"
)

func nowUnix() int64 {
	return time.Now().Unix()
}

func generateID(prefix string) string {
	return fmt.Sprintf("%s_%s", prefix, strings.ReplaceAll(uuid.New().String(), "-", "")[:24])
}

func sseEvent(eventType string, payload map[string]interface{}) string {
	if payload == nil {
		payload = map[string]interface{}{}
	}
	payload["type"] = eventType
	body, _ := json.Marshal(payload)
	return fmt.Sprintf("event: %s\ndata: %s\n\n", eventType, body)
}

type ResponsesStreamConverter struct {
	respID                 string
	msgID                  string
	reasoningID            string
	model                  string
	createdAt              int64
	nextOutIdx             int
	nextSequence           int
	streamBuf              string
	reasoningOutIdx        *int
	reasoningBuf           string
	reasoningClosed        bool
	textOutIdx             *int
	textBuf                string
	textClosed             bool
	toolCalls              map[int]*toolCallState
	responseCreatedEmitted bool
	contentDone            bool
	completionEmitted      bool
	usage                  *models.ResponseUsage
}

type toolCallState struct {
	Item        map[string]interface{}
	OutputIndex int
}

func NewResponsesStreamConverter(model string) *ResponsesStreamConverter {
	return &ResponsesStreamConverter{
		respID:       generateID("resp"),
		msgID:        generateID("msg"),
		reasoningID:  generateID("rs"),
		model:        model,
		createdAt:    nowUnix(),
		nextSequence: 1,
		toolCalls:    make(map[int]*toolCallState),
	}
}

func (c *ResponsesStreamConverter) allocateIndex() int {
	idx := c.nextOutIdx
	c.nextOutIdx++
	return idx
}

func (c *ResponsesStreamConverter) baseResponse(status string) map[string]interface{} {
	resp := map[string]interface{}{
		"id":         c.respID,
		"object":     "response",
		"model":      c.model,
		"created_at": c.createdAt,
		"status":     status,
		"output":     []interface{}{},
	}
	if c.usage != nil {
		resp["usage"] = c.usage
	}
	return resp
}

func (c *ResponsesStreamConverter) ProcessChunk(chunkText string) []string {
	c.streamBuf += normalizeSSEChunk(chunkText)
	return c.drainBufferedEvents(false)
}

func (c *ResponsesStreamConverter) handleDelta(delta map[string]interface{}) []string {
	var events []string

	if asString(delta["role"]) != "" {
		events = append(events, c.emitResponseCreated()...)
	}

	if reason := asString(delta["reasoning_content"]); reason != "" {
		events = append(events, c.ensureReasoningItemStarted()...)
		c.reasoningBuf += reason
	}

	if content := asString(delta["content"]); content != "" {
		events = append(events, c.closeReasoningItem()...)
		events = append(events, c.ensureTextItemStarted()...)
		c.textBuf += content

		events = append(events, c.emitEvent("response.output_text.delta", map[string]interface{}{
			"item_id":       c.msgID,
			"output_index":  *c.textOutIdx,
			"content_index": 0,
			"delta":         content,
		}))
	}

	if toolCalls, ok := delta["tool_calls"].([]interface{}); ok {
		for _, raw := range toolCalls {
			tc, ok := raw.(map[string]interface{})
			if !ok {
				continue
			}
			events = append(events, c.closeReasoningItem()...)
			events = append(events, c.handleToolCallDelta(tc)...)
		}
	}

	return events
}

func (c *ResponsesStreamConverter) handleToolCallDelta(tc map[string]interface{}) []string {
	var events []string
	tcIndex := asInt(tc["index"])
	function, _ := tc["function"].(map[string]interface{})

	if _, ok := c.toolCalls[tcIndex]; !ok {
		events = append(events, c.closeTextContent()...)
		events = append(events, c.emitResponseCreated()...)

		outIdx := c.allocateIndex()
		callID := firstNonEmptyString(asString(tc["id"]), generateID("call"))
		item := map[string]interface{}{
			"id":        generateID("fc"),
			"type":      "function_call",
			"status":    "in_progress",
			"call_id":   callID,
			"name":      "",
			"arguments": "",
		}
		c.toolCalls[tcIndex] = &toolCallState{
			Item:        item,
			OutputIndex: outIdx,
		}

		events = append(events, c.emitEvent("response.output_item.added", map[string]interface{}{
			"output_index": outIdx,
			"item":         item,
		}))
	}

	tcData := c.toolCalls[tcIndex]
	item := tcData.Item
	outputIndex := tcData.OutputIndex

	if name := asString(function["name"]); name != "" {
		item["name"] = name
	}
	if args := asString(function["arguments"]); args != "" {
		item["arguments"] = asString(item["arguments"]) + args
		events = append(events, c.emitEvent("response.function_call_arguments.delta", map[string]interface{}{
			"item_id":      asString(item["id"]),
			"output_index": outputIndex,
			"delta":        args,
		}))
	}

	return events
}

func (c *ResponsesStreamConverter) emitResponseCreated() []string {
	if c.responseCreatedEmitted {
		return nil
	}
	c.responseCreatedEmitted = true
	return []string{c.emitEvent("response.created", map[string]interface{}{
		"response": c.baseResponse("in_progress"),
	})}
}

func (c *ResponsesStreamConverter) ensureTextItemStarted() []string {
	events := c.emitResponseCreated()
	if c.textOutIdx == nil {
		idx := c.allocateIndex()
		c.textOutIdx = &idx

		msgItem := map[string]interface{}{
			"id":      c.msgID,
			"type":    "message",
			"role":    "assistant",
			"status":  "in_progress",
			"content": []interface{}{},
		}

		events = append(events, c.emitEvent("response.output_item.added", map[string]interface{}{
			"output_index": idx,
			"item":         msgItem,
		}))
		events = append(events, c.emitEvent("response.content_part.added", map[string]interface{}{
			"item_id":       c.msgID,
			"output_index":  idx,
			"content_index": 0,
			"part": map[string]interface{}{
				"type":        "output_text",
				"text":        "",
				"annotations": []interface{}{},
			},
		}))
	}
	return events
}

func (c *ResponsesStreamConverter) ensureReasoningItemStarted() []string {
	events := c.emitResponseCreated()
	if c.reasoningOutIdx == nil {
		idx := c.allocateIndex()
		c.reasoningOutIdx = &idx

		item := map[string]interface{}{
			"id":      generateID("rs"),
			"type":    "reasoning",
			"status":  "in_progress",
			"summary": []interface{}{},
		}

		item["id"] = c.reasoningID
		events = append(events, c.emitEvent("response.output_item.added", map[string]interface{}{
			"output_index": idx,
			"item":         item,
		}))
	}
	return events
}

func (c *ResponsesStreamConverter) closeReasoningItem() []string {
	if c.reasoningOutIdx == nil || c.reasoningClosed {
		return nil
	}
	c.reasoningClosed = true
	return []string{c.emitEvent("response.output_item.done", map[string]interface{}{
		"output_index": *c.reasoningOutIdx,
		"item": map[string]interface{}{
			"id":                c.reasoningID,
			"type":              "reasoning",
			"status":            "completed",
			"summary":           []interface{}{},
			"encrypted_content": c.reasoningBuf,
		},
	})}
}

func (c *ResponsesStreamConverter) closeTextContent() []string {
	if c.textOutIdx == nil || c.textClosed {
		return nil
	}
	c.textClosed = true

	textPart := map[string]interface{}{
		"type":        "output_text",
		"text":        c.textBuf,
		"annotations": []interface{}{},
	}
	msgItem := map[string]interface{}{
		"id":      c.msgID,
		"type":    "message",
		"role":    "assistant",
		"status":  "completed",
		"content": []interface{}{textPart},
	}

	return []string{
		c.emitEvent("response.output_text.done", map[string]interface{}{
			"item_id":       c.msgID,
			"output_index":  *c.textOutIdx,
			"content_index": 0,
			"text":          c.textBuf,
		}),
		c.emitEvent("response.content_part.done", map[string]interface{}{
			"item_id":       c.msgID,
			"output_index":  *c.textOutIdx,
			"content_index": 0,
			"part":          textPart,
		}),
		c.emitEvent("response.output_item.done", map[string]interface{}{
			"output_index": *c.textOutIdx,
			"item":         msgItem,
		}),
	}
}

func (c *ResponsesStreamConverter) handleFinish(finishReason string) []string {
	if c.contentDone {
		return nil
	}
	c.contentDone = true

	events := make([]string, 0)
	events = append(events, c.closeReasoningItem()...)
	events = append(events, c.closeTextContent()...)

	if finishReason == "tool_calls" {
		indexes := make([]int, 0, len(c.toolCalls))
		for idx := range c.toolCalls {
			indexes = append(indexes, idx)
		}
		sortInts(indexes)
		for _, idx := range indexes {
			tc := c.toolCalls[idx]
			item := tc.Item
			item["status"] = "completed"
			events = append(events, c.emitEvent("response.function_call_arguments.done", map[string]interface{}{
				"item_id":      asString(item["id"]),
				"output_index": tc.OutputIndex,
				"name":         asString(item["name"]),
				"arguments":    asString(item["arguments"]),
			}))
			events = append(events, c.emitEvent("response.output_item.done", map[string]interface{}{
				"output_index": tc.OutputIndex,
				"item":         item,
			}))
		}
	}

	return events
}

func (c *ResponsesStreamConverter) emitCompletion() []string {
	if c.completionEmitted {
		return nil
	}
	c.completionEmitted = true

	output := make([]interface{}, 0)
	if c.reasoningOutIdx != nil {
		output = append(output, map[string]interface{}{
			"id":                c.reasoningID,
			"type":              "reasoning",
			"status":            "completed",
			"summary":           []interface{}{},
			"encrypted_content": c.reasoningBuf,
		})
	}
	if c.textOutIdx != nil || len(c.toolCalls) == 0 {
		output = append(output, map[string]interface{}{
			"id":      c.msgID,
			"type":    "message",
			"role":    "assistant",
			"status":  "completed",
			"content": []interface{}{map[string]interface{}{"type": "output_text", "text": c.textBuf, "annotations": []interface{}{}}},
		})
	}

	indexes := make([]int, 0, len(c.toolCalls))
	for idx := range c.toolCalls {
		indexes = append(indexes, idx)
	}
	sortInts(indexes)
	for _, idx := range indexes {
		item := c.toolCalls[idx].Item
		item["status"] = "completed"
		output = append(output, item)
	}

	resp := c.baseResponse("completed")
	resp["output"] = output
	return []string{c.emitEvent("response.completed", map[string]interface{}{
		"response": resp,
	})}
}

func (c *ResponsesStreamConverter) handleDone() []string {
	var events []string
	events = append(events, c.drainBufferedEvents(true)...)
	if !c.contentDone {
		events = append(events, c.handleFinish("stop")...)
	}
	events = append(events, c.emitCompletion()...)
	return events
}

func (c *ResponsesStreamConverter) Finalize() []string {
	return c.handleDone()
}

func sortInts(values []int) {
	for i := 0; i < len(values); i++ {
		for j := i + 1; j < len(values); j++ {
			if values[j] < values[i] {
				values[i], values[j] = values[j], values[i]
			}
		}
	}
}

func (c *ResponsesStreamConverter) emitEvent(eventType string, payload map[string]interface{}) string {
	if payload == nil {
		payload = map[string]interface{}{}
	}
	payload["sequence_number"] = c.nextSequence
	c.nextSequence++
	return sseEvent(eventType, payload)
}

func (c *ResponsesStreamConverter) drainBufferedEvents(allowPartial bool) []string {
	var events []string
	for {
		idx := strings.Index(c.streamBuf, "\n\n")
		if idx < 0 {
			break
		}
		block := c.streamBuf[:idx]
		c.streamBuf = c.streamBuf[idx+2:]
		events = append(events, c.processEventBlock(block)...)
	}

	if allowPartial {
		block := strings.TrimSpace(c.streamBuf)
		c.streamBuf = ""
		if block != "" {
			events = append(events, c.processEventBlock(block)...)
		}
	}

	return events
}

func (c *ResponsesStreamConverter) processEventBlock(block string) []string {
	block = strings.TrimSpace(block)
	if block == "" {
		return nil
	}

	lines := strings.Split(block, "\n")
	dataLines := make([]string, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimRight(line, "\r")
		if strings.HasPrefix(line, "data:") {
			data := line[5:]
			if strings.HasPrefix(data, " ") {
				data = data[1:]
			}
			dataLines = append(dataLines, data)
		}
	}
	if len(dataLines) == 0 {
		return nil
	}

	dataStr := strings.TrimSpace(strings.Join(dataLines, "\n"))
	if dataStr == "" {
		return nil
	}
	if dataStr == "[DONE]" {
		return c.handleDone()
	}

	var chunk map[string]interface{}
	if err := json.Unmarshal([]byte(dataStr), &chunk); err != nil {
		return nil
	}

	var results []string
	if usageRaw, ok := chunk["usage"].(map[string]interface{}); ok {
		c.usage = &models.ResponseUsage{
			InputTokens:  asInt(usageRaw["prompt_tokens"]),
			OutputTokens: asInt(usageRaw["completion_tokens"]),
			TotalTokens:  asInt(usageRaw["total_tokens"]),
		}
		if c.contentDone && !c.completionEmitted {
			results = append(results, c.emitCompletion()...)
		}
	}

	if choices, ok := chunk["choices"].([]interface{}); ok {
		for _, choiceRaw := range choices {
			choice, ok := choiceRaw.(map[string]interface{})
			if !ok {
				continue
			}
			if delta, ok := choice["delta"].(map[string]interface{}); ok {
				results = append(results, c.handleDelta(delta)...)
			}
			if finishReason := asString(choice["finish_reason"]); finishReason != "" {
				results = append(results, c.handleFinish(finishReason)...)
			}
		}
	}

	return results
}

func normalizeSSEChunk(chunk string) string {
	chunk = strings.ReplaceAll(chunk, "\r\n", "\n")
	return strings.ReplaceAll(chunk, "\r", "\n")
}
