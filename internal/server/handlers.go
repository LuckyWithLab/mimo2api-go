package server

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"mimo2api/internal/config"
	"mimo2api/internal/converter"
	"mimo2api/internal/manager"
	"mimo2api/internal/metrics"
	"mimo2api/internal/models"
	"mimo2api/internal/state"
)

const statusClientClosed = 499

const (
	nodeFailureCooldown = 30 * time.Second
	nodeTimeoutCooldown = 120 * time.Second
	maxNodeAttempts     = 2
)

const compactSystemPrompt = `You are a conversation compaction assistant. Your task is to produce a compacted summary of the following conversation. The summary must preserve all key information including:
- Important facts, decisions, and conclusions
- User preferences, constraints, and requirements
- Pending action items or open questions
- Critical context needed to continue the conversation seamlessly

Output ONLY the compacted summary as a coherent, well-structured text. Do not include meta-commentary like "Here is the summary" — just provide the summary directly.`

// ─── 全局缓存 ───
var (
	modelMappingCache map[string]string
	mappingMu         sync.RWMutex
)

// 初始化时自动加载一次配置
func init() {
	modelMappingCache = make(map[string]string)
	data, err := os.ReadFile("model_mapping.json")
	if err == nil {
		_ = json.Unmarshal(data, &modelMappingCache)
	}
}

func SystemStatusHandler(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		// 修复: 避免产生并发竞争(Data Race)引发宕机
		"active_clients": state.GetConnectedClientsCount(),
	})
}

func StatsHandler(c *gin.Context) {
	activeClients := state.GetConnectedClientsCount()
	availableClients := state.GetAvailableClientsCount()
	pendingCount := state.GetPendingCount() // 修复: 使用带锁的读取方法

	requestsTotal, requestsSucceeded, requestsFailed,
		attemptsTotal, attemptsSucceeded, attemptsFailed,
		streamingReqs, nonStreamingReqs,
		latencySumMs, firstByteLatencySumMs,
		latencySamples, firstByteSamples,
		statusCodes, tokens,
		routeMetrics, nodeMetrics := state.Metrics.SnapshotRead()

	requestTotal := int(requestsTotal)
	requestTotal = maxInt(requestTotal, 1) // avoid div-by-zero

	// Build nodes list from active connections only
	nodes := []map[string]interface{}{}
	activeNodes := state.GetActiveNodes()
	for _, an := range activeNodes {
		nodeEntry := map[string]interface{}{
			"index":                      an.Index,
			"node":                       an.Host,
			"available":                  an.Available,
			"bridge_ready":               an.BridgeReady,
			"cooldown_remaining_seconds": int(an.CooldownUntil),
			"ban_remaining_seconds":      int(an.BanUntil),
			"pending_requests":           an.PendingRequests,
		}
		// Add metrics for this node if available
		if nm, ok := nodeMetrics[an.Host]; ok {
			attemptTotal := nm.AttemptsTotal
			if attemptTotal == 0 {
				attemptTotal = 1
			}
			nodeEntry["attempts"] = map[string]interface{}{
				"total":     nm.AttemptsTotal,
				"succeeded": nm.AttemptsSucceeded,
				"failed":    nm.AttemptsFailed,
			}
			nodeEntry["avg_first_byte_latency_ms"] = roundTo2(float64(nm.FirstByteLatencySumMs) / float64(attemptTotal))
			nodeEntry["status_codes"] = nm.StatusCodes
		} else {
			nodeEntry["attempts"] = map[string]interface{}{"total": 0, "succeeded": 0, "failed": 0}
			nodeEntry["avg_first_byte_latency_ms"] = 0.0
			nodeEntry["status_codes"] = map[string]int64{}
		}
		nodes = append(nodes, nodeEntry)
	}

	// Build routes map
	routes := map[string]interface{}{}
	for routeKey, rm := range routeMetrics {
		rTotal := rm.RequestsTotal
		if rTotal == 0 {
			rTotal = 1
		}
		routes[routeKey] = map[string]interface{}{
			"requests": map[string]interface{}{
				"total":     rm.RequestsTotal,
				"succeeded": rm.RequestsSucceeded,
				"failed":    rm.RequestsFailed,
			},
			"streaming_requests":        rm.StreamingRequests,
			"non_streaming_requests":    rm.NonStreamingRequests,
			"avg_latency_ms":            roundTo2(rm.RequestLatencySumMs / float64(rTotal)),
			"avg_first_byte_latency_ms": roundTo2(rm.RequestFirstByteLatencySumMs / float64(rTotal)),
			"status_codes":              rm.StatusCodes,
			"tokens": map[string]interface{}{
				"requests_with_usage": rm.Tokens.RequestsWithUsage,
				"prompt_tokens":       rm.Tokens.PromptTokens,
				"completion_tokens":   rm.Tokens.CompletionTokens,
				"total_tokens":        rm.Tokens.TotalTokens,
			},
		}
	}

	// Latency summary
	latencySummary := buildLatencySummary(latencySamples, latencySumMs, requestTotal)
	firstByteSummary := buildLatencySummary(firstByteSamples, firstByteLatencySumMs, requestTotal)

	c.JSON(200, gin.H{
		"uptime_seconds":    int(time.Since(state.StartTime).Seconds()),
		"active_clients":    activeClients,
		"available_clients": availableClients,
		"cooldown_clients":  activeClients - availableClients,
		"pending_requests":  pendingCount,
		"background_tasks":  manager.GlobalManager.GetActiveUsersCount(),
		"nodes":             nodes,
		"routes":            routes,
		"requests": map[string]interface{}{
			"total":     requestsTotal,
			"succeeded": requestsSucceeded,
			"failed":    requestsFailed,
		},
		"attempts": map[string]interface{}{
			"total":     attemptsTotal,
			"succeeded": attemptsSucceeded,
			"failed":    attemptsFailed,
		},
		"latency":                latencySummary,
		"first_byte_latency":     firstByteSummary,
		"status_codes":           statusCodes,
		"streaming_requests":     streamingReqs,
		"non_streaming_requests": nonStreamingReqs,
		"tokens": map[string]interface{}{
			"requests_with_usage": tokens.RequestsWithUsage,
			"prompt_tokens":       tokens.PromptTokens,
			"completion_tokens":   tokens.CompletionTokens,
			"total_tokens":        tokens.TotalTokens,
		},
	})
}

func buildLatencySummary(samples []float64, sumMs float64, total int) map[string]interface{} {
	if len(samples) == 0 || total == 0 {
		return map[string]interface{}{
			"avg_ms": 0.0, "p50_ms": 0.0, "p99_ms": 0.0, "max_ms": 0.0,
		}
	}
	sorted := append([]float64{}, samples...)
	sort.Float64s(sorted)
	avg := roundTo2(sumMs / float64(total))
	p50 := roundTo2(percentile(sorted, 50))
	p99 := roundTo2(percentile(sorted, 99))
	maxVal := roundTo2(sorted[len(sorted)-1])
	return map[string]interface{}{
		"avg_ms": avg, "p50_ms": p50, "p99_ms": p99, "max_ms": maxVal,
	}
}

func percentile(sorted []float64, p int) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := float64(p) / 100.0 * float64(len(sorted)-1)
	lower := int(idx)
	if lower >= len(sorted)-1 {
		return sorted[len(sorted)-1]
	}
	frac := idx - float64(lower)
	return sorted[lower] + frac*(sorted[lower+1]-sorted[lower])
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func roundTo2(f float64) float64 {
	return float64(int64(f*100+0.5)) / 100
}

func StatusHistoryHandler(c *gin.Context) {
	data, err := metrics.GetStatusHistory(24)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, data)
}

func UsersListHandler(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"users": manager.GlobalManager.GetUsersList(),
	})
}

func UsersAddHandler(c *gin.Context) {
	var body struct {
		RawText string `json:"raw_text"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	uid, err := manager.GlobalManager.AddUser(body.RawText)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"userId": uid})
}

func UsersDeleteHandler(c *gin.Context) {
	uid := c.Param("id")
	manager.GlobalManager.RemoveUser(uid)
	c.JSON(http.StatusOK, gin.H{"message": "deleted"})
}

func ModelMappingHandler(c *gin.Context) {
	c.JSON(http.StatusOK, loadModelMapping())
}

func RebuildHandler(c *gin.Context) {
	manager.GlobalManager.TriggerRebuild()
	c.JSON(http.StatusOK, gin.H{"ok": true, "message": "重排信号已发送，active slot 将在当前循环结束后重新轮询账号"})
}

func loadModelMapping() map[string]string {
	mappingMu.RLock()
	defer mappingMu.RUnlock()

	copied := make(map[string]string, len(modelMappingCache))
	for k, v := range modelMappingCache {
		copied[k] = v
	}
	return copied
}

func saveModelMapping(mapping map[string]string) error {
	mappingMu.Lock()
	defer mappingMu.Unlock()

	data, err := json.MarshalIndent(mapping, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile("model_mapping.json", data, 0644); err != nil {
		return err
	}

	// 更新内存缓存
	modelMappingCache = make(map[string]string, len(mapping))
	for k, v := range mapping {
		modelMappingCache[k] = v
	}
	return nil
}

func getMappedModel(model string) string {
	modelMapping := loadModelMapping()

	// Exact match
	if mapped, ok := modelMapping[model]; ok {
		return mapped
	}

	// Fuzzy match
	lowerModel := strings.ToLower(model)
	if strings.HasPrefix(lowerModel, "mimo-") {
		return model
	}

	if strings.Contains(lowerModel, "opus") {
		return "mimo-v2.5-pro"
	}
	if strings.Contains(lowerModel, "sonnet") {
		return "mimo-v2.5"
	}
	if strings.Contains(lowerModel, "haiku") {
		return "mimo-v2-flash"
	}
	if strings.Contains(lowerModel, "flash") || strings.Contains(lowerModel, "mini") {
		return "mimo-v2-flash"
	}
	if strings.Contains(lowerModel, "pro") {
		return "mimo-v2.5-pro"
	}

	// Default fallback
	return "mimo-v2.5-pro"
}

func applyModelMapping(bodyText []byte) []byte {
	var data map[string]interface{}
	if err := json.Unmarshal(bodyText, &data); err != nil {
		return bodyText
	}
	model, ok := data["model"].(string)
	if !ok {
		return bodyText
	}

	mapped := getMappedModel(model)
	if mapped != model {
		data["model"] = mapped
		newBody, _ := json.Marshal(data)
		return newBody
	}
	return bodyText
}

// injectSystemPrompt ensures the required system prompt is present in the messages array.
// If the first message is a system message, it prepends the required prompt to its content.
// Otherwise, it inserts a new system message at the beginning.
func injectSystemPrompt(bodyText []byte) []byte {
	if config.RequiredSystemPrompt == "" {
		return bodyText
	}
	var data map[string]interface{}
	if err := json.Unmarshal(bodyText, &data); err != nil {
		return bodyText
	}
	messages, ok := data["messages"].([]interface{})
	if !ok || len(messages) == 0 {
		return bodyText
	}
	required := config.RequiredSystemPrompt

	if first, ok := messages[0].(map[string]interface{}); ok {
		if role, _ := first["role"].(string); role == "system" {
			contentStr, _ := first["content"].(string)
			if strings.Contains(contentStr, required) {
				return bodyText
			}
			first["content"] = required + "\n\n" + contentStr
			newBody, _ := json.Marshal(data)
			return newBody
		}
	}

	systemMsg := map[string]interface{}{
		"role":    "system",
		"content": required,
	}
	data["messages"] = append([]interface{}{systemMsg}, messages...)
	newBody, _ := json.Marshal(data)
	return newBody
}

func PutModelMappingHandler(c *gin.Context) {
	var newMapping map[string]string
	if err := c.ShouldBindJSON(&newMapping); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "映射必须是合法 JSON 对象"})
		return
	}
	if err := saveModelMapping(newMapping); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, newMapping)
}

func DeleteModelMappingHandler(c *gin.Context) {
	modelName := strings.TrimPrefix(c.Param("name"), "/")
	mapping := loadModelMapping()
	if _, ok := mapping[modelName]; !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "模型不在映射中"})
		return
	}
	delete(mapping, modelName)
	if err := saveModelMapping(mapping); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "deleted": modelName})
}

func ChatCompletionsHandler(c *gin.Context) {
	commonCompletionsHandler(c, false)
}

func ResponsesHandler(c *gin.Context) {
	commonCompletionsHandler(c, true)
}

func CompactResponsesHandler(c *gin.Context) {
	bodyText, err := io.ReadAll(io.LimitReader(c.Request.Body, 10*1024*1024))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": gin.H{"message": "Failed to read request body"},
		})
		return
	}

	var reqData map[string]interface{}
	if err := json.Unmarshal(bodyText, &reqData); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": gin.H{"message": "Invalid JSON in request body"},
		})
		return
	}

	// model 是必填字段
	if _, ok := reqData["model"].(string); !ok {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": gin.H{"message": "model is required"},
		})
		return
	}

	// input 为空时直接返回最小化响应
	input := reqData["input"]
	if input == nil {
		resp := converter.ConvertResponsesResponse(map[string]interface{}{
			"model":   reqData["model"],
			"choices": []interface{}{},
		})
		c.JSON(http.StatusOK, resp)
		return
	}

	// 注入压缩系统提示词（通过 instructions 字段，ResponsesConvertRequest 会将其转为 system 消息）
	reqData["instructions"] = compactSystemPrompt

	// 强制非流式
	reqData["stream"] = false

	// 将修改后的 body 写回，供 commonCompletionsHandler 读取
	modifiedBody, _ := json.Marshal(reqData)
	c.Request.Body = io.NopCloser(bytes.NewReader(modifiedBody))

	commonCompletionsHandler(c, true)
}

func commonCompletionsHandler(c *gin.Context, isResponses bool) {
	startTime := time.Now()
	routeKey := "unknown" // will be set to modelName below
	success := false
	statusCode := 0
	firstByteTime := startTime
	nodeResponseIdleTimeout := time.Duration(maxInt(config.NodeResponseIdleTimeout, 30)) * time.Second
	keepAliveInterval := time.Duration(maxInt(config.KeepAliveIntervalSeconds, 5)) * time.Second

	reqID := uuid.New().String()

	bodyText, err := io.ReadAll(io.LimitReader(c.Request.Body, 10*1024*1024))
	if err != nil {
		statusCode = 400
		state.Metrics.RecordRequestStarted(routeKey, true)
		state.Metrics.RecordRequestFinished(routeKey, statusCode, float64(time.Since(startTime).Milliseconds()), 0, false)
		log.Printf("[ERR] req=%s route=%s status=400 dur=%dms reason=read_body_failed err=%v", reqID, routeKey, time.Since(startTime).Milliseconds(), err)
		writeErrorResponse(c, http.StatusBadRequest, "Gateway Error: 读取请求失败")
		return
	}
	bodyText = applyModelMapping(bodyText)

	var modelName string
	var isStreaming = true
	var reqData map[string]interface{}
	if err := json.Unmarshal(bodyText, &reqData); err == nil {
		if streamVal, ok := reqData["stream"].(bool); ok {
			isStreaming = streamVal
		}
		if m, ok := reqData["model"].(string); ok {
			modelName = m
		}
	}
	if modelName != "" {
		routeKey = modelName
	}

	// Convert for /v1/responses if needed
	forwardPath := c.Request.URL.Path
	if forwardPath == "/v1/messages" {
		forwardPath = "/anthropic/v1/messages"
	}
	if isResponses {
		forwardPath = "/v1/chat/completions" // forward to chat completions
		if reqData != nil {
			var convertErr error
			reqData, convertErr = converter.ResponsesConvertRequest(reqData)
			if convertErr != nil {
				log.Printf("[ERR] req=%s route=%s status=400 dur=%dms reason=responses_convert_failed err=%v", reqID, routeKey, time.Since(startTime).Milliseconds(), convertErr)
				c.JSON(http.StatusBadRequest, gin.H{
					"error": gin.H{"message": convertErr.Error()},
				})
				return
			}
			if _, ok := reqData["stream"].(bool); !ok {
				reqData["stream"] = true
				isStreaming = true
			}
			bodyText, _ = json.Marshal(reqData)
		}
	}

	// 注入上游必需的系统提示词（两条路径统一处理）
	bodyText = injectSystemPrompt(bodyText)

	// Record request start
	state.Metrics.RecordRequestStarted(routeKey, isStreaming)

	node401Cooldown := time.Duration(maxInt(config.Node401Cooldown, 1)) * time.Second
	excludedNodes := make(map[*websocket.Conn]struct{}, maxNodeAttempts)
	responseCommitted := false // 跨 attempt 持久化：是否已向客户端写过任何字节
	flusher, _ := c.Writer.(http.Flusher)

attemptLoop:
	for attempt := 1; attempt <= maxNodeAttempts; attempt++ {
		targetWS := state.GetNextClientExcluding(excludedNodes)
		if targetWS == nil {
			statusCode = 503
			state.Metrics.RecordRequestFinished(routeKey, statusCode, float64(time.Since(startTime).Milliseconds()), 0, false)
			if attempt == 1 {
				log.Printf("[ERR] req=%s node=nil route=%s status=503 dur=%dms reason=no_available_node", reqID, routeKey, time.Since(startTime).Milliseconds())
				writeErrorResponse(c, http.StatusServiceUnavailable, "Gateway Error: 没有可用的内网节点")
			} else {
				log.Printf("[ERR] req=%s node=nil route=%s status=503 dur=%dms attempts=%d reason=no_available_node_for_retry", reqID, routeKey, time.Since(startTime).Milliseconds(), attempt-1)
				writeErrorResponse(c, http.StatusServiceUnavailable, "Gateway Error: 没有可用的重试节点")
			}
			return
		}

		nodeKey := targetWS.Host
		nodeReqID := reqID
		if attempt > 1 {
			nodeReqID = reqID + "-retry-" + strconv.Itoa(attempt)
		}

		state.Metrics.RecordAttemptStarted(nodeKey)
		queue := state.CreatePendingRequest(targetWS.Conn, nodeReqID)

		payload := map[string]interface{}{
			"type":   "req",
			"req_id": nodeReqID,
			"method": c.Request.Method,
			"path":   forwardPath,
			"body":   string(bodyText),
		}

		targetWS.Mu.Lock()
		_ = targetWS.Conn.SetWriteDeadline(time.Now().Add(state.NodeWriteTimeout))
		err = targetWS.Conn.WriteJSON(payload)
		_ = targetWS.Conn.SetWriteDeadline(time.Time{})
		targetWS.Mu.Unlock()

		if err != nil {
			state.CleanupPendingRequest(nodeReqID)
			state.Metrics.RecordAttemptFinished(nodeKey, http.StatusBadGateway, 0, false)
			state.CooldownClient(targetWS.Conn, nodeFailureCooldown)
			state.CloseClientConn(targetWS.Conn)
			excludedNodes[targetWS.Conn] = struct{}{}
			if attempt < maxNodeAttempts {
				log.Printf("[WARN] req=%s node=%s route=%s attempt=%d/%d reason=write_to_node_failed retrying err=%v", reqID, nodeKey, routeKey, attempt, maxNodeAttempts, err)
				continue
			}

			statusCode = http.StatusBadGateway
			state.Metrics.RecordRequestFinished(routeKey, statusCode, float64(time.Since(startTime).Milliseconds()), 0, false)
			log.Printf("[ERR] req=%s node=%s route=%s status=502 dur=%dms attempts=%d reason=write_to_node_failed err=%v", reqID, nodeKey, routeKey, time.Since(startTime).Milliseconds(), attempt, err)
			writeErrorResponse(c, http.StatusBadGateway, "Gateway Error: 节点下发失败")
			return
		}

		// ─── 等待首字节 + keepalive 心跳防 CF 超时 ───
		var firstMsg map[string]interface{}
		firstByteDeadline := time.NewTimer(nodeResponseIdleTimeout)
		kaTicker := time.NewTicker(keepAliveInterval)
	waitFirstByte:
		for {
			select {
			case firstMsg = <-queue:
				break waitFirstByte
			case <-kaTicker.C:
				if isStreaming {
					if !responseCommitted {
						c.Writer.Header().Set("Content-Type", "text/event-stream")
						c.Writer.Header().Set("Cache-Control", "no-cache")
						c.Writer.Header().Set("Connection", "keep-alive")
						c.Writer.Header().Set("X-Accel-Buffering", "no")
					}
					_, _ = c.Writer.Write([]byte(": keepalive\n\n"))
					if flusher != nil {
						flusher.Flush()
					}
					responseCommitted = true
				}
				// 非流式：不在此阶段发字节，避免锁死 200 status code
			case <-firstByteDeadline.C:
				kaTicker.Stop()
				state.SendCancelToNode(targetWS.Conn, nodeReqID)
				state.CleanupPendingRequest(nodeReqID)
				state.Metrics.RecordAttemptFinished(nodeKey, http.StatusGatewayTimeout, 0, false)
				state.CooldownClient(targetWS.Conn, nodeTimeoutCooldown)
				statusCode = http.StatusGatewayTimeout
				state.Metrics.RecordRequestFinished(routeKey, statusCode, float64(time.Since(startTime).Milliseconds()), 0, false)
				log.Printf("[ERR] req=%s node=%s route=%s status=504 dur=%dms attempts=%d reason=idle_timeout threshold=%ds", reqID, nodeKey, routeKey, time.Since(startTime).Milliseconds(), attempt, config.NodeResponseIdleTimeout)
				if !responseCommitted {
					writeErrorResponse(c, http.StatusGatewayTimeout, "Gateway Error: 请求超时")
				} else {
					writeSSEError(c.Writer, flusher, "Gateway Error: 请求超时")
				}
				return
			case <-c.Request.Context().Done():
				kaTicker.Stop()
				firstByteDeadline.Stop()
				state.SendCancelToNode(targetWS.Conn, nodeReqID)
				state.CleanupPendingRequest(nodeReqID)
				statusCode = statusClientClosed
				state.Metrics.RecordRequestFinished(routeKey, statusCode, float64(time.Since(startTime).Milliseconds()), 0, false)
				state.Metrics.RecordAttemptFinished(nodeKey, statusCode, 0, false)
				log.Printf("[ERR] req=%s node=%s route=%s status=%d dur=%dms reason=client_disconnected_before_first_response", reqID, nodeKey, routeKey, statusClientClosed, time.Since(startTime).Milliseconds())
				return
			}
		}
		kaTicker.Stop()
		firstByteDeadline.Stop()

		// firstMsg == nil 表示 channel 被关闭
		if firstMsg == nil {
			state.CleanupPendingRequest(nodeReqID)
			state.Metrics.RecordAttemptFinished(nodeKey, http.StatusGatewayTimeout, 0, false)
			state.CooldownClient(targetWS.Conn, nodeFailureCooldown)
			excludedNodes[targetWS.Conn] = struct{}{}
			if attempt < maxNodeAttempts {
				log.Printf("[WARN] req=%s node=%s route=%s attempt=%d/%d reason=channel_closed_no_response retrying", reqID, nodeKey, routeKey, attempt, maxNodeAttempts)
				continue
			}

			statusCode = http.StatusGatewayTimeout
			state.Metrics.RecordRequestFinished(routeKey, statusCode, float64(time.Since(startTime).Milliseconds()), 0, false)
			log.Printf("[ERR] req=%s node=%s route=%s status=504 dur=%dms attempts=%d reason=channel_closed_no_response", reqID, nodeKey, routeKey, time.Since(startTime).Milliseconds(), attempt)
			if !responseCommitted {
				writeErrorResponse(c, http.StatusGatewayTimeout, "Gateway Error: 节点无响应")
			} else {
				writeSSEError(c.Writer, flusher, "Gateway Error: 节点无响应")
			}
			return
		}

		firstByteTime = time.Now()
		ttftMs := float64(firstByteTime.Sub(startTime).Milliseconds())

		statusCode = 200
		if s, ok := firstMsg["status"].(float64); ok {
			statusCode = int(s)
		}

		contentType, responseHeaders := normalizeResponseHeaders(firstMsg["headers"])
		initialBody, _ := firstMsg["body"].(string)
		firstMsgType, _ := firstMsg["type"].(string)

		if firstMsgType == "error" {
			state.CleanupPendingRequest(nodeReqID)
			statusCode = http.StatusBadGateway
			state.Metrics.RecordRequestFinished(routeKey, statusCode, float64(time.Since(startTime).Milliseconds()), ttftMs, false)
			state.Metrics.RecordAttemptFinished(nodeKey, statusCode, ttftMs, false)
			state.CooldownClient(targetWS.Conn, nodeFailureCooldown)
			if initialBody == "" {
				initialBody = "Gateway Error: 节点返回错误"
			}
			log.Printf("[ERR] req=%s node=%s route=%s status=502 dur=%dms ttft=%dms reason=node_error_first_msg body=%s", reqID, nodeKey, routeKey, time.Since(startTime).Milliseconds(), int64(ttftMs), initialBody)
			if responseCommitted {
				writeSSEError(c.Writer, flusher, initialBody)
			} else {
				writeErrorResponse(c, http.StatusBadGateway, initialBody)
			}
			return
		}

		if authStatus, authReason, authDetected := detectUnauthorizedResponse(statusCode, initialBody); authDetected {
			state.CleanupPendingRequest(nodeReqID)
			state.Metrics.RecordAttemptFinished(nodeKey, authStatus, ttftMs, false)
			state.CooldownClient(targetWS.Conn, node401Cooldown)
			excludedNodes[targetWS.Conn] = struct{}{}
			if attempt < maxNodeAttempts {
				log.Printf("[WARN] req=%s node=%s route=%s attempt=%d/%d reason=%s cooldown=%ds retrying", reqID, nodeKey, routeKey, attempt, maxNodeAttempts, authReason, int(node401Cooldown/time.Second))
				continue
			}

			state.Metrics.RecordRequestFinished(routeKey, authStatus, float64(time.Since(startTime).Milliseconds()), ttftMs, false)
			log.Printf("[ERR] req=%s node=%s route=%s status=%d dur=%dms ttft=%dms reason=%s body=%s", reqID, nodeKey, routeKey, authStatus, time.Since(startTime).Milliseconds(), int64(ttftMs), authReason, bodyFingerprint(initialBody))
			if responseCommitted {
				writeSSEError(c.Writer, flusher, "Authentication failed: "+authReason)
			} else {
				applyResponseHeaders(c, responseHeaders)
				c.Data(authStatus, contentType, []byte(initialBody))
			}
			return
		}

		if statusCode >= 400 {
			// 500/502 服务器错误时，排除当前节点并尝试其他节点
			if (statusCode == 500 || statusCode == 502) && !responseCommitted {
				excludedNodes[targetWS.Conn] = struct{}{}
				if attempt < maxNodeAttempts {
					state.CleanupPendingRequest(nodeReqID)
					state.Metrics.RecordAttemptFinished(nodeKey, statusCode, ttftMs, false)
					log.Printf("[WARN] req=%s node=%s route=%s attempt=%d/%d status=%d reason=upstream_error retrying", reqID, nodeKey, routeKey, attempt, maxNodeAttempts, statusCode)
					continue attemptLoop
				}
			}
			state.CleanupPendingRequest(nodeReqID)
			success = false
			state.Metrics.RecordRequestFinished(routeKey, statusCode, float64(time.Since(startTime).Milliseconds()), ttftMs, false)
			state.Metrics.RecordAttemptFinished(nodeKey, statusCode, ttftMs, false)
			log.Printf("[ERR] req=%s node=%s route=%s status=%d dur=%dms ttft=%dms reason=upstream_error body=%s", reqID, nodeKey, routeKey, statusCode, time.Since(startTime).Milliseconds(), int64(ttftMs), bodyFingerprint(initialBody))
			if responseCommitted {
				writeSSEError(c.Writer, flusher, "Upstream error: "+strconv.Itoa(statusCode))
			} else {
				applyResponseHeaders(c, responseHeaders)
				c.Data(statusCode, contentType, []byte(initialBody))
			}
			return
		}

		if firstMsgType == "finish" {
			state.CleanupPendingRequest(nodeReqID)
			durationMs := float64(time.Since(startTime).Milliseconds())

			// 检测空 body 异常：200 + 空 body 应视为上游错误
			if statusCode == 200 && initialBody == "" {
				statusCode = http.StatusBadGateway
				state.Metrics.RecordRequestFinished(routeKey, statusCode, durationMs, ttftMs, false)
				state.Metrics.RecordAttemptFinished(nodeKey, statusCode, ttftMs, false)
				log.Printf("[ERR] req=%s node=%s route=%s status=502 dur=%dms ttft=%dms reason=upstream_empty_body", reqID, nodeKey, routeKey, int64(durationMs), int64(ttftMs))
				if responseCommitted {
					writeSSEError(c.Writer, flusher, "Gateway Error: 上游返回空响应")
				} else {
					writeErrorResponse(c, http.StatusBadGateway, "Gateway Error: 上游返回空响应")
				}
				return
			}

			state.Metrics.RecordRequestFinished(routeKey, statusCode, durationMs, ttftMs, true)
			state.Metrics.RecordAttemptFinished(nodeKey, statusCode, ttftMs, true)
			if usage, ok := firstMsg["usage"].(map[string]interface{}); ok {
				state.Metrics.RecordUsage(routeKey, usage)
			}
			if isStreaming {
				log.Printf("[WARN] req=%s node=%s route=%s reason=stream_finish_no_data body_len=%d", reqID, nodeKey, routeKey, len(initialBody))
				applyResponseHeaders(c, responseHeaders)
				c.Writer.Header().Set("Content-Type", "text/event-stream")
				c.Writer.Header().Set("Cache-Control", "no-cache")
				c.Writer.Header().Set("Connection", "keep-alive")
				c.Writer.Header().Set("X-Accel-Buffering", "no")
				c.Status(statusCode)
				if initialBody != "" {
					_, _ = c.Writer.Write([]byte(initialBody))
					if flusher != nil {
						flusher.Flush()
					}
				}
				return
			}

			applyResponseHeaders(c, responseHeaders)
			c.Writer.Header().Set("Content-Type", contentType)
			c.Data(statusCode, contentType, []byte(initialBody))
			return
		}

		clientGone := c.Request.Context().Done()
		if !isStreaming {
			rawBody, ok, clientCancelled, nodeErrMsg := collectResponseBody(queue, clientGone, targetWS.Conn, reqID, nodeReqID, nodeKey, routeKey)
			if !ok {
				state.CleanupPendingRequest(nodeReqID)
				if clientCancelled {
					statusCode = statusClientClosed
					state.Metrics.RecordRequestFinished(routeKey, statusCode, float64(time.Since(startTime).Milliseconds()), ttftMs, false)
					state.Metrics.RecordAttemptFinished(nodeKey, statusCode, ttftMs, false)
					log.Printf("[ERR] req=%s node=%s route=%s status=%d dur=%dms ttft=%dms reason=client_disconnected_nonstream", reqID, nodeKey, routeKey, statusClientClosed, time.Since(startTime).Milliseconds(), int64(ttftMs))
					return
				}
				statusCode = 502
				state.Metrics.RecordRequestFinished(routeKey, statusCode, float64(time.Since(startTime).Milliseconds()), ttftMs, false)
				state.Metrics.RecordAttemptFinished(nodeKey, statusCode, ttftMs, false)
				log.Printf("[ERR] req=%s node=%s route=%s status=502 dur=%dms ttft=%dms reason=collect_response_failed", reqID, nodeKey, routeKey, time.Since(startTime).Milliseconds(), int64(ttftMs))
				errMsg := nodeErrMsg
				if errMsg == "" {
					errMsg = "Gateway Error: 节点返回异常"
				}
				writeErrorResponse(c, http.StatusBadGateway, errMsg)
				return
			}

			if authStatus, authReason, authDetected := detectUnauthorizedResponse(statusCode, rawBody); authDetected {
				state.CleanupPendingRequest(nodeReqID)
				state.Metrics.RecordAttemptFinished(nodeKey, authStatus, ttftMs, false)
				state.CooldownClient(targetWS.Conn, node401Cooldown)
				excludedNodes[targetWS.Conn] = struct{}{}
				if attempt < maxNodeAttempts {
					log.Printf("[WARN] req=%s node=%s route=%s attempt=%d/%d reason=%s cooldown=%ds retrying", reqID, nodeKey, routeKey, attempt, maxNodeAttempts, authReason, int(node401Cooldown/time.Second))
					continue
				}

				state.Metrics.RecordRequestFinished(routeKey, authStatus, float64(time.Since(startTime).Milliseconds()), ttftMs, false)
				log.Printf("[ERR] req=%s node=%s route=%s status=%d dur=%dms ttft=%dms reason=%s body=%s", reqID, nodeKey, routeKey, authStatus, time.Since(startTime).Milliseconds(), int64(ttftMs), authReason, bodyFingerprint(rawBody))
				applyResponseHeaders(c, responseHeaders)
				c.Data(authStatus, contentType, []byte(rawBody))
				return
			}

			state.CleanupPendingRequest(nodeReqID)

			// 检测空 body 异常：200 + 空 body 应视为上游错误
			if statusCode == 200 && rawBody == "" {
				statusCode = http.StatusBadGateway
				durationMs := float64(time.Since(startTime).Milliseconds())
				state.Metrics.RecordRequestFinished(routeKey, statusCode, durationMs, ttftMs, false)
				state.Metrics.RecordAttemptFinished(nodeKey, statusCode, ttftMs, false)
				log.Printf("[ERR] req=%s node=%s route=%s status=502 dur=%dms ttft=%dms reason=upstream_empty_body", reqID, nodeKey, routeKey, int64(durationMs), int64(ttftMs))
				writeErrorResponse(c, http.StatusBadGateway, "Gateway Error: 上游返回空响应")
				return
			}

			applyResponseHeaders(c, responseHeaders)
			c.Writer.Header().Set("Content-Type", contentType)

			success = true
			durationMs := float64(time.Since(startTime).Milliseconds())
			state.Metrics.RecordRequestFinished(routeKey, statusCode, durationMs, ttftMs, true)
			state.Metrics.RecordAttemptFinished(nodeKey, statusCode, ttftMs, true)

			var chatResp map[string]interface{}
			if err := json.Unmarshal([]byte(rawBody), &chatResp); err == nil {
				if usage, ok := chatResp["usage"].(map[string]interface{}); ok {
					state.Metrics.RecordUsage(routeKey, usage)
				}
			}

			if isResponses {
				var chatRespParsed map[string]interface{}
				if err := json.Unmarshal([]byte(rawBody), &chatRespParsed); err != nil {
					log.Printf("[ERR] req=%s node=%s route=%s status=502 dur=%dms ttft=%dms reason=upstream_invalid_json err=%v body=%s", reqID, nodeKey, routeKey, time.Since(startTime).Milliseconds(), int64(ttftMs), err, bodyFingerprint(rawBody))
					c.JSON(http.StatusBadGateway, gin.H{"error": gin.H{"message": "上游返回了非法 JSON"}})
					return
				}
				c.JSON(statusCode, converter.ConvertResponsesResponse(chatRespParsed))
				return
			}
			c.Data(statusCode, contentType, []byte(rawBody))
			return
		}

		var streamConv *converter.ResponsesStreamConverter
		if isResponses {
			streamConv = converter.NewResponsesStreamConverter(modelName)
		}

		headersApplied := responseCommitted // response 已提交时跳过 SSE 基础 headers
		applyStreamHeaders := func() {
			// 上游特定 headers 始终要应用（如 x-request-id 等）
			applyResponseHeaders(c, responseHeaders)
			if headersApplied {
				return
			}
			c.Writer.Header().Set("Content-Type", "text/event-stream")
			c.Writer.Header().Set("Cache-Control", "no-cache")
			c.Writer.Header().Set("Connection", "keep-alive")
			c.Writer.Header().Set("X-Accel-Buffering", "no")
			headersApplied = true
			responseCommitted = true
		}

		streamHasWritten := false
		var finishUsage map[string]interface{}
		// ─── 流式 chunk 循环 + keepalive 心跳 ───
		streamKATicker := time.NewTicker(keepAliveInterval)
		defer streamKATicker.Stop()
		streamIdleTimer := time.NewTimer(nodeResponseIdleTimeout)
		defer streamIdleTimer.Stop()
		for {
			var msg map[string]interface{}
			var ok bool
			select {
			case msg, ok = <-queue:
				if !ok {
					state.CleanupPendingRequest(nodeReqID)
					if !success {
						statusCode = 502
						state.Metrics.RecordRequestFinished(routeKey, statusCode, float64(time.Since(startTime).Milliseconds()), ttftMs, false)
						state.Metrics.RecordAttemptFinished(nodeKey, statusCode, ttftMs, false)
						state.CooldownClient(targetWS.Conn, nodeFailureCooldown)
						log.Printf("[ERR] req=%s node=%s route=%s status=502 dur=%dms ttft=%dms reason=stream_channel_closed", reqID, nodeKey, routeKey, time.Since(startTime).Milliseconds(), int64(ttftMs))
						if responseCommitted {
							writeSSEError(c.Writer, flusher, "Gateway Error: 流式连接异常中断")
						} else {
							writeErrorResponse(c, statusCode, "Gateway Error: 流式连接异常中断")
						}
					}
					return
				}
				// 收到消息，重置空闲计时器
				if !streamIdleTimer.Stop() {
					select {
					case <-streamIdleTimer.C:
					default:
					}
				}
				streamIdleTimer.Reset(nodeResponseIdleTimeout)
			case <-streamKATicker.C:
				// SSE 注释行 keepalive，防止 CF 超时
				applyStreamHeaders()
				_, _ = c.Writer.Write([]byte(": keepalive\n\n"))
				if flusher != nil {
					flusher.Flush()
				}
				continue
			case <-clientGone:
				state.SendCancelToNode(targetWS.Conn, nodeReqID)
				state.CleanupPendingRequest(nodeReqID)
				log.Printf("[ERR] req=%s node=%s route=%s status=%d dur=%dms reason=client_disconnected", reqID, nodeKey, routeKey, statusClientClosed, time.Since(startTime).Milliseconds())
				state.Metrics.RecordRequestFinished(routeKey, statusClientClosed, float64(time.Since(startTime).Milliseconds()), ttftMs, false)
				state.Metrics.RecordAttemptFinished(nodeKey, statusClientClosed, ttftMs, false)
				return
			case <-streamIdleTimer.C:
				statusCode = 504
				state.SendCancelToNode(targetWS.Conn, nodeReqID)
				state.CleanupPendingRequest(nodeReqID)
				state.Metrics.RecordRequestFinished(routeKey, statusCode, float64(time.Since(startTime).Milliseconds()), ttftMs, false)
				state.Metrics.RecordAttemptFinished(nodeKey, statusCode, ttftMs, false)
				state.CooldownClient(targetWS.Conn, nodeTimeoutCooldown)
				log.Printf("[ERR] req=%s node=%s route=%s status=504 dur=%dms ttft=%dms reason=stream_idle_timeout threshold=%ds", reqID, nodeKey, routeKey, time.Since(startTime).Milliseconds(), int64(ttftMs), config.NodeResponseIdleTimeout)
				return
			}

			msgType, ok := msg["type"].(string)
			if !ok {
				continue
			}

			if msgType == "finish" {
				if usage, ok := msg["usage"].(map[string]interface{}); ok {
					finishUsage = usage
				}

				applyStreamHeaders()
				if isResponses && streamConv != nil {
					var finalizeErr error
					for _, ev := range streamConv.Finalize() {
						if _, err := c.Writer.Write([]byte(ev)); err != nil {
							state.SendCancelToNode(targetWS.Conn, nodeReqID)
							state.CleanupPendingRequest(nodeReqID)
							log.Printf("[ERR] req=%s node=%s route=%s status=%d dur=%dms reason=finish_write_failed err=%v", reqID, nodeKey, routeKey, statusClientClosed, time.Since(startTime).Milliseconds(), err)
							state.Metrics.RecordRequestFinished(routeKey, statusClientClosed, float64(time.Since(startTime).Milliseconds()), ttftMs, false)
							state.Metrics.RecordAttemptFinished(nodeKey, statusClientClosed, ttftMs, false)
							finalizeErr = err
							break
						}
					}
					if finalizeErr != nil {
						return
					}
					if flusher != nil {
						flusher.Flush()
					}
				}
				state.CleanupPendingRequest(nodeReqID)
				success = true
				durationMs := float64(time.Since(startTime).Milliseconds())
				state.Metrics.RecordRequestFinished(routeKey, statusCode, durationMs, ttftMs, true)
				state.Metrics.RecordAttemptFinished(nodeKey, statusCode, ttftMs, true)
				if finishUsage != nil {
					state.Metrics.RecordUsage(routeKey, finishUsage)
				}
				return
			}

			if msgType == "chunk" {
				bodyStr, ok := msg["body"].(string)
				if !ok {
					continue
				}

				if !streamHasWritten {
					if authStatus, authReason, authDetected := detectUnauthorizedResponse(statusCode, bodyStr); authDetected {
						state.CleanupPendingRequest(nodeReqID)
						state.Metrics.RecordAttemptFinished(nodeKey, authStatus, ttftMs, false)
						state.CooldownClient(targetWS.Conn, node401Cooldown)
						excludedNodes[targetWS.Conn] = struct{}{}
						if attempt < maxNodeAttempts {
							log.Printf("[WARN] req=%s node=%s route=%s attempt=%d/%d reason=%s cooldown=%ds retrying", reqID, nodeKey, routeKey, attempt, maxNodeAttempts, authReason, int(node401Cooldown/time.Second))
							continue attemptLoop
						}

						state.Metrics.RecordRequestFinished(routeKey, authStatus, float64(time.Since(startTime).Milliseconds()), ttftMs, false)
						log.Printf("[ERR] req=%s node=%s route=%s status=%d dur=%dms ttft=%dms reason=%s body=%s", reqID, nodeKey, routeKey, authStatus, time.Since(startTime).Milliseconds(), int64(ttftMs), authReason, bodyFingerprint(bodyStr))
						if responseCommitted {
							writeSSEError(c.Writer, flusher, "Authentication failed: "+authReason)
						} else {
							c.Data(authStatus, contentType, []byte(bodyStr))
						}
						return
					}
				}

				if finishUsage == nil && !isResponses && strings.Contains(bodyStr, `"usage"`) {
					var chunkData map[string]interface{}
					if err := json.Unmarshal([]byte(bodyStr), &chunkData); err == nil {
						if dataStr, ok := chunkData["data"].(string); ok && dataStr != "[DONE]" {
							var sseData map[string]interface{}
							if err := json.Unmarshal([]byte(dataStr), &sseData); err == nil {
								if usage, ok := sseData["usage"].(map[string]interface{}); ok {
									finishUsage = usage
								}
							}
						}
					}
				}

				applyStreamHeaders()
				if isResponses && streamConv != nil {
					events := streamConv.ProcessChunk(bodyStr)
					for _, ev := range events {
						if _, err := c.Writer.Write([]byte(ev)); err != nil {
							state.SendCancelToNode(targetWS.Conn, nodeReqID)
							state.CleanupPendingRequest(nodeReqID)
							log.Printf("[ERR] req=%s node=%s route=%s status=%d dur=%dms reason=stream_write_failed err=%v", reqID, nodeKey, routeKey, statusClientClosed, time.Since(startTime).Milliseconds(), err)
							state.Metrics.RecordRequestFinished(routeKey, statusClientClosed, float64(time.Since(startTime).Milliseconds()), ttftMs, false)
							state.Metrics.RecordAttemptFinished(nodeKey, statusClientClosed, ttftMs, false)
							return
						}
						streamHasWritten = true
					}
				} else {
					if _, err := c.Writer.Write([]byte(bodyStr)); err != nil {
						state.SendCancelToNode(targetWS.Conn, nodeReqID)
						state.CleanupPendingRequest(nodeReqID)
						log.Printf("[ERR] req=%s node=%s route=%s status=%d dur=%dms reason=stream_write_failed err=%v", reqID, nodeKey, routeKey, statusClientClosed, time.Since(startTime).Milliseconds(), err)
						state.Metrics.RecordRequestFinished(routeKey, statusClientClosed, float64(time.Since(startTime).Milliseconds()), ttftMs, false)
						state.Metrics.RecordAttemptFinished(nodeKey, statusClientClosed, ttftMs, false)
						return
					}
					streamHasWritten = true
				}

				if flusher != nil {
					flusher.Flush()
				}
				continue
			}

			if msgType == "error" {
				state.CleanupPendingRequest(nodeReqID)
				statusCode = 502
				state.Metrics.RecordRequestFinished(routeKey, statusCode, float64(time.Since(startTime).Milliseconds()), ttftMs, false)
				state.Metrics.RecordAttemptFinished(nodeKey, statusCode, ttftMs, false)
				state.CooldownClient(targetWS.Conn, nodeFailureCooldown)
				errBody, _ := msg["body"].(string)
				if errBody == "" {
					errBody = "Gateway Error: 节点返回错误"
				}
				log.Printf("[ERR] req=%s node=%s route=%s status=502 dur=%dms ttft=%dms reason=node_error_msg body=%s", reqID, nodeKey, routeKey, time.Since(startTime).Milliseconds(), int64(ttftMs), errBody)
				if responseCommitted {
					writeSSEError(c.Writer, flusher, errBody)
				} else {
					writeErrorResponse(c, statusCode, errBody)
				}
				return
			}
		}
	}
}

func writeErrorResponse(c *gin.Context, statusCode int, errMsg string) {
	path := c.Request.URL.Path
	if strings.HasPrefix(path, "/anthropic/") || path == "/v1/messages" {
		c.JSON(statusCode, gin.H{
			"type": "error",
			"error": gin.H{
				"type":    "api_error",
				"message": errMsg,
			},
		})
		return
	}

	c.JSON(statusCode, gin.H{
		"error": gin.H{
			"message": errMsg,
			"type":    "gateway_error",
			"param":   nil,
			"code":    "gateway_error",
		},
	})
}

func writeSSEError(w io.Writer, flusher http.Flusher, errMsg string) {
	payload, _ := json.Marshal(map[string]interface{}{
		"error": map[string]string{
			"message": errMsg,
			"type":    "server_error",
		},
	})
	_, _ = w.Write([]byte("event: error\ndata: " + string(payload) + "\n\n"))
	if flusher != nil {
		flusher.Flush()
	}
}

func bodyFingerprint(s string) string {
	sum := sha256.Sum256([]byte(s))
	return "len=" + strconv.Itoa(len(s)) + " sha256=" + hex.EncodeToString(sum[:8])
}

func detectUnauthorizedResponse(statusCode int, body string) (int, string, bool) {
	if statusCode == http.StatusUnauthorized {
		return http.StatusUnauthorized, "upstream_status_401", true
	}
	if body == "" {
		return 0, "", false
	}

	var payload map[string]interface{}
	if err := json.Unmarshal([]byte(body), &payload); err != nil {
		return 0, "", false
	}

	if code, ok := extractStatusCode(payload); ok && code == http.StatusUnauthorized {
		return http.StatusUnauthorized, "upstream_body_code_401", true
	}

	errValue, ok := payload["error"]
	if !ok {
		return 0, "", false
	}

	errMap, ok := errValue.(map[string]interface{})
	if !ok {
		return 0, "", false
	}

	if code, ok := extractStatusCode(errMap); ok && code == http.StatusUnauthorized {
		return http.StatusUnauthorized, "upstream_error_code_401", true
	}
	return 0, "", false
}

func extractStatusCode(data map[string]interface{}) (int, bool) {
	for _, key := range []string{"status", "status_code", "code"} {
		raw, ok := data[key]
		if !ok {
			continue
		}
		switch value := raw.(type) {
		case float64:
			return int(value), true
		case int:
			return value, true
		case json.Number:
			parsed, err := value.Int64()
			if err == nil {
				return int(parsed), true
			}
		case string:
			trimmed := strings.TrimSpace(value)
			parsed, err := strconv.Atoi(trimmed)
			if err == nil {
				return parsed, true
			}
		}
	}
	return 0, false
}

func interfaceToString(value interface{}) string {
	if s, ok := value.(string); ok {
		return s
	}
	return ""
}

func ModelsHandler(c *gin.Context) {
	var data []interface{}
	for _, m := range models.Models {
		data = append(data, map[string]interface{}{
			"id":             m.ID,
			"object":         "model",
			"created":        1700000000,
			"owned_by":       "mimo",
			"context_length": m.Context,
			"max_tokens":     m.MaxTokens,
		})
	}
	c.JSON(http.StatusOK, gin.H{
		"object": "list",
		"data":   data,
	})
}

func AnthropicModelsHandler(c *gin.Context) {
	var data []interface{}
	for _, m := range models.Models {
		data = append(data, map[string]interface{}{
			"id":               m.ID,
			"display_name":     m.Name,
			"created_at":       "2025-01-01T00:00:00Z",
			"type":             "model",
			"max_input_tokens": m.Context,
			"max_tokens":       m.MaxTokens,
		})
	}
	c.JSON(http.StatusOK, gin.H{
		"data":     data,
		"has_more": false,
		"first_id": models.Models[0].ID,
		"last_id":  models.Models[len(models.Models)-1].ID,
	})
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func WSTunnelHandler(c *gin.Context) {
	if config.WSAuthToken != "" {
		providedToken := c.Query("token")
		if providedToken == "" {
			providedToken = c.GetHeader("x-ws-auth")
		}
		if providedToken == "" {
			authHeader := c.GetHeader("Authorization")
			if len(authHeader) > 7 && authHeader[:7] == "Bearer " {
				providedToken = authHeader[7:]
			}
		}

		if providedToken != config.WSAuthToken {
			c.JSON(401, gin.H{"error": "Unauthorized"})
			return
		}
	}

	ws, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		return
	}

	remoteIP := c.ClientIP()
	nodeID := strings.TrimSpace(c.Query("node_id"))
	nodeLabel := strings.TrimSpace(c.Query("node_label"))
	hostLabel := remoteIP
	if nodeLabel != "" {
		hostLabel = nodeLabel
	} else if nodeID != "" {
		hostLabel = nodeID + "@" + remoteIP
	}

	state.RegisterClient(ws, hostLabel)
	defer func() {
		state.UnregisterClient(ws)
		ws.Close()
	}()

	for {
		var msg map[string]interface{}
		if err := ws.ReadJSON(&msg); err != nil {
			break
		}

		reqID, ok := msg["req_id"].(string)
		if !ok {
			continue
		}

		state.PushResponse(reqID, msg)
	}
}

func normalizeResponseHeaders(raw interface{}) (string, map[string]string) {
	headersMap, _ := raw.(map[string]interface{})
	responseHeaders := make(map[string]string)
	contentType := "application/json"

	for key, value := range headersMap {
		lowerKey := strings.ToLower(key)
		stringValue, ok := value.(string)
		if !ok {
			continue
		}
		if lowerKey == "content-type" {
			contentType = stringValue
			continue
		}
		switch lowerKey {
		case "content-length", "transfer-encoding", "content-encoding", "connection":
			continue
		default:
			responseHeaders[key] = stringValue
		}
	}

	return contentType, responseHeaders
}

func applyResponseHeaders(c *gin.Context, headers map[string]string) {
	for key, value := range headers {
		c.Writer.Header().Set(key, value)
	}
}

func collectResponseBody(queue chan map[string]interface{}, clientGone <-chan struct{}, ws *websocket.Conn, logReqID, nodeReqID, nodeKey, routeKey string) (string, bool, bool, string) {
	var builder strings.Builder

	for {
		select {
		case msg, ok := <-queue:
			if !ok {
				return builder.String(), false, false, ""
			}
			msgType, _ := msg["type"].(string)
			switch msgType {
			case "finish":
				return builder.String(), true, false, ""
			case "error":
				errBody, _ := msg["body"].(string)
				return builder.String(), false, false, errBody
			case "chunk":
				if body, ok := msg["body"].(string); ok {
					builder.WriteString(body)
				}
			}
		case <-clientGone:
			state.SendCancelToNode(ws, nodeReqID)
			log.Printf("[ERR] req=%s node=%s route=%s reason=client_disconnected_nonstream", logReqID, nodeKey, routeKey)
			return builder.String(), false, true, ""
		}
	}
}
