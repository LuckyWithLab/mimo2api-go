package server

import (
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
	"mimo2api/internal/state"
)

const statusClientClosed = 499

const (
	nodeFailureCooldown = 30 * time.Second
	nodeTimeoutCooldown = 120 * time.Second
	maxNodeAttempts     = 2
)

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
		"background_tasks":  manager.GlobalManager.GetUsersCount(),
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
	c.JSON(http.StatusOK, gin.H{"ok": true, "message": "重建信号已发送，所有节点将在当前循环结束后立即重建"})
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

	// If no match but it already has mimo prefix, keep it
	if strings.HasPrefix(lowerModel, "mimo-") {
		return model
	}

	// Default fallback
	return "mimo-v2.5-pro"
}

func applyModelMapping(bodyText []byte) []byte {
	mappingMu.RLock()
	cacheLen := len(modelMappingCache)
	mappingMu.RUnlock()

	if cacheLen == 0 {
		return bodyText
	}

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

func commonCompletionsHandler(c *gin.Context, isResponses bool) {
	startTime := time.Now()
	routeKey := "unknown" // will be set to modelName below
	success := false
	statusCode := 0
	firstByteTime := startTime
	nodeResponseIdleTimeout := time.Duration(maxInt(config.NodeResponseIdleTimeout, 30)) * time.Second

	reqID := uuid.New().String()

	bodyText, err := io.ReadAll(io.LimitReader(c.Request.Body, 10*1024*1024))
	if err != nil {
		statusCode = 400
		state.Metrics.RecordRequestStarted(routeKey, true)
		state.Metrics.RecordRequestFinished(routeKey, statusCode, float64(time.Since(startTime).Milliseconds()), 0, false)
		log.Printf("[ERR] req=%s route=%s status=400 dur=%dms reason=read_body_failed err=%v", reqID, routeKey, time.Since(startTime).Milliseconds(), err)
		c.String(http.StatusBadRequest, "Gateway Error: 读取请求失败")
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

	// Record request start
	state.Metrics.RecordRequestStarted(routeKey, isStreaming)

	var firstMsg map[string]interface{}
	var queue chan map[string]interface{}
	var targetWS *state.TunnelClient
	var nodeKey string
	var nodeReqID string
	excludedNodes := make(map[*websocket.Conn]struct{}, maxNodeAttempts)

	for attempt := 1; attempt <= maxNodeAttempts; attempt++ {
		targetWS = state.GetNextClientExcluding(excludedNodes)
		if targetWS == nil {
			statusCode = 503
			state.Metrics.RecordRequestFinished(routeKey, statusCode, float64(time.Since(startTime).Milliseconds()), 0, false)
			if attempt == 1 {
				log.Printf("[ERR] req=%s node=nil route=%s status=503 dur=%dms reason=no_available_node", reqID, routeKey, time.Since(startTime).Milliseconds())
				c.String(http.StatusServiceUnavailable, "Gateway Error: 没有可用的内网节点")
			} else {
				log.Printf("[ERR] req=%s node=nil route=%s status=503 dur=%dms attempts=%d reason=no_available_node_for_retry", reqID, routeKey, time.Since(startTime).Milliseconds(), attempt-1)
				c.String(http.StatusServiceUnavailable, "Gateway Error: 没有可用的重试节点")
			}
			return
		}

		nodeKey = targetWS.Host
		nodeReqID = reqID
		if attempt > 1 {
			nodeReqID = reqID + "-retry-" + strconv.Itoa(attempt)
		}

		state.Metrics.RecordAttemptStarted(nodeKey)
		queue = state.CreatePendingRequest(targetWS.Conn, nodeReqID)

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
			c.String(http.StatusBadGateway, "Gateway Error: 节点下发失败")
			return
		}

		select {
		case firstMsg = <-queue:
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
				c.String(http.StatusGatewayTimeout, "Gateway Error: 节点无响应")
				return
			}
		case <-time.After(nodeResponseIdleTimeout):
			state.SendCancelToNode(targetWS.Conn, nodeReqID)
			state.CleanupPendingRequest(nodeReqID)
			state.Metrics.RecordAttemptFinished(nodeKey, http.StatusGatewayTimeout, 0, false)
			state.CooldownClient(targetWS.Conn, nodeTimeoutCooldown)
			statusCode = http.StatusGatewayTimeout
			state.Metrics.RecordRequestFinished(routeKey, statusCode, float64(time.Since(startTime).Milliseconds()), 0, false)
			log.Printf("[ERR] req=%s node=%s route=%s status=504 dur=%dms attempts=%d reason=idle_timeout threshold=%ds", reqID, nodeKey, routeKey, time.Since(startTime).Milliseconds(), attempt, config.NodeResponseIdleTimeout)
			c.String(http.StatusGatewayTimeout, "Gateway Error: 请求超时")
			return
		case <-c.Request.Context().Done():
			state.SendCancelToNode(targetWS.Conn, nodeReqID)
			state.CleanupPendingRequest(nodeReqID)
			statusCode = statusClientClosed
			state.Metrics.RecordRequestFinished(routeKey, statusCode, float64(time.Since(startTime).Milliseconds()), 0, false)
			state.Metrics.RecordAttemptFinished(nodeKey, statusCode, 0, false)
			log.Printf("[ERR] req=%s node=%s route=%s status=%d dur=%dms reason=client_disconnected_before_first_response", reqID, nodeKey, routeKey, statusClientClosed, time.Since(startTime).Milliseconds())
			return
		}

		break
	}
	defer state.CleanupPendingRequest(nodeReqID)

	firstByteTime = time.Now()
	ttftMs := float64(firstByteTime.Sub(startTime).Milliseconds())

	statusCode = 200
	if s, ok := firstMsg["status"].(float64); ok {
		statusCode = int(s)
	}

	if statusCode >= 400 {
		success = false
		state.Metrics.RecordRequestFinished(routeKey, statusCode, float64(time.Since(startTime).Milliseconds()), ttftMs, false)
		state.Metrics.RecordAttemptFinished(nodeKey, statusCode, ttftMs, false)
		bodyStr, _ := firstMsg["body"].(string)
		log.Printf("[ERR] req=%s node=%s route=%s status=%d dur=%dms ttft=%dms reason=upstream_error body=%s", reqID, nodeKey, routeKey, statusCode, time.Since(startTime).Milliseconds(), int64(ttftMs), bodyFingerprint(bodyStr))
		c.String(statusCode, bodyStr)
		return
	}

	contentType, responseHeaders := normalizeResponseHeaders(firstMsg["headers"])
	applyResponseHeaders(c, responseHeaders)

	if isStreaming {
		c.Writer.Header().Set("Content-Type", "text/event-stream")
		c.Writer.Header().Set("Cache-Control", "no-cache")
		c.Writer.Header().Set("Connection", "keep-alive")
	} else {
		c.Writer.Header().Set("Content-Type", contentType)
	}

	flusher, _ := c.Writer.(http.Flusher)
	var streamConv *converter.ResponsesStreamConverter
	if isResponses {
		streamConv = converter.NewResponsesStreamConverter(modelName)
	}

	clientGone := c.Request.Context().Done()

	if !isStreaming {
		rawBody, ok, clientCancelled := collectResponseBody(queue, clientGone, targetWS.Conn, reqID, nodeReqID, nodeKey, routeKey)
		if !ok {
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
			c.String(http.StatusBadGateway, "Gateway Error: 节点返回异常")
			return
		}
		success = true
		durationMs := float64(time.Since(startTime).Milliseconds())
		state.Metrics.RecordRequestFinished(routeKey, statusCode, durationMs, ttftMs, true)
		state.Metrics.RecordAttemptFinished(nodeKey, statusCode, ttftMs, true)

		// Try to extract usage from non-streaming response
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

	// Streaming loop - track usage from finish message
	var finishUsage map[string]interface{}
	for {
		var msg map[string]interface{}
		var ok bool
		select {
		case msg, ok = <-queue:
			if !ok {
				// Channel closed - record as failure if not already recorded
				if !success {
					statusCode = 502
					state.Metrics.RecordRequestFinished(routeKey, statusCode, float64(time.Since(startTime).Milliseconds()), ttftMs, false)
					state.Metrics.RecordAttemptFinished(nodeKey, statusCode, ttftMs, false)
					state.CooldownClient(targetWS.Conn, nodeFailureCooldown)
					log.Printf("[ERR] req=%s node=%s route=%s status=502 dur=%dms ttft=%dms reason=stream_channel_closed", reqID, nodeKey, routeKey, time.Since(startTime).Milliseconds(), int64(ttftMs))
				}
				return
			}
		case <-clientGone:
			state.SendCancelToNode(targetWS.Conn, reqID)
			log.Printf("[ERR] req=%s node=%s route=%s status=%d dur=%dms reason=client_disconnected", reqID, nodeKey, routeKey, statusClientClosed, time.Since(startTime).Milliseconds())
			state.Metrics.RecordRequestFinished(routeKey, statusClientClosed, float64(time.Since(startTime).Milliseconds()), ttftMs, false)
			state.Metrics.RecordAttemptFinished(nodeKey, statusClientClosed, ttftMs, false)
			return
		case <-time.After(nodeResponseIdleTimeout):
			statusCode = 504
			state.Metrics.RecordRequestFinished(routeKey, statusCode, float64(time.Since(startTime).Milliseconds()), ttftMs, false)
			state.Metrics.RecordAttemptFinished(nodeKey, statusCode, ttftMs, false)
			state.SendCancelToNode(targetWS.Conn, reqID)
			state.CooldownClient(targetWS.Conn, nodeTimeoutCooldown)
			log.Printf("[ERR] req=%s node=%s route=%s status=504 dur=%dms ttft=%dms reason=stream_idle_timeout threshold=%ds", reqID, nodeKey, routeKey, time.Since(startTime).Milliseconds(), int64(ttftMs), config.NodeResponseIdleTimeout)
			return
		}

		msgType, ok := msg["type"].(string)
		if !ok {
			continue
		}

		if msgType == "finish" {
			// Extract usage from finish message
			if usage, ok := msg["usage"].(map[string]interface{}); ok {
				finishUsage = usage
			}

			if isResponses && streamConv != nil {
				var finalizeErr error
				for _, ev := range streamConv.Finalize() {
					if _, err := c.Writer.Write([]byte(ev)); err != nil {
						state.SendCancelToNode(targetWS.Conn, reqID)
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
			success = true
			durationMs := float64(time.Since(startTime).Milliseconds())
			state.Metrics.RecordRequestFinished(routeKey, statusCode, durationMs, ttftMs, true)
			state.Metrics.RecordAttemptFinished(nodeKey, statusCode, ttftMs, true)
			if finishUsage != nil {
				state.Metrics.RecordUsage(routeKey, finishUsage)
			}
			break
		} else if msgType == "chunk" {
			if bodyStr, ok := msg["body"].(string); ok {
				// 优化：只有当字符串中实际包含 "usage" 字眼时，才去触发昂贵的 JSON 反序列化解析。
				// 极大降低高频流式输出带来的 CPU 负担。
				if finishUsage == nil && !isResponses && strings.Contains(bodyStr, `"usage"`) {
					var chunkData map[string]interface{}
					if err := json.Unmarshal([]byte(bodyStr), &chunkData); err == nil {
						// Check for SSE format: data: {...}
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

				if isResponses && streamConv != nil {
					events := streamConv.ProcessChunk(bodyStr)
					for _, ev := range events {
						if _, err := c.Writer.Write([]byte(ev)); err != nil {
							state.SendCancelToNode(targetWS.Conn, reqID)
							log.Printf("[ERR] req=%s node=%s route=%s status=%d dur=%dms reason=stream_write_failed err=%v", reqID, nodeKey, routeKey, statusClientClosed, time.Since(startTime).Milliseconds(), err)
							state.Metrics.RecordRequestFinished(routeKey, statusClientClosed, float64(time.Since(startTime).Milliseconds()), ttftMs, false)
							state.Metrics.RecordAttemptFinished(nodeKey, statusClientClosed, ttftMs, false)
							return
						}
					}
				} else {
					if _, err := c.Writer.Write([]byte(bodyStr)); err != nil {
						state.SendCancelToNode(targetWS.Conn, reqID)
						log.Printf("[ERR] req=%s node=%s route=%s status=%d dur=%dms reason=stream_write_failed err=%v", reqID, nodeKey, routeKey, statusClientClosed, time.Since(startTime).Milliseconds(), err)
						state.Metrics.RecordRequestFinished(routeKey, statusClientClosed, float64(time.Since(startTime).Milliseconds()), ttftMs, false)
						state.Metrics.RecordAttemptFinished(nodeKey, statusClientClosed, ttftMs, false)
						return
					}
				}

				if isStreaming && flusher != nil {
					flusher.Flush()
				}
			}
		} else if msgType == "error" {
			statusCode = 502
			state.Metrics.RecordRequestFinished(routeKey, statusCode, float64(time.Since(startTime).Milliseconds()), ttftMs, false)
			state.Metrics.RecordAttemptFinished(nodeKey, statusCode, ttftMs, false)
			state.CooldownClient(targetWS.Conn, nodeFailureCooldown)
			errBody, _ := msg["body"].(string)
			log.Printf("[ERR] req=%s node=%s route=%s status=502 dur=%dms ttft=%dms reason=node_error_msg body=%s", reqID, nodeKey, routeKey, time.Since(startTime).Milliseconds(), int64(ttftMs), bodyFingerprint(errBody))
			break
		}
	}
}

func bodyFingerprint(s string) string {
	sum := sha256.Sum256([]byte(s))
	return "len=" + strconv.Itoa(len(s)) + " sha256=" + hex.EncodeToString(sum[:8])
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
	{"mimo-v2-pro", "MiMo V2 Pro", 1048576, 131072},
	{"mimo-v2-flash", "MiMo V2 Flash", 256000, 131072},
	{"mimo-v2-omni", "MiMo V2 Omni", 256000, 131072},
	{"mimo-v2.5-tts-voicedesign", "MiMo V2.5 TTS VoiceDesign", 8192, 8192},
	{"mimo-v2.5-tts-voiceclone", "MiMo V2.5 TTS VoiceClone", 8192, 8192},
	{"mimo-v2-tts", "MiMo V2 TTS", 8192, 8192},
}

func ModelsHandler(c *gin.Context) {
	var data []interface{}
	for _, m := range Models {
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
	for _, m := range Models {
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
		"first_id": Models[0].ID,
		"last_id":  Models[len(Models)-1].ID,
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

	state.RegisterClient(ws, c.ClientIP())
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

func collectResponseBody(queue chan map[string]interface{}, clientGone <-chan struct{}, ws *websocket.Conn, logReqID, nodeReqID, nodeKey, routeKey string) (string, bool, bool) {
	var builder strings.Builder

	for {
		select {
		case msg, ok := <-queue:
			if !ok {
				return builder.String(), false, false
			}
			msgType, _ := msg["type"].(string)
			switch msgType {
			case "finish":
				return builder.String(), true, false
			case "error":
				return builder.String(), false, false
			case "chunk":
				if body, ok := msg["body"].(string); ok {
					builder.WriteString(body)
				}
			}
		case <-clientGone:
			state.SendCancelToNode(ws, nodeReqID)
			log.Printf("[ERR] req=%s node=%s route=%s reason=client_disconnected_nonstream", logReqID, nodeKey, routeKey)
			return builder.String(), false, true
		}
	}
}
