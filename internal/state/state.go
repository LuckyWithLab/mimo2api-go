package state

import (
	"encoding/json"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// ─── 常量定义 ───

const (
	MaxConcurrentRequests = 16                      // 单个节点的最大并发请求数
	MaxLatencySamples     = 2000                    // 延迟采样保留的最大数量
	PendingQueueSize      = 100                     // 挂起队列的通道缓冲大小
	PushResponseTimeout   = 1 * time.Second         // 推送响应的超时时间
	NodeWriteTimeout      = 5 * time.Second         // 向节点下发请求/取消的写超时
	MetricsSnapshotPath   = "gateway_snapshot.json" // 指标快照保存路径
)

type TunnelClient struct {
	Conn          *websocket.Conn
	Host          string
	CooldownUntil int64
	BanUntil      int64
	Mu            sync.Mutex
}

// ─── 指标监控 ───

type RouteMetrics struct {
	RequestsTotal                int64
	RequestsSucceeded            int64
	RequestsFailed               int64
	StreamingRequests            int64
	NonStreamingRequests         int64
	RequestLatencySumMs          float64
	RequestFirstByteLatencySumMs float64
	StatusCodes                  map[string]int64
	Tokens                       TokenMetrics
}

type TokenMetrics struct {
	RequestsWithUsage int64
	PromptTokens      int64
	CompletionTokens  int64
	TotalTokens       int64
}

type NodeMetrics struct {
	AttemptsTotal         int64
	AttemptsSucceeded     int64
	AttemptsFailed        int64
	LatencySumMs          float64
	FirstByteLatencySumMs float64
	StatusCodes           map[string]int64
}

type GatewayMetrics struct {
	mu sync.RWMutex

	RequestsTotal                int64
	RequestsSucceeded            int64
	RequestsFailed               int64
	StreamingRequests            int64
	NonStreamingRequests         int64
	AttemptsTotal                int64
	AttemptsSucceeded            int64
	AttemptsFailed               int64
	RequestLatencySumMs          float64
	RequestFirstByteLatencySumMs float64
	RequestLatencySamplesMs      []float64
	RequestFirstByteSamplesMs    []float64
	StatusCodes                  map[string]int64
	Tokens                       TokenMetrics
	Routes                       map[string]*RouteMetrics
	Nodes                        map[string]*NodeMetrics

	// 用于基于快照的历史聚合
	HistoryLastSnapshot    *MetricsSnapshot
	HistoryLastBucketStart int64
}

type MetricsSnapshot struct {
	CapturedAt int64
	Gateway    GatewaySnapshotEntry
	Routes     map[string]RouteSnapshotEntry
}

type GatewaySnapshotEntry struct {
	RequestsTotal       int64
	RequestsSucceeded   int64
	RequestsFailed      int64
	RequestLatencySumMs float64
}

func (e *GatewaySnapshotEntry) GetRequestsTotal() int64         { return e.RequestsTotal }
func (e *GatewaySnapshotEntry) GetRequestsSucceeded() int64     { return e.RequestsSucceeded }
func (e *GatewaySnapshotEntry) GetRequestsFailed() int64        { return e.RequestsFailed }
func (e *GatewaySnapshotEntry) GetRequestLatencySumMs() float64 { return e.RequestLatencySumMs }

type RouteSnapshotEntry struct {
	RequestsTotal       int64
	RequestsSucceeded   int64
	RequestsFailed      int64
	RequestLatencySumMs float64
}

func (e *RouteSnapshotEntry) GetRequestsTotal() int64         { return e.RequestsTotal }
func (e *RouteSnapshotEntry) GetRequestsSucceeded() int64     { return e.RequestsSucceeded }
func (e *RouteSnapshotEntry) GetRequestsFailed() int64        { return e.RequestsFailed }
func (e *RouteSnapshotEntry) GetRequestLatencySumMs() float64 { return e.RequestLatencySumMs }

var Metrics = &GatewayMetrics{
	StatusCodes:               make(map[string]int64),
	Tokens:                    TokenMetrics{},
	Routes:                    make(map[string]*RouteMetrics),
	Nodes:                     make(map[string]*NodeMetrics),
	RequestLatencySamplesMs:   make([]float64, 0, MaxLatencySamples),
	RequestFirstByteSamplesMs: make([]float64, 0, MaxLatencySamples),
}

// ─── GatewayMetrics 方法 ───

func (gm *GatewayMetrics) RecordRequestStarted(routeKey string, isStreaming bool) {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	gm.RequestsTotal++
	if isStreaming {
		gm.StreamingRequests++
	} else {
		gm.NonStreamingRequests++
	}

	rm := gm.ensureRouteMetrics(routeKey)
	rm.RequestsTotal++
	if isStreaming {
		rm.StreamingRequests++
	} else {
		rm.NonStreamingRequests++
	}
}

func (gm *GatewayMetrics) RecordAttemptStarted(nodeKey string) {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	gm.AttemptsTotal++
	nm := gm.ensureNodeMetrics(nodeKey)
	nm.AttemptsTotal++
}

func (gm *GatewayMetrics) RecordAttemptFinished(nodeKey string, statusCode int, firstByteLatencyMs float64, success bool) {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	if success {
		gm.AttemptsSucceeded++
	} else {
		gm.AttemptsFailed++
	}

	nm := gm.ensureNodeMetrics(nodeKey)
	if success {
		nm.AttemptsSucceeded++
	} else {
		nm.AttemptsFailed++
	}
	nm.FirstByteLatencySumMs += firstByteLatencyMs
	nm.StatusCodes[strconv.Itoa(statusCode)]++
}

func (gm *GatewayMetrics) RecordRequestFinished(routeKey string, statusCode int, durationMs float64, firstByteLatencyMs float64, success bool) {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	if success {
		gm.RequestsSucceeded++
	} else {
		gm.RequestsFailed++
	}

	gm.RequestLatencySumMs += durationMs
	gm.RequestFirstByteLatencySumMs += firstByteLatencyMs

	gm.RequestLatencySamplesMs = appendLatencySample(gm.RequestLatencySamplesMs, durationMs, MaxLatencySamples)
	gm.RequestFirstByteSamplesMs = appendLatencySample(gm.RequestFirstByteSamplesMs, firstByteLatencyMs, MaxLatencySamples)

	gm.StatusCodes[strconv.Itoa(statusCode)]++

	rm := gm.ensureRouteMetrics(routeKey)
	if success {
		rm.RequestsSucceeded++
	} else {
		rm.RequestsFailed++
	}
	rm.RequestLatencySumMs += durationMs
	rm.RequestFirstByteLatencySumMs += firstByteLatencyMs
	rm.StatusCodes[strconv.Itoa(statusCode)]++
}

func (gm *GatewayMetrics) RecordUsage(routeKey string, usage map[string]interface{}) {
	if usage == nil {
		return
	}
	promptTokens := int64FromUsage(usage, "prompt_tokens", "input_tokens")
	completionTokens := int64FromUsage(usage, "completion_tokens", "output_tokens")
	totalTokens := int64FromUsage(usage, "total_tokens")
	if totalTokens == 0 {
		totalTokens = promptTokens + completionTokens
	}

	gm.mu.Lock()
	defer gm.mu.Unlock()

	gm.Tokens.RequestsWithUsage++
	gm.Tokens.PromptTokens += promptTokens
	gm.Tokens.CompletionTokens += completionTokens
	gm.Tokens.TotalTokens += totalTokens

	rm := gm.ensureRouteMetrics(routeKey)
	rm.Tokens.RequestsWithUsage++
	rm.Tokens.PromptTokens += promptTokens
	rm.Tokens.CompletionTokens += completionTokens
	rm.Tokens.TotalTokens += totalTokens
}

// ─── 快照功能 ───

func (gm *GatewayMetrics) CaptureSnapshot() MetricsSnapshot {
	gm.mu.RLock()
	defer gm.mu.RUnlock()

	snap := MetricsSnapshot{
		CapturedAt: time.Now().Unix(),
		Gateway: GatewaySnapshotEntry{
			RequestsTotal:       gm.RequestsTotal,
			RequestsSucceeded:   gm.RequestsSucceeded,
			RequestsFailed:      gm.RequestsFailed,
			RequestLatencySumMs: gm.RequestLatencySumMs,
		},
		Routes: make(map[string]RouteSnapshotEntry),
	}

	for k, v := range gm.Routes {
		snap.Routes[k] = RouteSnapshotEntry{
			RequestsTotal:       v.RequestsTotal,
			RequestsSucceeded:   v.RequestsSucceeded,
			RequestsFailed:      v.RequestsFailed,
			RequestLatencySumMs: v.RequestLatencySumMs,
		}
	}

	return snap
}

// ─── 读取辅助方法 (供 StatsHandler 使用) ───

// SnapshotRead 返回用于构建统计信息的完整指标副本。
// 调用方不应持有内部 map 的引用。
func (gm *GatewayMetrics) SnapshotRead() (
	requestsTotal, requestsSucceeded, requestsFailed int64,
	attemptsTotal, attemptsSucceeded, attemptsFailed int64,
	streamingRequests, nonStreamingRequests int64,
	latencySumMs, firstByteLatencySumMs float64,
	latencySamples, firstByteSamples []float64,
	statusCodes map[string]int64,
	tokens TokenMetrics,
	routes map[string]*RouteMetrics,
	nodes map[string]*NodeMetrics,
) {
	gm.mu.RLock()
	defer gm.mu.RUnlock()

	return gm.RequestsTotal, gm.RequestsSucceeded, gm.RequestsFailed,
		gm.AttemptsTotal, gm.AttemptsSucceeded, gm.AttemptsFailed,
		gm.StreamingRequests, gm.NonStreamingRequests,
		gm.RequestLatencySumMs, gm.RequestFirstByteLatencySumMs,
		append([]float64{}, gm.RequestLatencySamplesMs...),
		append([]float64{}, gm.RequestFirstByteSamplesMs...),
		copyMapInt64(gm.StatusCodes),
		gm.Tokens,
		copyRoutes(gm.Routes),
		copyNodes(gm.Nodes)
}

func (gm *GatewayMetrics) GetHistoryLastSnapshot() *MetricsSnapshot {
	gm.mu.RLock()
	defer gm.mu.RUnlock()
	return cloneMetricsSnapshot(gm.HistoryLastSnapshot)
}

func (gm *GatewayMetrics) SetHistoryLastSnapshot(snap *MetricsSnapshot) {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	gm.HistoryLastSnapshot = cloneMetricsSnapshot(snap)
}

func (gm *GatewayMetrics) GetHistoryState() (*MetricsSnapshot, int64) {
	gm.mu.RLock()
	defer gm.mu.RUnlock()
	return cloneMetricsSnapshot(gm.HistoryLastSnapshot), gm.HistoryLastBucketStart
}

func (gm *GatewayMetrics) SetHistoryState(bucketStart int64, snap *MetricsSnapshot) {
	gm.mu.Lock()
	defer gm.mu.Unlock()
	gm.HistoryLastBucketStart = bucketStart
	gm.HistoryLastSnapshot = cloneMetricsSnapshot(snap)
}

// ─── 持久化快照 (保存/加载累积指标到 JSON) ───

type metricsSnapshotData struct {
	SavedAt                float64              `json:"saved_at"`
	RequestsTotal          int64                `json:"requests_total"`
	RequestsSucceeded      int64                `json:"requests_succeeded"`
	RequestsFailed         int64                `json:"requests_failed"`
	StreamingRequests      int64                `json:"streaming_requests"`
	NonStreamingRequests   int64                `json:"non_streaming_requests"`
	AttemptsTotal          int64                `json:"attempts_total"`
	AttemptsSucceeded      int64                `json:"attempts_succeeded"`
	AttemptsFailed         int64                `json:"attempts_failed"`
	RequestLatencySumMs    float64              `json:"request_latency_sum_ms"`
	FirstByteLatSumMs      float64              `json:"request_first_byte_latency_sum_ms"`
	StatusCodes            map[string]int64     `json:"status_codes"`
	Tokens                 tokenSnapshot        `json:"tokens"`
	Routes                 map[string]routeSnap `json:"routes"`
	Nodes                  map[string]nodeSnap  `json:"nodes"`
	HistoryLastBucketStart int64                `json:"history_last_bucket_start"`
	HistoryLastSnapshot    *MetricsSnapshot     `json:"history_last_snapshot,omitempty"`
}

type tokenSnapshot struct {
	RequestsWithUsage int64 `json:"requests_with_usage"`
	PromptTokens      int64 `json:"prompt_tokens"`
	CompletionTokens  int64 `json:"completion_tokens"`
	TotalTokens       int64 `json:"total_tokens"`
}

type routeSnap struct {
	RequestsTotal        int64            `json:"requests_total"`
	RequestsSucceeded    int64            `json:"requests_succeeded"`
	RequestsFailed       int64            `json:"requests_failed"`
	StreamingRequests    int64            `json:"streaming_requests"`
	NonStreamingRequests int64            `json:"non_streaming_requests"`
	LatencySumMs         float64          `json:"request_latency_sum_ms"`
	FirstByteLatSumMs    float64          `json:"request_first_byte_latency_sum_ms"`
	StatusCodes          map[string]int64 `json:"status_codes"`
	Tokens               tokenSnapshot    `json:"tokens"`
}

type nodeSnap struct {
	AttemptsTotal     int64            `json:"attempts_total"`
	AttemptsSucceeded int64            `json:"attempts_succeeded"`
	AttemptsFailed    int64            `json:"attempts_failed"`
	LatencySumMs      float64          `json:"latency_sum_ms"`
	FirstByteLatSumMs float64          `json:"first_byte_latency_sum_ms"`
	StatusCodes       map[string]int64 `json:"status_codes"`
}

func (gm *GatewayMetrics) SaveSnapshot() {
	gm.mu.RLock()
	data := metricsSnapshotData{
		SavedAt:                float64(time.Now().Unix()),
		RequestsTotal:          gm.RequestsTotal,
		RequestsSucceeded:      gm.RequestsSucceeded,
		RequestsFailed:         gm.RequestsFailed,
		StreamingRequests:      gm.StreamingRequests,
		NonStreamingRequests:   gm.NonStreamingRequests,
		AttemptsTotal:          gm.AttemptsTotal,
		AttemptsSucceeded:      gm.AttemptsSucceeded,
		AttemptsFailed:         gm.AttemptsFailed,
		RequestLatencySumMs:    gm.RequestLatencySumMs,
		FirstByteLatSumMs:      gm.RequestFirstByteLatencySumMs,
		StatusCodes:            copyMapInt64(gm.StatusCodes),
		HistoryLastBucketStart: gm.HistoryLastBucketStart,
		HistoryLastSnapshot:    cloneMetricsSnapshot(gm.HistoryLastSnapshot),
		Tokens: tokenSnapshot{
			RequestsWithUsage: gm.Tokens.RequestsWithUsage,
			PromptTokens:      gm.Tokens.PromptTokens,
			CompletionTokens:  gm.Tokens.CompletionTokens,
			TotalTokens:       gm.Tokens.TotalTokens,
		},
		Routes: make(map[string]routeSnap),
		Nodes:  make(map[string]nodeSnap),
	}
	for k, v := range gm.Routes {
		data.Routes[k] = routeSnap{
			RequestsTotal:        v.RequestsTotal,
			RequestsSucceeded:    v.RequestsSucceeded,
			RequestsFailed:       v.RequestsFailed,
			StreamingRequests:    v.StreamingRequests,
			NonStreamingRequests: v.NonStreamingRequests,
			LatencySumMs:         v.RequestLatencySumMs,
			FirstByteLatSumMs:    v.RequestFirstByteLatencySumMs,
			StatusCodes:          copyMapInt64(v.StatusCodes),
			Tokens: tokenSnapshot{
				RequestsWithUsage: v.Tokens.RequestsWithUsage,
				PromptTokens:      v.Tokens.PromptTokens,
				CompletionTokens:  v.Tokens.CompletionTokens,
				TotalTokens:       v.Tokens.TotalTokens,
			},
		}
	}
	for k, v := range gm.Nodes {
		data.Nodes[k] = nodeSnap{
			AttemptsTotal:     v.AttemptsTotal,
			AttemptsSucceeded: v.AttemptsSucceeded,
			AttemptsFailed:    v.AttemptsFailed,
			LatencySumMs:      v.LatencySumMs,
			FirstByteLatSumMs: v.FirstByteLatencySumMs,
			StatusCodes:       copyMapInt64(v.StatusCodes),
		}
	}
	gm.mu.RUnlock()

	tmp := MetricsSnapshotPath + ".tmp"
	b, _ := json.Marshal(data)
	if err := os.WriteFile(tmp, b, 0644); err != nil {
		return
	}
	os.Rename(tmp, MetricsSnapshotPath)
}

func (gm *GatewayMetrics) LoadSnapshot() bool {
	b, err := os.ReadFile(MetricsSnapshotPath)
	if err != nil {
		log.Println("Metrics snapshot file not found:", err)
		return false
	}
	var data metricsSnapshotData
	if err := json.Unmarshal(b, &data); err != nil {
		log.Println("Metrics snapshot parse error:", err)
		return false
	}
	log.Printf("Metrics snapshot loaded: requests_total=%d, tokens=%d, routes=%d, nodes=%d",
		data.RequestsTotal, data.Tokens.TotalTokens, len(data.Routes), len(data.Nodes))

	gm.mu.Lock()
	gm.RequestsTotal = data.RequestsTotal
	gm.RequestsSucceeded = data.RequestsSucceeded
	gm.RequestsFailed = data.RequestsFailed
	gm.StreamingRequests = data.StreamingRequests
	gm.NonStreamingRequests = data.NonStreamingRequests
	gm.AttemptsTotal = data.AttemptsTotal
	gm.AttemptsSucceeded = data.AttemptsSucceeded
	gm.AttemptsFailed = data.AttemptsFailed
	gm.RequestLatencySumMs = data.RequestLatencySumMs
	gm.RequestFirstByteLatencySumMs = data.FirstByteLatSumMs
	gm.StatusCodes = data.StatusCodes
	gm.HistoryLastBucketStart = data.HistoryLastBucketStart
	gm.HistoryLastSnapshot = cloneMetricsSnapshot(data.HistoryLastSnapshot)
	gm.Tokens = TokenMetrics{
		RequestsWithUsage: data.Tokens.RequestsWithUsage,
		PromptTokens:      data.Tokens.PromptTokens,
		CompletionTokens:  data.Tokens.CompletionTokens,
		TotalTokens:       data.Tokens.TotalTokens,
	}
	for k, v := range data.Routes {
		rm := &RouteMetrics{
			RequestsTotal:                v.RequestsTotal,
			RequestsSucceeded:            v.RequestsSucceeded,
			RequestsFailed:               v.RequestsFailed,
			StreamingRequests:            v.StreamingRequests,
			NonStreamingRequests:         v.NonStreamingRequests,
			RequestLatencySumMs:          v.LatencySumMs,
			RequestFirstByteLatencySumMs: v.FirstByteLatSumMs,
			StatusCodes:                  v.StatusCodes,
			Tokens: TokenMetrics{
				RequestsWithUsage: v.Tokens.RequestsWithUsage,
				PromptTokens:      v.Tokens.PromptTokens,
				CompletionTokens:  v.Tokens.CompletionTokens,
				TotalTokens:       v.Tokens.TotalTokens,
			},
		}
		gm.Routes[k] = rm
	}
	for k, v := range data.Nodes {
		nm := &NodeMetrics{
			AttemptsTotal:         v.AttemptsTotal,
			AttemptsSucceeded:     v.AttemptsSucceeded,
			AttemptsFailed:        v.AttemptsFailed,
			LatencySumMs:          v.LatencySumMs,
			FirstByteLatencySumMs: v.FirstByteLatSumMs,
			StatusCodes:           v.StatusCodes,
		}
		gm.Nodes[k] = nm
	}
	gm.mu.Unlock()

	return true
}

func cloneMetricsSnapshot(snap *MetricsSnapshot) *MetricsSnapshot {
	if snap == nil {
		return nil
	}

	cloned := &MetricsSnapshot{
		CapturedAt: snap.CapturedAt,
		Gateway:    snap.Gateway,
		Routes:     make(map[string]RouteSnapshotEntry, len(snap.Routes)),
	}
	for k, v := range snap.Routes {
		cloned.Routes[k] = v
	}
	return cloned
}

// ─── 内部辅助方法 ───

func (gm *GatewayMetrics) ensureRouteMetrics(routeKey string) *RouteMetrics {
	if rm, ok := gm.Routes[routeKey]; ok {
		return rm
	}
	rm := &RouteMetrics{
		StatusCodes: make(map[string]int64),
		Tokens:      TokenMetrics{},
	}
	gm.Routes[routeKey] = rm
	return rm
}

func (gm *GatewayMetrics) ensureNodeMetrics(nodeKey string) *NodeMetrics {
	if nm, ok := gm.Nodes[nodeKey]; ok {
		return nm
	}
	nm := &NodeMetrics{
		StatusCodes: make(map[string]int64),
	}
	gm.Nodes[nodeKey] = nm
	return nm
}

func int64FromUsage(usage map[string]interface{}, keys ...string) int64 {
	for _, key := range keys {
		if v, ok := usage[key]; ok {
			switch n := v.(type) {
			case float64:
				return int64(n)
			case int64:
				return n
			case int:
				return int64(n)
			}
		}
	}
	return 0
}

// appendLatencySample 优化后：避免达到容量时触发新的底层数组分配。
// 利用原切片进行覆盖操作，减轻垃圾回收(GC)压力。
func appendLatencySample(samples []float64, val float64, maxLen int) []float64 {
	if len(samples) < maxLen {
		return append(samples, val)
	}
	// 当容量占满时，整体向前移动一位，覆盖队首（最旧）元素
	copy(samples, samples[1:])
	samples[len(samples)-1] = val
	return samples
}

func copyMapInt64(m map[string]int64) map[string]int64 {
	result := make(map[string]int64, len(m))
	for k, v := range m {
		result[k] = v
	}
	return result
}

func copyRoutes(m map[string]*RouteMetrics) map[string]*RouteMetrics {
	result := make(map[string]*RouteMetrics, len(m))
	for k, v := range m {
		r := *v // 结构体浅拷贝
		r.StatusCodes = copyMapInt64(v.StatusCodes)
		r.Tokens = v.Tokens
		result[k] = &r
	}
	return result
}

func copyNodes(m map[string]*NodeMetrics) map[string]*NodeMetrics {
	result := make(map[string]*NodeMetrics, len(m))
	for k, v := range m {
		n := *v
		n.StatusCodes = copyMapInt64(v.StatusCodes)
		result[k] = &n
	}
	return result
}

func roundTo2(f float64) float64 {
	return float64(int64(f*100+0.5)) / 100
}

// ─── 原始状态变量 ───

var (
	ActiveClients = make(map[*websocket.Conn]*TunnelClient)
	PendingQueues = make(map[string]chan map[string]interface{})
	ReqIDToWS     = make(map[string]*websocket.Conn)
	WSToReqIDs    = make(map[*websocket.Conn]map[string]bool)
	BridgeReady   = make(map[*websocket.Conn]bool)

	CurrentClientIdx = 0
	ActiveList       []*websocket.Conn
	mu               sync.RWMutex
	StartTime        = time.Now()
)

func GetConnectedClientsCount() int {
	mu.RLock()
	defer mu.RUnlock()
	return len(ActiveList)
}

// 补充一个并发安全的获取队列长度的方法
func GetPendingCount() int {
	mu.RLock()
	defer mu.RUnlock()
	return len(PendingQueues)
}

func GetAvailableClientsCount() int {
	mu.RLock()
	defer mu.RUnlock()

	now := time.Now().Unix()
	count := 0
	for _, ws := range ActiveList {
		if tc, ok := ActiveClients[ws]; ok {
			ready := BridgeReady[ws]
			if ready && tc.CooldownUntil <= now && tc.BanUntil <= now {
				reqIDs := WSToReqIDs[ws]
				if len(reqIDs) < MaxConcurrentRequests {
					count++
				}
			}
		}
	}
	return count
}

type ActiveNodeInfo struct {
	Index           int
	Host            string
	Available       bool
	BridgeReady     bool
	CooldownUntil   int64
	BanUntil        int64
	PendingRequests int
}

func GetActiveNodes() []ActiveNodeInfo {
	mu.RLock()
	defer mu.RUnlock()

	now := time.Now().Unix()
	result := []ActiveNodeInfo{}
	for i, ws := range ActiveList {
		tc, ok := ActiveClients[ws]
		if !ok {
			continue
		}
		ready := BridgeReady[ws]
		isAvailable := ready && tc.CooldownUntil <= now && tc.BanUntil <= now && len(WSToReqIDs[ws]) < MaxConcurrentRequests
		result = append(result, ActiveNodeInfo{
			Index:           i,
			Host:            tc.Host,
			Available:       isAvailable,
			BridgeReady:     ready,
			CooldownUntil:   maxInt64(0, tc.CooldownUntil-now),
			BanUntil:        maxInt64(0, tc.BanUntil-now),
			PendingRequests: len(WSToReqIDs[ws]),
		})
	}
	return result
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func RegisterClient(ws *websocket.Conn, host string) {
	mu.Lock()
	defer mu.Unlock()

	client := &TunnelClient{
		Conn: ws,
		Host: host,
	}
	ActiveClients[ws] = client
	ActiveList = append(ActiveList, ws)
	WSToReqIDs[ws] = make(map[string]bool)
	BridgeReady[ws] = true
	log.Printf("[INFO] node=%s reason=ws_register active_clients=%d", host, len(ActiveList))
}

// SendCancelToNode sends a cancel message to the node for a given request.
// The bridge may or may not support it, but we send it anyway as a best-effort cleanup.
func SendCancelToNode(ws *websocket.Conn, reqID string) {
	mu.RLock()
	tc, ok := ActiveClients[ws]
	mu.RUnlock()
	if !ok {
		return
	}

	tc.Mu.Lock()
	defer tc.Mu.Unlock()
	_ = ws.SetWriteDeadline(time.Now().Add(NodeWriteTimeout))
	err := ws.WriteJSON(map[string]interface{}{
		"type":   "cancel",
		"req_id": reqID,
	})
	_ = ws.SetWriteDeadline(time.Time{})
	if err != nil {
		log.Printf("[ERR] node=%s req=%s reason=cancel_send_failed err=%v", tc.Host, reqID, err)
	}
}

func CooldownClient(ws *websocket.Conn, d time.Duration) {
	if d <= 0 {
		return
	}

	mu.Lock()
	defer mu.Unlock()

	tc, ok := ActiveClients[ws]
	if !ok {
		return
	}

	until := time.Now().Add(d).Unix()
	if until > tc.CooldownUntil {
		tc.CooldownUntil = until
	}
}

func CloseClientConn(ws *websocket.Conn) {
	mu.RLock()
	tc, ok := ActiveClients[ws]
	mu.RUnlock()
	if !ok {
		return
	}

	tc.Mu.Lock()
	defer tc.Mu.Unlock()
	_ = ws.Close()
}

func UnregisterClient(ws *websocket.Conn) {
	mu.Lock()
	defer mu.Unlock()

	var host string
	if tc, ok := ActiveClients[ws]; ok {
		host = tc.Host
	}

	if reqs, ok := WSToReqIDs[ws]; ok {
		if len(reqs) > 0 {
			log.Printf("[ERR] node=%s reason=ws_disconnect affected_requests=%d", host, len(reqs))
		}
		for reqID := range reqs {
			if ch, ok := PendingQueues[reqID]; ok {
				select {
				case ch <- map[string]interface{}{
					"status": 502,
					"body":   "Gateway Error: 节点在处理请求时断开连接",
				}:
				default:
				}
				close(ch)
				delete(PendingQueues, reqID)
			}
			delete(ReqIDToWS, reqID)
		}
	}

	delete(ActiveClients, ws)
	delete(WSToReqIDs, ws)
	delete(BridgeReady, ws)

	// O(1) 移除法：找到目标元素，与切片最后一个元素互换并缩减长度。
	// 大幅减少 O(N) 遍历时间，降低全局大锁阻塞其他请求的可能。
	idx := -1
	for i, c := range ActiveList {
		if c == ws {
			idx = i
			break
		}
	}
	if idx != -1 {
		lastIdx := len(ActiveList) - 1
		ActiveList[idx] = ActiveList[lastIdx]
		ActiveList[lastIdx] = nil // 避免潜在的内存泄漏
		ActiveList = ActiveList[:lastIdx]
	}

	if len(ActiveList) == 0 {
		CurrentClientIdx = 0
	} else {
		CurrentClientIdx %= len(ActiveList)
	}

	log.Printf("[INFO] node=%s reason=ws_unregister active_clients=%d", host, len(ActiveList))
}

func GetNextClient() *TunnelClient {
	return GetNextClientExcluding(nil)
}

func GetNextClientExcluding(excluded map[*websocket.Conn]struct{}) *TunnelClient {
	mu.Lock()
	defer mu.Unlock()

	return getNextClientLocked(excluded)
}

func getNextClientLocked(excluded map[*websocket.Conn]struct{}) *TunnelClient {
	if len(ActiveList) == 0 {
		return nil
	}

	CurrentClientIdx %= len(ActiveList)
	now := time.Now().Unix()

	for i := 0; i < len(ActiveList); i++ {
		c := ActiveList[CurrentClientIdx]
		CurrentClientIdx = (CurrentClientIdx + 1) % len(ActiveList)

		if excluded != nil {
			if _, skip := excluded[c]; skip {
				continue
			}
		}

		if tc, ok := ActiveClients[c]; ok {
			ready := BridgeReady[c]
			// 修复：增加了对就绪状态和并发请求上限的校验
			if ready && tc.CooldownUntil <= now && tc.BanUntil <= now {
				if len(WSToReqIDs[c]) < MaxConcurrentRequests {
					return tc
				}
			}
		}
	}
	return nil
}

func CreatePendingRequest(ws *websocket.Conn, reqID string) chan map[string]interface{} {
	mu.Lock()
	defer mu.Unlock()

	ch := make(chan map[string]interface{}, PendingQueueSize)
	PendingQueues[reqID] = ch
	ReqIDToWS[reqID] = ws
	if WSToReqIDs[ws] == nil {
		WSToReqIDs[ws] = make(map[string]bool)
	}
	WSToReqIDs[ws][reqID] = true
	return ch
}

func CleanupPendingRequest(reqID string) {
	mu.Lock()
	defer mu.Unlock()

	if ch, ok := PendingQueues[reqID]; ok {
		close(ch)
		delete(PendingQueues, reqID)
	}

	if ws, ok := ReqIDToWS[reqID]; ok {
		if reqs, ok2 := WSToReqIDs[ws]; ok2 {
			delete(reqs, reqID)
		}
		delete(ReqIDToWS, reqID)
	}
}

func PushResponse(reqID string, data map[string]interface{}) bool {
	mu.RLock()
	ch, ok := PendingQueues[reqID]
	mu.RUnlock()

	if ok {
		// 修复：使用 NewTimer 并在结束时主动清理，避免 time.After() 遗留过多导致内存泄漏
		timer := time.NewTimer(PushResponseTimeout)
		defer timer.Stop()
		defer func() {
			_ = recover()
		}()

		select {
		case ch <- data:
			return true
		case <-timer.C:
			return false
		}
	}
	return false
}
