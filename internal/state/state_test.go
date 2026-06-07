package state

import (
	"encoding/json"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"mimo2api/internal/config"
)

func resetClientStateForTest() {
	ActiveClients = make(map[*websocket.Conn]*TunnelClient)
	PendingQueues = make(map[string]chan map[string]interface{})
	ReqIDToWS = make(map[string]*websocket.Conn)
	WSToReqIDs = make(map[*websocket.Conn]map[string]bool)
	BridgeReady = make(map[*websocket.Conn]bool)
	CurrentClientIdx = 0
	ActiveList = nil
}

func resetMetricsStateForTest() {
	Metrics.mu.Lock()
	defer Metrics.mu.Unlock()

	Metrics.RequestsTotal = 0
	Metrics.RequestsSucceeded = 0
	Metrics.RequestsFailed = 0
	Metrics.StreamingRequests = 0
	Metrics.NonStreamingRequests = 0
	Metrics.AttemptsTotal = 0
	Metrics.AttemptsSucceeded = 0
	Metrics.AttemptsFailed = 0
	Metrics.RequestLatencySumMs = 0
	Metrics.RequestFirstByteLatencySumMs = 0
	Metrics.RequestLatencySamplesMs = nil
	Metrics.RequestFirstByteSamplesMs = nil
	Metrics.StatusCodes = make(map[string]int64)
	Metrics.Tokens = TokenMetrics{}
	Metrics.Routes = make(map[string]*RouteMetrics)
	Metrics.Nodes = make(map[string]*NodeMetrics)
	Metrics.HistoryLastSnapshot = nil
	Metrics.HistoryLastBucketStart = 0
}

func TestGatewayMetricsBucketsKnownModelRoutesAndOther(t *testing.T) {
	resetMetricsStateForTest()
	t.Cleanup(resetMetricsStateForTest)

	Metrics.RecordRequestStarted("mimo-v2.5", true)
	Metrics.RecordRequestFinished("MiMo-v2.5", http.StatusOK, 120, 30, true)
	Metrics.RecordUsage("mimo-v2.5", map[string]interface{}{
		"prompt_tokens":     float64(10),
		"completion_tokens": float64(20),
	})

	Metrics.RecordRequestStarted("MiMo-v2.5-pro", true)
	Metrics.RecordRequestFinished("mimo-v2.5-pro", http.StatusOK, 60, 20, true)

	Metrics.RecordRequestStarted("gpt-4o", false)
	Metrics.RecordRequestFinished("claude-3.5", http.StatusBadGateway, 80, 25, false)
	Metrics.RecordUsage("custom-model", map[string]interface{}{
		"input_tokens":  float64(3),
		"output_tokens": float64(4),
	})

	_, _, _, _, _, _, _, _, _, _, _, _, _, _, routes, _ := Metrics.SnapshotRead()
	if len(routes) != 3 {
		t.Fatalf("expected 2 model route buckets plus other, got %d: %#v", len(routes), routes)
	}

	mimo25Route := routes["mimo-v2.5"]
	if mimo25Route == nil || mimo25Route.RequestsTotal != 1 || mimo25Route.RequestsSucceeded != 1 || mimo25Route.Tokens.TotalTokens != 30 {
		t.Fatalf("unexpected mimo-v2.5 route metrics: %#v", mimo25Route)
	}

	mimoProRoute := routes["mimo-v2.5-pro"]
	if mimoProRoute == nil || mimoProRoute.RequestsTotal != 1 || mimoProRoute.RequestsSucceeded != 1 {
		t.Fatalf("unexpected mimo-v2.5-pro route metrics: %#v", mimoProRoute)
	}

	otherRoute := routes[RouteMetricsOther]
	if otherRoute == nil || otherRoute.RequestsTotal != 1 || otherRoute.RequestsFailed != 1 || otherRoute.Tokens.TotalTokens != 7 {
		t.Fatalf("unexpected other route metrics: %#v", otherRoute)
	}
}

func TestGatewayMetricsLoadSnapshotPreservesKnownModelRoutesAndCollapsesOther(t *testing.T) {
	resetMetricsStateForTest()
	t.Cleanup(resetMetricsStateForTest)

	oldDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("get cwd: %v", err)
	}
	if err := os.Chdir(t.TempDir()); err != nil {
		t.Fatalf("chdir temp dir: %v", err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(oldDir); err != nil {
			t.Fatalf("restore cwd: %v", err)
		}
	})

	data := metricsSnapshotData{
		StatusCodes: map[string]int64{},
		Routes: map[string]routeSnap{
			"mimo-v2.5":     {RequestsTotal: 1, RequestsSucceeded: 1, LatencySumMs: 100},
			"MiMo-v2.5-pro": {RequestsTotal: 2, RequestsFailed: 2, LatencySumMs: 200},
			"gpt-4o":        {RequestsTotal: 3, RequestsSucceeded: 3, LatencySumMs: 300},
			"other":         {RequestsTotal: 4, RequestsFailed: 4, LatencySumMs: 400},
		},
		Nodes: map[string]nodeSnap{},
		HistoryLastSnapshot: &MetricsSnapshot{
			Routes: map[string]RouteSnapshotEntry{
				"mimo-v2.5":     {RequestsTotal: 5},
				"mimo-v2-flash": {RequestsTotal: 6},
				"claude":        {RequestsTotal: 7},
			},
		},
	}
	raw, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}
	if err := os.WriteFile(MetricsSnapshotPath, raw, 0644); err != nil {
		t.Fatalf("write snapshot: %v", err)
	}

	if ok := Metrics.LoadSnapshot(); !ok {
		t.Fatal("expected snapshot to load")
	}

	_, _, _, _, _, _, _, _, _, _, _, _, _, _, routes, _ := Metrics.SnapshotRead()
	if len(routes) != 3 {
		t.Fatalf("expected 2 model route buckets plus other after load, got %d: %#v", len(routes), routes)
	}
	if routes["mimo-v2.5"].RequestsTotal != 1 || routes["mimo-v2.5-pro"].RequestsTotal != 2 || routes[RouteMetricsOther].RequestsTotal != 7 {
		t.Fatalf("unexpected collapsed routes: %#v", routes)
	}

	historySnap := Metrics.GetHistoryLastSnapshot()
	if historySnap == nil ||
		historySnap.Routes["mimo-v2.5"].RequestsTotal != 5 ||
		historySnap.Routes["mimo-v2-flash"].RequestsTotal != 6 ||
		historySnap.Routes[RouteMetricsOther].RequestsTotal != 7 {
		t.Fatalf("unexpected collapsed history snapshot: %#v", historySnap)
	}
}

func TestGetNextClientNormalizesStaleIndex(t *testing.T) {
	resetClientStateForTest()

	now := int64(0)
	for i := 0; i < 5; i++ {
		ws := &websocket.Conn{}
		ActiveList = append(ActiveList, ws)
		ActiveClients[ws] = &TunnelClient{
			Conn:          ws,
			Host:          "node",
			CooldownUntil: now,
			BanUntil:      now,
		}
		BridgeReady[ws] = true
	}
	CurrentClientIdx = 9

	client := GetNextClient()
	if client == nil {
		t.Fatal("expected an available client")
	}
	if CurrentClientIdx != 0 {
		t.Fatalf("expected normalized next index to be 0, got %d", CurrentClientIdx)
	}
}

func TestUnregisterClientNormalizesCurrentClientIdx(t *testing.T) {
	resetClientStateForTest()

	ws1 := &websocket.Conn{}
	ws2 := &websocket.Conn{}
	ws3 := &websocket.Conn{}

	RegisterClient(ws1, "node-1")
	RegisterClient(ws2, "node-2")
	RegisterClient(ws3, "node-3")
	CurrentClientIdx = 2

	UnregisterClient(ws3)

	if got := len(ActiveList); got != 2 {
		t.Fatalf("expected 2 active clients after unregister, got %d", got)
	}
	if CurrentClientIdx != 0 {
		t.Fatalf("expected current client index to wrap to 0, got %d", CurrentClientIdx)
	}
}

func TestSendCancelToNodeDoesNotPanicForUnknownConn(t *testing.T) {
	resetClientStateForTest()
	SendCancelToNode(&websocket.Conn{}, "req-1")
}

func TestCooldownClientMakesNodeUnavailable(t *testing.T) {
	resetClientStateForTest()

	ws := &websocket.Conn{}
	RegisterClient(ws, "node-1")

	if got := GetAvailableClientsCount(); got != 1 {
		t.Fatalf("expected 1 available client before cooldown, got %d", got)
	}

	CooldownClient(ws, 30*time.Second)

	if got := GetAvailableClientsCount(); got != 0 {
		t.Fatalf("expected 0 available clients during cooldown, got %d", got)
	}
}

func TestGetNextClientExcludingSkipsProvidedNode(t *testing.T) {
	resetClientStateForTest()

	ws1 := &websocket.Conn{}
	ws2 := &websocket.Conn{}

	RegisterClient(ws1, "node-1")
	RegisterClient(ws2, "node-2")

	client := GetNextClientExcluding(map[*websocket.Conn]struct{}{
		ws1: {},
	})
	if client == nil {
		t.Fatal("expected an available client")
	}
	if client.Conn != ws2 {
		t.Fatalf("expected node-2, got %s", client.Host)
	}
}

func TestPushResponseClosedChannelDoesNotPanic(t *testing.T) {
	resetClientStateForTest()

	ch := make(chan map[string]interface{})
	close(ch)

	reqID := "req-closed"
	PendingQueues[reqID] = ch

	if ok := PushResponse(reqID, map[string]interface{}{"type": "finish"}); ok {
		t.Fatal("expected push to closed channel to fail")
	}
}

func TestGetAvailableClientsCountRespectsConfiguredLimit(t *testing.T) {
	resetClientStateForTest()

	oldMaxPending := config.MaxPendingPerClient
	config.MaxPendingPerClient = 2
	defer func() {
		config.MaxPendingPerClient = oldMaxPending
	}()

	ws := &websocket.Conn{}
	RegisterClient(ws, "node-1")
	WSToReqIDs[ws]["req-1"] = true

	if got := GetAvailableClientsCount(); got != 1 {
		t.Fatalf("expected node to remain available below configured limit, got %d", got)
	}

	WSToReqIDs[ws]["req-2"] = true

	if got := GetAvailableClientsCount(); got != 0 {
		t.Fatalf("expected node to become unavailable at configured limit, got %d", got)
	}
}
