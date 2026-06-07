package metrics

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"mimo2api/internal/state"
)

func resetMetricsStateForTest() {
	state.Metrics.RequestsTotal = 0
	state.Metrics.RequestsSucceeded = 0
	state.Metrics.RequestsFailed = 0
	state.Metrics.StreamingRequests = 0
	state.Metrics.NonStreamingRequests = 0
	state.Metrics.AttemptsTotal = 0
	state.Metrics.AttemptsSucceeded = 0
	state.Metrics.AttemptsFailed = 0
	state.Metrics.RequestLatencySumMs = 0
	state.Metrics.RequestFirstByteLatencySumMs = 0
	state.Metrics.RequestLatencySamplesMs = nil
	state.Metrics.RequestFirstByteSamplesMs = nil
	state.Metrics.StatusCodes = make(map[string]int64)
	state.Metrics.Tokens = state.TokenMetrics{}
	state.Metrics.Routes = make(map[string]*state.RouteMetrics)
	state.Metrics.Nodes = make(map[string]*state.NodeMetrics)
	state.Metrics.SetHistoryState(0, nil)
}

func TestFlushHistoryBucketDoesNotAdvanceBaselineOnWriteError(t *testing.T) {
	resetMetricsStateForTest()
	BucketSeconds = 60
	RetentionDays = 1

	prevSnap := &state.MetricsSnapshot{
		CapturedAt: 120,
		Gateway: state.GatewaySnapshotEntry{
			RequestsTotal:     5,
			RequestsSucceeded: 4,
			RequestsFailed:    1,
		},
		Routes: map[string]state.RouteSnapshotEntry{},
	}
	state.Metrics.SetHistoryState(60, prevSnap)

	state.Metrics.RequestsTotal = 9
	state.Metrics.RequestsSucceeded = 7
	state.Metrics.RequestsFailed = 2

	previousDB := db
	closedDB, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	if err := closedDB.Close(); err != nil {
		t.Fatalf("close sqlite db: %v", err)
	}
	db = closedDB
	defer func() {
		db = previousDB
	}()

	err = flushHistoryBucket(120)
	if err == nil {
		t.Fatal("expected flushHistoryBucket to return an error")
	}

	gotSnap, gotBucketStart := state.Metrics.GetHistoryState()
	if gotBucketStart != 60 {
		t.Fatalf("expected history bucket start to stay at 60, got %d", gotBucketStart)
	}
	if gotSnap == nil || gotSnap.Gateway.RequestsTotal != 5 {
		t.Fatalf("expected previous baseline snapshot to be preserved, got %#v", gotSnap)
	}
}

func TestFlushHistoryBucketResetsBaselineOnBucketGap(t *testing.T) {
	resetMetricsStateForTest()
	BucketSeconds = 60
	RetentionDays = 1
	db = nil

	prevSnap := &state.MetricsSnapshot{
		CapturedAt: 120,
		Gateway: state.GatewaySnapshotEntry{
			RequestsTotal:     5,
			RequestsSucceeded: 4,
			RequestsFailed:    1,
		},
		Routes: map[string]state.RouteSnapshotEntry{},
	}
	state.Metrics.SetHistoryState(60, prevSnap)

	state.Metrics.RequestsTotal = 12
	state.Metrics.RequestsSucceeded = 10
	state.Metrics.RequestsFailed = 2

	if err := flushHistoryBucket(180); err != nil {
		t.Fatalf("expected bucket gap reset to succeed, got %v", err)
	}

	gotSnap, gotBucketStart := state.Metrics.GetHistoryState()
	if gotBucketStart != 180 {
		t.Fatalf("expected baseline bucket start to reset to 180, got %d", gotBucketStart)
	}
	if gotSnap == nil || gotSnap.Gateway.RequestsTotal != 12 {
		t.Fatalf("expected baseline snapshot to reset to current metrics, got %#v", gotSnap)
	}
}

func TestBuildHistoryRowsCalculatesGatewayAndRouteDeltas(t *testing.T) {
	BucketSeconds = 60

	previous := &state.MetricsSnapshot{
		Gateway: state.GatewaySnapshotEntry{
			RequestsTotal:       10,
			RequestsSucceeded:   8,
			RequestsFailed:      2,
			RequestLatencySumMs: 500,
		},
		Routes: map[string]state.RouteSnapshotEntry{
			"/chat": {
				RequestsTotal:       4,
				RequestsSucceeded:   3,
				RequestsFailed:      1,
				RequestLatencySumMs: 120,
			},
		},
	}
	current := state.MetricsSnapshot{
		Gateway: state.GatewaySnapshotEntry{
			RequestsTotal:       16,
			RequestsSucceeded:   13,
			RequestsFailed:      3,
			RequestLatencySumMs: 920,
		},
		Routes: map[string]state.RouteSnapshotEntry{
			"/chat": {
				RequestsTotal:       7,
				RequestsSucceeded:   5,
				RequestsFailed:      2,
				RequestLatencySumMs: 330,
			},
		},
	}

	rows := buildHistoryRows(120, current, previous)
	if len(rows) != 2 {
		t.Fatalf("expected 2 history rows, got %d", len(rows))
	}

	var gatewayRow, routeRow *HistoryRow
	for i := range rows {
		switch rows[i].ComponentType {
		case "gateway":
			gatewayRow = &rows[i]
		case "route":
			routeRow = &rows[i]
		}
	}

	if gatewayRow == nil {
		t.Fatal("expected gateway row")
	}
	if gatewayRow.RequestsTotal != 6 || gatewayRow.RequestsSucceeded != 5 || gatewayRow.RequestsFailed != 1 {
		t.Fatalf("unexpected gateway delta: %#v", gatewayRow)
	}
	if gatewayRow.AvgLatencyMs != 70 || gatewayRow.SuccessRate != 83.33 || gatewayRow.Status != "major_outage" {
		t.Fatalf("unexpected gateway aggregates: %#v", gatewayRow)
	}

	if routeRow == nil {
		t.Fatal("expected route row")
	}
	if routeRow.ComponentKey != "/chat" || routeRow.RequestsTotal != 3 {
		t.Fatalf("unexpected route row: %#v", routeRow)
	}
	if routeRow.AvgLatencyMs != 70 || routeRow.SuccessRate != 66.67 || routeRow.Status != "major_outage" {
		t.Fatalf("unexpected route aggregates: %#v", routeRow)
	}
}

func TestGetStatusHistoryPreservesKnownModelRoutesAndCollapsesOther(t *testing.T) {
	resetMetricsStateForTest()

	previousDB := db
	if err := InitDB(filepath.Join(t.TempDir(), "metrics.db")); err != nil {
		t.Fatalf("init db: %v", err)
	}
	testDB := db
	t.Cleanup(func() {
		_ = testDB.Close()
		db = previousDB
	})

	bucketStart := time.Now().Unix()/int64(BucketSeconds)*int64(BucketSeconds) - int64(BucketSeconds)
	rows := []HistoryRow{
		{ComponentType: "route", ComponentKey: "mimo-v2.5", BucketStart: bucketStart, RequestsTotal: 1, RequestsSucceeded: 1, RequestsFailed: 0, SuccessRate: 100, AvgLatencyMs: 100, Status: "operational"},
		{ComponentType: "route", ComponentKey: "MiMo-v2.5-pro", BucketStart: bucketStart, RequestsTotal: 2, RequestsSucceeded: 1, RequestsFailed: 1, SuccessRate: 50, AvgLatencyMs: 50, Status: "major_outage"},
		{ComponentType: "route", ComponentKey: "gpt-4o", BucketStart: bucketStart, RequestsTotal: 4, RequestsSucceeded: 4, RequestsFailed: 0, SuccessRate: 100, AvgLatencyMs: 25, Status: "operational"},
	}
	if err := writeHistoryRows(rows); err != nil {
		t.Fatalf("write history rows: %v", err)
	}

	data, err := GetStatusHistory(1)
	if err != nil {
		t.Fatalf("get status history: %v", err)
	}

	components := data["components"].([]StatusHistoryComponent)
	var routeComponents []StatusHistoryComponent
	for _, comp := range components {
		if comp.ComponentType == "route" {
			routeComponents = append(routeComponents, comp)
		}
	}
	if len(routeComponents) != 3 {
		t.Fatalf("expected 2 model route components plus other, got %d: %#v", len(routeComponents), routeComponents)
	}

	byKey := make(map[string]StatusHistoryComponent, len(routeComponents))
	for _, comp := range routeComponents {
		byKey[comp.ComponentKey] = comp
	}
	mimo25Comp := byKey["mimo-v2.5"]
	if mimo25Comp.Summary["requests_total"] != 1 || mimo25Comp.Summary["requests_succeeded"] != 1 || mimo25Comp.UptimePercentage != 100 {
		t.Fatalf("unexpected mimo-v2.5 history component: %#v", mimo25Comp)
	}
	mimoProComp := byKey["mimo-v2.5-pro"]
	if mimoProComp.Summary["requests_total"] != 2 || mimoProComp.Summary["requests_succeeded"] != 1 || mimoProComp.UptimePercentage != 50 {
		t.Fatalf("unexpected mimo-v2.5-pro history component: %#v", mimoProComp)
	}
	otherComp := byKey[state.RouteMetricsOther]
	if otherComp.Summary["requests_total"] != 4 || otherComp.Summary["requests_succeeded"] != 4 || otherComp.UptimePercentage != 100 {
		t.Fatalf("unexpected other history component: %#v", otherComp)
	}
}
