package metrics

import (
	"database/sql"
	"testing"

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
