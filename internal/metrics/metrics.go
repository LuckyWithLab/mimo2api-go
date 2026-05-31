package metrics

import (
	"database/sql"
	"fmt"
	"log"
	"sort"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"mimo2api/internal/config"
	"mimo2api/internal/state"
)

var db *sql.DB

var BucketSeconds int
var RetentionDays int

func InitDB(path string) error {
	BucketSeconds = maxInt(60, config.MetricsBucketSeconds)
	RetentionDays = maxInt(1, config.MetricsRetentionDays)

	var err error
	db, err = sql.Open("sqlite3", path+"?_journal_mode=WAL&_synchronous=NORMAL")
	if err != nil {
		return err
	}

	// Create status_history table (compatible with Python version)
	createTableSQL := `
	CREATE TABLE IF NOT EXISTS status_history (
		component_type TEXT NOT NULL,
		component_key TEXT NOT NULL,
		bucket_start INTEGER NOT NULL,
		requests_total INTEGER NOT NULL,
		requests_succeeded INTEGER NOT NULL,
		requests_failed INTEGER NOT NULL,
		success_rate REAL NOT NULL,
		avg_latency_ms REAL NOT NULL,
		status TEXT NOT NULL,
		PRIMARY KEY (component_type, component_key, bucket_start)
	);`

	createIndexSQL := `
	CREATE INDEX IF NOT EXISTS idx_status_history_bucket
	ON status_history (bucket_start);`

	if _, err = db.Exec(createTableSQL); err != nil {
		return err
	}
	if _, err = db.Exec(createIndexSQL); err != nil {
		return err
	}

	return nil
}

// ─── Snapshot subtraction (for delta calculation) ───

type DeltaResult struct {
	RequestsTotal     int64
	RequestsSucceeded int64
	RequestsFailed    int64
	AvgLatencyMs      float64
	SuccessRate       float64
}

// snapshotFields is an interface for both GatewaySnapshotEntry and RouteSnapshotEntry
type snapshotFields interface {
	GetRequestsTotal() int64
	GetRequestsSucceeded() int64
	GetRequestsFailed() int64
	GetRequestLatencySumMs() float64
}

func subtractSnapshot(current, previous snapshotFields) DeltaResult {
	if previous == nil {
		total := current.GetRequestsTotal()
		succeeded := current.GetRequestsSucceeded()
		failed := current.GetRequestsFailed()
		avgLat := float64(0)
		sRate := float64(0)
		if total > 0 {
			avgLat = roundTo2(current.GetRequestLatencySumMs() / float64(total))
			sRate = roundTo2(float64(succeeded) / float64(total) * 100)
		}
		return DeltaResult{total, succeeded, failed, avgLat, sRate}
	}

	total := max0int64(current.GetRequestsTotal() - previous.GetRequestsTotal())
	succeeded := max0int64(current.GetRequestsSucceeded() - previous.GetRequestsSucceeded())
	failed := max0int64(current.GetRequestsFailed() - previous.GetRequestsFailed())
	latSum := max0float64(current.GetRequestLatencySumMs() - previous.GetRequestLatencySumMs())

	avgLat := float64(0)
	sRate := float64(0)
	if total > 0 {
		avgLat = roundTo2(latSum / float64(total))
		sRate = roundTo2(float64(succeeded) / float64(total) * 100)
	}
	return DeltaResult{total, succeeded, failed, avgLat, sRate}
}

func classifyComponentStatus(totalRequests int64, successRate float64) string {
	if totalRequests <= 0 {
		return "no_data"
	}
	if successRate >= 95 {
		return "operational"
	}
	if successRate >= 85 {
		return "degraded"
	}
	return "major_outage"
}

// ─── Build history rows ───

type HistoryRow struct {
	ComponentType     string
	ComponentKey      string
	BucketStart       int64
	RequestsTotal     int64
	RequestsSucceeded int64
	RequestsFailed    int64
	SuccessRate       float64
	AvgLatencyMs      float64
	Status            string
}

func buildHistoryRows(bucketStart int64, currentSnap state.MetricsSnapshot, previousSnap *state.MetricsSnapshot) []HistoryRow {
	rows := []HistoryRow{}

	gatewayDelta := subtractSnapshot(&currentSnap.Gateway, nil)
	if previousSnap != nil {
		gatewayDelta = subtractSnapshot(&currentSnap.Gateway, &previousSnap.Gateway)
	}

	rows = append(rows, HistoryRow{
		ComponentType:     "gateway",
		ComponentKey:      "gateway",
		BucketStart:       bucketStart,
		RequestsTotal:     gatewayDelta.RequestsTotal,
		RequestsSucceeded: gatewayDelta.RequestsSucceeded,
		RequestsFailed:    gatewayDelta.RequestsFailed,
		SuccessRate:       gatewayDelta.SuccessRate,
		AvgLatencyMs:      gatewayDelta.AvgLatencyMs,
		Status:            classifyComponentStatus(gatewayDelta.RequestsTotal, gatewayDelta.SuccessRate),
	})

	// Routes
	var previousRoutes map[string]state.RouteSnapshotEntry
	if previousSnap != nil {
		previousRoutes = previousSnap.Routes
	} else {
		previousRoutes = map[string]state.RouteSnapshotEntry{}
	}

	allRouteKeys := make(map[string]bool)
	for k := range currentSnap.Routes {
		allRouteKeys[k] = true
	}
	for k := range previousRoutes {
		allRouteKeys[k] = true
	}

	for routeKey := range allRouteKeys {
		curEntry := currentSnap.Routes[routeKey]
		var prevEntry *state.RouteSnapshotEntry
		if p, ok := previousRoutes[routeKey]; ok {
			prevEntry = &p
		}
		routeDelta := subtractSnapshot(&curEntry, prevEntry)

		rows = append(rows, HistoryRow{
			ComponentType:     "route",
			ComponentKey:      routeKey,
			BucketStart:       bucketStart,
			RequestsTotal:     routeDelta.RequestsTotal,
			RequestsSucceeded: routeDelta.RequestsSucceeded,
			RequestsFailed:    routeDelta.RequestsFailed,
			SuccessRate:       routeDelta.SuccessRate,
			AvgLatencyMs:      routeDelta.AvgLatencyMs,
			Status:            classifyComponentStatus(routeDelta.RequestsTotal, routeDelta.SuccessRate),
		})
	}

	return rows
}

func writeHistoryRows(rows []HistoryRow) error {
	if db == nil || len(rows) == 0 {
		return nil
	}

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin metrics tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	stmt, err := tx.Prepare(`
	INSERT INTO status_history (
		component_type, component_key, bucket_start,
		requests_total, requests_succeeded, requests_failed,
		success_rate, avg_latency_ms, status
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(component_type, component_key, bucket_start) DO UPDATE SET
		requests_total = excluded.requests_total,
		requests_succeeded = excluded.requests_succeeded,
		requests_failed = excluded.requests_failed,
		success_rate = excluded.success_rate,
		avg_latency_ms = excluded.avg_latency_ms,
		status = excluded.status
	`)
	if err != nil {
		return fmt.Errorf("prepare metrics insert: %w", err)
	}
	defer stmt.Close()

	for _, row := range rows {
		_, err := stmt.Exec(
			row.ComponentType, row.ComponentKey, row.BucketStart,
			row.RequestsTotal, row.RequestsSucceeded, row.RequestsFailed,
			row.SuccessRate, row.AvgLatencyMs, row.Status,
		)
		if err != nil {
			return fmt.Errorf("insert history row %s/%s@%d: %w", row.ComponentType, row.ComponentKey, row.BucketStart, err)
		}
	}

	// Retention cleanup
	retentionCutoff := time.Now().Unix() - int64(RetentionDays)*86400
	if _, err := tx.Exec("DELETE FROM status_history WHERE bucket_start < ?", retentionCutoff); err != nil {
		return fmt.Errorf("cleanup retention rows: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit metrics tx: %w", err)
	}

	return nil
}

// ─── History Worker ───

func flushHistoryBucket(bucketStart int64) error {
	currentSnap := state.Metrics.CaptureSnapshot()
	previousSnap, previousBucketStart := state.Metrics.GetHistoryState()

	if previousSnap == nil {
		// Fresh start: do not backfill a partial bucket with incomplete data.
		state.Metrics.SetHistoryState(bucketStart, &currentSnap)
		return nil
	}

	expectedBucketStart := previousBucketStart + int64(BucketSeconds)
	if bucketStart != expectedBucketStart {
		log.Printf("Metrics history gap detected, resetting baseline: last_bucket_start=%d current_bucket_start=%d",
			previousBucketStart, bucketStart)
		state.Metrics.SetHistoryState(bucketStart, &currentSnap)
		return nil
	}

	rows := buildHistoryRows(bucketStart, currentSnap, previousSnap)
	if err := writeHistoryRows(rows); err != nil {
		return err
	}

	state.Metrics.SetHistoryState(bucketStart, &currentSnap)
	return nil
}

func StartHistoryWorker() {
	log.Println("📊 Metrics history worker started, bucket_seconds=", BucketSeconds)

	go func() {
		bucketDuration := time.Duration(BucketSeconds) * time.Second
		for {
			now := time.Now()
			nextBucketStart := now.Truncate(bucketDuration).Add(bucketDuration)
			sleepDuration := time.Until(nextBucketStart)
			if sleepDuration < time.Second {
				sleepDuration = time.Second
			}
			time.Sleep(sleepDuration)

			currentUnix := time.Now().Unix()
			bucketToFlush := max0int64(currentUnix/int64(BucketSeconds)*int64(BucketSeconds) - int64(BucketSeconds))
			if err := flushHistoryBucket(bucketToFlush); err != nil {
				log.Println("Metrics history flush error:", err)
			}
		}
	}()
}

// ─── GetStatusHistory (reads status_history table) ───

type StatusHistoryPoint struct {
	BucketStart       int64   `json:"bucket_start"`
	BucketEnd         int64   `json:"bucket_end"`
	Status            string  `json:"status"`
	RequestsTotal     int64   `json:"requests_total"`
	RequestsSucceeded int64   `json:"requests_succeeded"`
	RequestsFailed    int64   `json:"requests_failed"`
	SuccessRate       float64 `json:"success_rate"`
	AvgLatencyMs      float64 `json:"avg_latency_ms"`
}

type StatusHistoryComponent struct {
	ComponentType    string               `json:"component_type"`
	ComponentKey     string               `json:"component_key"`
	DisplayName      string               `json:"display_name"`
	Points           []StatusHistoryPoint `json:"points"`
	UptimePercentage float64              `json:"uptime_percentage"`
	Summary          map[string]int64     `json:"summary"`
}

func GetStatusHistory(hours int) (map[string]interface{}, error) {
	if db == nil {
		return map[string]interface{}{
			"bucket_seconds": BucketSeconds,
			"hours":          hours,
			"generated_at":   time.Now().Unix(),
			"bucket_starts":  []int64{},
			"components":     []StatusHistoryComponent{},
		}, nil
	}

	hours = maxInt(1, minInt(hours, 24*RetentionDays))
	now := time.Now().Unix()
	bucketSpan := int64(hours) * 3600

	latestCompleteBucket := max0int64(now/int64(BucketSeconds)*int64(BucketSeconds) - int64(BucketSeconds))
	startBucket := max0int64(latestCompleteBucket - bucketSpan + int64(BucketSeconds))
	sinceTs := startBucket

	rows, err := db.Query(`
		SELECT
			component_type, component_key, bucket_start,
			requests_total, requests_succeeded, requests_failed,
			success_rate, avg_latency_ms, status
		FROM status_history
		WHERE bucket_start >= ?
		ORDER BY component_type, component_key, bucket_start
	`, sinceTs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Build component map
	components := make(map[string]*StatusHistoryComponent)

	// Default components: gateway + known routes
	defaultGatewayKey := "gateway|gateway"
	components[defaultGatewayKey] = &StatusHistoryComponent{
		ComponentType: "gateway",
		ComponentKey:  "gateway",
		DisplayName:   "Gateway",
		Points:        []StatusHistoryPoint{},
		Summary:       map[string]int64{"requests_total": 0, "requests_succeeded": 0, "requests_failed": 0},
	}

	// Add current routes from metrics without copying large latency sample slices.
	currentSnap := state.Metrics.CaptureSnapshot()
	for routeKey := range currentSnap.Routes {
		compKey := "route|" + routeKey
		components[compKey] = &StatusHistoryComponent{
			ComponentType: "route",
			ComponentKey:  routeKey,
			DisplayName:   routeKey,
			Points:        []StatusHistoryPoint{},
			Summary:       map[string]int64{"requests_total": 0, "requests_succeeded": 0, "requests_failed": 0},
		}
	}

	// Fill from DB rows
	for rows.Next() {
		var componentType, componentKey, status string
		var bucketStart, requestsTotal, requestsSucceeded, requestsFailed int64
		var successRate, avgLatencyMs float64

		if err := rows.Scan(&componentType, &componentKey, &bucketStart, &requestsTotal, &requestsSucceeded, &requestsFailed, &successRate, &avgLatencyMs, &status); err != nil {
			continue
		}

		compKey := componentType + "|" + componentKey
		comp, ok := components[compKey]
		if !ok {
			displayName := componentKey
			if componentType == "gateway" {
				displayName = "Gateway"
			}
			comp = &StatusHistoryComponent{
				ComponentType: componentType,
				ComponentKey:  componentKey,
				DisplayName:   displayName,
				Points:        []StatusHistoryPoint{},
				Summary:       map[string]int64{"requests_total": 0, "requests_succeeded": 0, "requests_failed": 0},
			}
			components[compKey] = comp
		}

		comp.Points = append(comp.Points, StatusHistoryPoint{
			BucketStart:       bucketStart,
			BucketEnd:         bucketStart + int64(BucketSeconds),
			Status:            status,
			RequestsTotal:     requestsTotal,
			RequestsSucceeded: requestsSucceeded,
			RequestsFailed:    requestsFailed,
			SuccessRate:       roundTo2(successRate),
			AvgLatencyMs:      roundTo2(avgLatencyMs),
		})
		comp.Summary["requests_total"] += requestsTotal
		comp.Summary["requests_succeeded"] += requestsSucceeded
		comp.Summary["requests_failed"] += requestsFailed
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Build bucket_starts list and fill gaps
	bucketStarts := []int64{}
	if latestCompleteBucket >= startBucket {
		for bs := startBucket; bs <= latestCompleteBucket; bs += int64(BucketSeconds) {
			bucketStarts = append(bucketStarts, bs)
		}
	}

	// Convert map to sorted list, fill gaps
	historyComponents := []StatusHistoryComponent{}
	var compKeys []string
	for k := range components {
		compKeys = append(compKeys, k)
	}
	sort.Strings(compKeys)

	for _, compKey := range compKeys {
		comp := components[compKey]

		// Build point map for gap filling
		pointMap := make(map[int64]StatusHistoryPoint)
		for _, p := range comp.Points {
			pointMap[p.BucketStart] = p
		}

		// Fill gaps
		filledPoints := make([]StatusHistoryPoint, 0, len(bucketStarts))
		for _, bs := range bucketStarts {
			if p, ok := pointMap[bs]; ok {
				filledPoints = append(filledPoints, p)
			} else {
				filledPoints = append(filledPoints, StatusHistoryPoint{
					BucketStart:       bs,
					BucketEnd:         bs + int64(BucketSeconds),
					Status:            "no_data",
					RequestsTotal:     0,
					RequestsSucceeded: 0,
					RequestsFailed:    0,
					SuccessRate:       0.0,
					AvgLatencyMs:      0.0,
				})
			}
		}

		comp.Points = filledPoints
		total := comp.Summary["requests_total"]
		if total > 0 {
			comp.UptimePercentage = roundTo2(float64(comp.Summary["requests_succeeded"]) / float64(total) * 100)
		} else {
			comp.UptimePercentage = 0.0
		}
		historyComponents = append(historyComponents, *comp)
	}

	return map[string]interface{}{
		"bucket_seconds": BucketSeconds,
		"hours":          hours,
		"generated_at":   now,
		"bucket_starts":  bucketStarts,
		"components":     historyComponents,
	}, nil
}

// ─── Utility ───

func roundTo2(f float64) float64 {
	return float64(int64(f*100+0.5)) / 100
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max0int64(n int64) int64 {
	if n < 0 {
		return 0
	}
	return n
}

func max0float64(f float64) float64 {
	if f < 0 {
		return 0
	}
	return f
}
