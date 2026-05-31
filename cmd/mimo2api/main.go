package main

import (
	"fmt"
	"log"
	"time"

	"mimo2api/internal/config"
	"mimo2api/internal/manager"
	"mimo2api/internal/metrics"
	"mimo2api/internal/state"
	"mimo2api/internal/server"
)

func main() {
	log.Println("Starting mimo2api (Go Version)...")

	// Load configuration
	config.Load()

	// Restore cumulative metrics from snapshot
	if state.Metrics.LoadSnapshot() {
		log.Println("📊 已从快照恢复累积指标")
	}

	// Initialize Database
	if err := metrics.InitDB(config.MetricsDBPath); err != nil {
		log.Fatalf("Failed to initialize metrics DB: %v", err)
	}

	// Start metrics history worker
	metrics.StartHistoryWorker()

	// Start periodic metrics snapshot save (every 60 seconds)
	go func() {
		for {
			time.Sleep(60 * time.Second)
			state.Metrics.SaveSnapshot()
		}
	}()

	// Setup Server
	r := server.SetupRouter()

	// Load users from directory
	manager.GlobalManager.LoadUsersFromDir("users")

	addr := fmt.Sprintf("%s:%d", config.ServerHost, config.ServerPort)
	log.Printf("Listening and serving HTTP on %s", addr)

	if err := r.Run(addr); err != nil {
		// Save snapshot before exiting
		state.Metrics.SaveSnapshot()
		log.Fatalf("Server failed: %v", err)
	}
}