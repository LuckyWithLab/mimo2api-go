package manager

import (
	"testing"
	"time"

	"mimo2api/internal/models"
)

func TestWaitForSignalSeesPendingRebuildImmediately(t *testing.T) {
	m := &AccountManager{
		Users:          make(map[string]models.UserRecord),
		LifecycleStops: make(map[string]chan struct{}),
		rebuildCh:      make(chan struct{}),
	}
	stopCh := make(chan struct{})

	m.TriggerRebuild()

	signal, version := m.waitForSignal(stopCh, 0, time.Second)
	if signal != waitSignalRebuild {
		t.Fatalf("expected rebuild signal, got %v", signal)
	}
	if version != 1 {
		t.Fatalf("expected rebuild version 1, got %d", version)
	}
}

func TestWaitForSignalReturnsStopForClosedLifecycle(t *testing.T) {
	m := &AccountManager{
		Users:          make(map[string]models.UserRecord),
		LifecycleStops: make(map[string]chan struct{}),
		rebuildCh:      make(chan struct{}),
	}
	stopCh := make(chan struct{})
	close(stopCh)

	signal, _ := m.waitForSignal(stopCh, 0, time.Second)
	if signal != waitSignalStop {
		t.Fatalf("expected stop signal, got %v", signal)
	}
}

func TestWaitForSignalWakesOnBroadcastRebuild(t *testing.T) {
	m := &AccountManager{
		Users:          make(map[string]models.UserRecord),
		LifecycleStops: make(map[string]chan struct{}),
		rebuildCh:      make(chan struct{}),
	}
	stopCh := make(chan struct{})

	go func() {
		time.Sleep(20 * time.Millisecond)
		m.TriggerRebuild()
	}()

	signal, version := m.waitForSignal(stopCh, 0, time.Second)
	if signal != waitSignalRebuild {
		t.Fatalf("expected rebuild signal, got %v", signal)
	}
	if version != 1 {
		t.Fatalf("expected rebuild version 1, got %d", version)
	}
}

func TestShouldReuseHealthyInstance(t *testing.T) {
	if !shouldReuseHealthyInstance(false, false, 0, "AVAILABLE", 600) {
		t.Fatal("expected healthy available instance to be reused")
	}
	if shouldReuseHealthyInstance(false, true, 0, "AVAILABLE", 600) {
		t.Fatal("expected stagger rotation due to force rebuild instead of reuse")
	}
	if shouldReuseHealthyInstance(false, false, 1, "AVAILABLE", 600) {
		t.Fatal("expected pending rebuild version to skip healthy reuse")
	}
	if shouldReuseHealthyInstance(true, false, 0, "AVAILABLE", 600) {
		t.Fatal("expected bootstrap-pending instance to skip healthy reuse")
	}
	if shouldReuseHealthyInstance(false, false, 0, "AVAILABLE", 180) {
		t.Fatal("expected near-expiry instance to skip healthy reuse")
	}
}
