package manager

import (
	"fmt"
	"testing"
	"time"

	"mimo2api/internal/models"
)

func newTestManager() *AccountManager {
	return &AccountManager{
		Users:          make(map[string]models.UserRecord),
		LifecycleStops: make(map[string]chan struct{}),
		rebuildCh:      make(chan struct{}),
	}
}

func TestWaitForSignalSeesPendingRebuildImmediately(t *testing.T) {
	m := newTestManager()
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
	m := newTestManager()
	stopCh := make(chan struct{})
	close(stopCh)

	signal, _ := m.waitForSignal(stopCh, 0, time.Second)
	if signal != waitSignalStop {
		t.Fatalf("expected stop signal, got %v", signal)
	}
}

func TestWaitForSignalWakesOnBroadcastRebuild(t *testing.T) {
	m := newTestManager()
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

func TestReserveLifecycleSlotsCapsActiveAccountsAtFour(t *testing.T) {
	m := newTestManager()
	for i := 0; i < 6; i++ {
		userID := fmt.Sprintf("user-%d", i)
		m.Users[userID] = models.UserRecord{UserID: userID, Status: "QUEUED"}
		m.UserOrder = append(m.UserOrder, userID)
	}

	launches := m.reserveLifecycleSlots()
	if len(launches) != maxActiveLifecycleSlots {
		t.Fatalf("expected %d launches, got %d", maxActiveLifecycleSlots, len(launches))
	}
	if len(m.LifecycleStops) != maxActiveLifecycleSlots {
		t.Fatalf("expected %d active slots, got %d", maxActiveLifecycleSlots, len(m.LifecycleStops))
	}
	for i, launch := range launches {
		want := fmt.Sprintf("user-%d", i)
		if launch.user.UserID != want {
			t.Fatalf("launch %d: expected %s, got %s", i, want, launch.user.UserID)
		}
		if got := m.Users[want].Status; got != "SCHEDULED" {
			t.Fatalf("expected %s to be SCHEDULED, got %s", want, got)
		}
	}

	if extra := m.reserveLifecycleSlots(); len(extra) != 0 {
		t.Fatalf("expected no extra launches while slots are full, got %d", len(extra))
	}
}

func TestReserveLifecycleSlotsRoundRobinsAfterSlotFreed(t *testing.T) {
	m := newTestManager()
	for i := 0; i < 6; i++ {
		userID := fmt.Sprintf("user-%d", i)
		m.Users[userID] = models.UserRecord{UserID: userID, Status: "QUEUED"}
		m.UserOrder = append(m.UserOrder, userID)
	}

	_ = m.reserveLifecycleSlots()
	delete(m.LifecycleStops, "user-1")

	launches := m.reserveLifecycleSlots()
	if len(launches) != 1 {
		t.Fatalf("expected one launch after freeing a slot, got %d", len(launches))
	}
	if got := launches[0].user.UserID; got != "user-4" {
		t.Fatalf("expected round-robin to launch user-4, got %s", got)
	}
}
