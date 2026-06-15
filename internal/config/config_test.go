package config

import "testing"

func TestLoadNodeCooldownConfigAllowsZeroAndCustomDurations(t *testing.T) {
	oldFailureCooldown := NodeFailureCooldown
	oldTimeoutCooldown := NodeTimeoutCooldown
	t.Cleanup(func() {
		NodeFailureCooldown = oldFailureCooldown
		NodeTimeoutCooldown = oldTimeoutCooldown
	})

	t.Setenv("MIMO_NODE_FAILURE_COOLDOWN_SECONDS", "0")
	t.Setenv("MIMO_NODE_TIMEOUT_COOLDOWN_SECONDS", "45")

	Load()

	if NodeFailureCooldown != 0 {
		t.Fatalf("expected failure cooldown to be disabled with 0, got %d", NodeFailureCooldown)
	}
	if NodeTimeoutCooldown != 45 {
		t.Fatalf("expected timeout cooldown 45, got %d", NodeTimeoutCooldown)
	}
}

func TestLoadNodeCooldownConfigClampsNegativeToZero(t *testing.T) {
	oldFailureCooldown := NodeFailureCooldown
	oldTimeoutCooldown := NodeTimeoutCooldown
	t.Cleanup(func() {
		NodeFailureCooldown = oldFailureCooldown
		NodeTimeoutCooldown = oldTimeoutCooldown
	})

	t.Setenv("MIMO_NODE_FAILURE_COOLDOWN_SECONDS", "-1")
	t.Setenv("MIMO_NODE_TIMEOUT_COOLDOWN_SECONDS", "-2")

	Load()

	if NodeFailureCooldown != 0 {
		t.Fatalf("expected negative failure cooldown to clamp to 0, got %d", NodeFailureCooldown)
	}
	if NodeTimeoutCooldown != 0 {
		t.Fatalf("expected negative timeout cooldown to clamp to 0, got %d", NodeTimeoutCooldown)
	}
}
