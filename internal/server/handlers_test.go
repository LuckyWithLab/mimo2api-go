package server

import "testing"

func TestBodyFingerprintIncludesLength(t *testing.T) {
	got := bodyFingerprint("hello")
	if got == "" {
		t.Fatal("expected fingerprint string")
	}
	if got[:4] != "len=" {
		t.Fatalf("expected length prefix, got %q", got)
	}
}
