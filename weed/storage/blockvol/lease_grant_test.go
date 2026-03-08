package blockvol

import (
	"errors"
	"testing"
	"time"
)

// TestLeaseGrant tests the explicit lease grant mechanism.
func TestLeaseGrant(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "keepalive_longevity", run: testLeaseKeepaliveLongevity},
		{name: "heartbeat_loss_lease_expires", run: testHeartbeatLossLeaseExpires},
		{name: "old_primary_cannot_renew_after_promotion", run: testOldPrimaryCannotRenew},
		{name: "stale_epoch_grant_rejected", run: testStaleEpochGrantRejected},
	}
	for _, tt := range tests {
		t.Run(tt.name, tt.run)
	}
}

// Test 1: Lease keepalive longevity — writes continue past TTL with healthy heartbeat.
func testLeaseKeepaliveLongevity(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	// Assign as primary with short 200ms lease.
	if err := v.HandleAssignment(1, RolePrimary, 200*time.Millisecond); err != nil {
		t.Fatalf("HandleAssignment: %v", err)
	}

	// Write should succeed immediately.
	data := make([]byte, v.Info().BlockSize)
	data[0] = 0xAA
	if err := v.WriteLBA(0, data); err != nil {
		t.Fatalf("write before TTL: %v", err)
	}

	// Simulate periodic lease grants (like master heartbeat responses).
	// Grant every 100ms for 500ms total — well past the 200ms TTL.
	// Uses HandleAssignment (the real production path) instead of a direct
	// lease.Grant() — same epoch + same role = lease refresh.
	for i := 0; i < 5; i++ {
		time.Sleep(100 * time.Millisecond)
		// Lease grant via HandleAssignment same-role refresh path.
		if err := v.HandleAssignment(1, RolePrimary, 200*time.Millisecond); err != nil {
			t.Fatalf("lease grant at iteration %d: %v", i, err)
		}

		// Write should still succeed because lease was renewed.
		data[0] = byte(i + 1)
		if err := v.WriteLBA(0, data); err != nil {
			t.Fatalf("write at iteration %d (t=%dms): %v", i, (i+1)*100, err)
		}
	}

	// Final verification: we wrote past 500ms with a 200ms TTL lease.
	if !v.lease.IsValid() {
		t.Error("lease should still be valid after continuous renewal")
	}
}

// Test 2: Heartbeat loss — writes fail after TTL expiry.
func testHeartbeatLossLeaseExpires(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	// Assign as primary with short 100ms lease.
	if err := v.HandleAssignment(1, RolePrimary, 100*time.Millisecond); err != nil {
		t.Fatalf("HandleAssignment: %v", err)
	}

	// Write should succeed immediately.
	data := make([]byte, v.Info().BlockSize)
	if err := v.WriteLBA(0, data); err != nil {
		t.Fatalf("write before expiry: %v", err)
	}

	// Do NOT renew the lease — simulate heartbeat loss.
	time.Sleep(150 * time.Millisecond)

	// Write should fail with ErrLeaseExpired.
	err := v.WriteLBA(0, data)
	if err == nil {
		t.Fatal("expected write to fail after lease expiry, got nil")
	}
	if !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("expected ErrLeaseExpired, got: %v", err)
	}
}

// Test 3: Old primary cannot keep renewing after promotion elsewhere.
// After demotion, lease grants with old epoch must not revive writes.
func testOldPrimaryCannotRenew(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	// Start as primary at epoch 1.
	if err := v.HandleAssignment(1, RolePrimary, 30*time.Second); err != nil {
		t.Fatalf("HandleAssignment: %v", err)
	}

	data := make([]byte, v.Info().BlockSize)
	if err := v.WriteLBA(0, data); err != nil {
		t.Fatalf("write as primary: %v", err)
	}

	// Demote: master sends Stale assignment with epoch 2.
	if err := v.HandleAssignment(2, RoleStale, 0); err != nil {
		t.Fatalf("demote: %v", err)
	}

	// Write must fail — no longer primary.
	writeErr := v.WriteLBA(0, data)
	if writeErr == nil {
		t.Fatal("expected write to fail after demotion, got nil")
	}
	if !errors.Is(writeErr, ErrNotPrimary) {
		t.Fatalf("expected ErrNotPrimary, got: %v", writeErr)
	}

	// Old primary tries to re-assign as Primary with stale epoch.
	// After demotion to Stale, Stale->Primary is an invalid transition
	// (must go through rebuild). Even if it succeeded, writeGate checks role.
	err := v.HandleAssignment(1, RolePrimary, 30*time.Second)
	if err == nil {
		t.Fatal("expected error for Stale->Primary transition, got nil")
	}
}

// Test 4: Mixed epochs — renewal for stale epoch is rejected by HandleAssignment.
func testStaleEpochGrantRejected(t *testing.T) {
	v := createTestVol(t)
	defer v.Close()

	// Primary at epoch 5.
	if err := v.HandleAssignment(5, RolePrimary, 30*time.Second); err != nil {
		t.Fatalf("HandleAssignment: %v", err)
	}

	// Lease grant with epoch 3 (stale) via HandleAssignment — must be rejected.
	err := v.HandleAssignment(3, RolePrimary, 30*time.Second)
	if err == nil {
		t.Fatal("expected error for stale epoch, got nil")
	}
	if !errors.Is(err, ErrEpochRegression) {
		t.Fatalf("expected ErrEpochRegression, got: %v", err)
	}

	// Epoch should remain 5.
	if v.Epoch() != 5 {
		t.Errorf("epoch should remain 5, got %d", v.Epoch())
	}

	// Lease grant with matching epoch 5 should succeed.
	if err := v.HandleAssignment(5, RolePrimary, 30*time.Second); err != nil {
		t.Fatalf("same-epoch refresh should succeed: %v", err)
	}

	// Lease grant with epoch 6 (bump) should also succeed.
	if err := v.HandleAssignment(6, RolePrimary, 30*time.Second); err != nil {
		t.Fatalf("epoch bump should succeed: %v", err)
	}
	if v.Epoch() != 6 {
		t.Errorf("epoch should be 6 after bump, got %d", v.Epoch())
	}
}
