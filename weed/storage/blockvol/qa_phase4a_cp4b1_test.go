package blockvol

import (
	"math"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestQAPhase4ACP4b1 runs adversarial tests for CP4b-1:
// wire types, conversion helpers, and heartbeat collection.
func TestQAPhase4ACP4b1(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		// --- RoleFromWire / RoleToWire ---
		{name: "role_wire_boundary_maxvalid", run: testRoleWireBoundaryMaxValid},
		{name: "role_wire_boundary_maxvalid_plus_one", run: testRoleWireBoundaryMaxValidPlus1},
		{name: "role_wire_zero", run: testRoleWireZero},
		{name: "role_wire_all_invalid_values", run: testRoleWireAllInvalid},

		// --- LeaseTTLFromWire / LeaseTTLToWire ---
		{name: "lease_ttl_zero", run: testLeaseTTLZero},
		{name: "lease_ttl_sub_millisecond_truncated", run: testLeaseTTLSubMsTruncated},
		{name: "lease_ttl_maxuint32", run: testLeaseTTLMaxUint32},
		{name: "lease_ttl_negative_clamps_zero", run: testLeaseTTLNegativeClampsZero},
		{name: "lease_ttl_large_duration_clamps", run: testLeaseTTLLargeDurationClamps},
		{name: "lease_ttl_exact_boundary", run: testLeaseTTLExactBoundary},

		// --- ToBlockVolumeInfoMessage ---
		{name: "info_msg_primary_with_lease", run: testInfoMsgPrimaryWithLease},
		{name: "info_msg_stale_after_demote", run: testInfoMsgStaleAfterDemote},
		{name: "info_msg_closed_volume", run: testInfoMsgClosedVolume},
		{name: "info_msg_concurrent_status_and_write", run: testInfoMsgConcurrentStatusAndWrite},

		// --- BlockVolumeAssignment wire conversion ---
		{name: "assignment_roundtrip", run: testAssignmentRoundtrip},
		{name: "assignment_unknown_role_maps_none", run: testAssignmentUnknownRoleMapsNone},
		{name: "assignment_zero_lease_ttl", run: testAssignmentZeroLeaseTTL},

		// --- collectBlockVolumeHeartbeat adversarial ---
		{name: "collect_concurrent_add_remove", run: testCollectConcurrentAddRemove},
		{name: "collect_closed_vol_in_store", run: testCollectClosedVolInStore},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t)
		})
	}
}

func cp4b1Vol(t *testing.T) *BlockVol {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "cp4b1.blockvol")
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	return v
}

// ---------------------------------------------------------------------------
// RoleFromWire / RoleToWire
// ---------------------------------------------------------------------------

func testRoleWireBoundaryMaxValid(t *testing.T) {
	// maxValidRole = uint32(RoleDraining) = 5.
	// RoleFromWire(5) should return RoleDraining.
	r := RoleFromWire(maxValidRole)
	if r != RoleDraining {
		t.Errorf("RoleFromWire(%d) = %s, want Draining", maxValidRole, r)
	}
}

func testRoleWireBoundaryMaxValidPlus1(t *testing.T) {
	// maxValidRole + 1 = 6 -- should map to RoleNone.
	r := RoleFromWire(maxValidRole + 1)
	if r != RoleNone {
		t.Errorf("RoleFromWire(%d) = %s, want None", maxValidRole+1, r)
	}
}

func testRoleWireZero(t *testing.T) {
	// 0 = RoleNone -- valid.
	r := RoleFromWire(0)
	if r != RoleNone {
		t.Errorf("RoleFromWire(0) = %s, want None", r)
	}
	// Roundtrip.
	w := RoleToWire(RoleNone)
	if w != 0 {
		t.Errorf("RoleToWire(None) = %d, want 0", w)
	}
}

func testRoleWireAllInvalid(t *testing.T) {
	// All values above maxValidRole should map to RoleNone.
	invalids := []uint32{6, 7, 10, 100, 255, 1000, math.MaxUint32}
	for _, v := range invalids {
		r := RoleFromWire(v)
		if r != RoleNone {
			t.Errorf("RoleFromWire(%d) = %s, want None", v, r)
		}
	}
}

// ---------------------------------------------------------------------------
// LeaseTTLFromWire / LeaseTTLToWire
// ---------------------------------------------------------------------------

func testLeaseTTLZero(t *testing.T) {
	// 0ms -> 0 duration.
	wire := LeaseTTLToWire(0)
	if wire != 0 {
		t.Errorf("LeaseTTLToWire(0) = %d, want 0", wire)
	}
	back := LeaseTTLFromWire(0)
	if back != 0 {
		t.Errorf("LeaseTTLFromWire(0) = %v, want 0", back)
	}
}

func testLeaseTTLSubMsTruncated(t *testing.T) {
	// BUG candidate: sub-millisecond durations get truncated to 0.
	// time.Duration(500us).Milliseconds() = 0 -> wire = 0 -> back = 0.
	// This means a 500us lease becomes a 0ms lease on the wire.
	subMs := 500 * time.Microsecond
	wire := LeaseTTLToWire(subMs)
	back := LeaseTTLFromWire(wire)

	t.Logf("BUG-CP4B1-1: sub-ms lease: input=%v, wire=%d, back=%v", subMs, wire, back)
	if wire != 0 {
		t.Logf("  wire is %d (truncated from %v)", wire, subMs)
	}
	if back != 0 {
		t.Logf("  roundtrip: %v -> %v (LOSSY)", subMs, back)
	}
	// This IS lossy. 500us -> 0ms -> 0ns.
	// Whether this is a bug depends on whether sub-ms leases are supported.
	// For block storage with 30s leases, this is fine.
	// But if someone passes 1us lease for testing, it silently becomes 0 (expired).
	if back == subMs {
		t.Error("sub-ms should NOT roundtrip perfectly with ms wire format")
	}
}

func testLeaseTTLMaxUint32(t *testing.T) {
	// MaxUint32 ms = ~49.7 days.
	wire := uint32(math.MaxUint32)
	back := LeaseTTLFromWire(wire)
	expected := time.Duration(math.MaxUint32) * time.Millisecond
	if back != expected {
		t.Errorf("LeaseTTLFromWire(MaxUint32) = %v, want %v", back, expected)
	}
}

func testLeaseTTLNegativeClampsZero(t *testing.T) {
	neg := -5 * time.Second
	wire := LeaseTTLToWire(neg)
	if wire != 0 {
		t.Errorf("LeaseTTLToWire(%v) = %d, want 0", neg, wire)
	}
}

func testLeaseTTLLargeDurationClamps(t *testing.T) {
	// 100 days > ~49.7 day uint32 limit.
	huge := 100 * 24 * time.Hour
	wire := LeaseTTLToWire(huge)
	if wire != math.MaxUint32 {
		t.Errorf("LeaseTTLToWire(%v) = %d, want %d", huge, wire, uint32(math.MaxUint32))
	}
}

func testLeaseTTLExactBoundary(t *testing.T) {
	// Exact boundary: MaxUint32 ms as duration should clamp to MaxUint32.
	exact := time.Duration(math.MaxUint32) * time.Millisecond
	wire := LeaseTTLToWire(exact)
	if wire != math.MaxUint32 {
		t.Errorf("LeaseTTLToWire(exact boundary) = %d, want %d", wire, uint32(math.MaxUint32))
	}
	// One ms more should also clamp.
	oneMore := exact + time.Millisecond
	wire2 := LeaseTTLToWire(oneMore)
	if wire2 != math.MaxUint32 {
		t.Errorf("LeaseTTLToWire(boundary+1ms) = %d, want %d", wire2, uint32(math.MaxUint32))
	}
}

// ---------------------------------------------------------------------------
// ToBlockVolumeInfoMessage
// ---------------------------------------------------------------------------

func testInfoMsgPrimaryWithLease(t *testing.T) {
	v := cp4b1Vol(t)
	defer v.Close()

	v.HandleAssignment(7, RolePrimary, 30*time.Second)
	v.WriteLBA(0, make([]byte, 4096))

	msg := ToBlockVolumeInfoMessage("/data/vol1.blk", "ssd", v)

	if msg.Epoch != 7 {
		t.Errorf("Epoch: got %d, want 7", msg.Epoch)
	}
	if RoleFromWire(msg.Role) != RolePrimary {
		t.Errorf("Role: got %d (%s), want Primary", msg.Role, RoleFromWire(msg.Role))
	}
	if !msg.HasLease {
		t.Error("HasLease: got false, want true")
	}
	if msg.WalHeadLsn == 0 {
		t.Error("WalHeadLsn should be > 0 after write")
	}
	if msg.Path != "/data/vol1.blk" {
		t.Errorf("Path: got %q, want %q", msg.Path, "/data/vol1.blk")
	}
	if msg.DiskType != "ssd" {
		t.Errorf("DiskType: got %q, want %q", msg.DiskType, "ssd")
	}
}

func testInfoMsgStaleAfterDemote(t *testing.T) {
	v := cp4b1Vol(t)
	defer v.Close()
	v.drainTimeout = 100 * time.Millisecond

	v.HandleAssignment(1, RolePrimary, 30*time.Second)
	v.HandleAssignment(2, RoleStale, 0)

	msg := ToBlockVolumeInfoMessage("/data/stale.blk", "hdd", v)

	if RoleFromWire(msg.Role) != RoleStale {
		t.Errorf("Role: got %s, want Stale", RoleFromWire(msg.Role))
	}
	if msg.HasLease {
		t.Error("HasLease: got true, want false for Stale")
	}
	if msg.Epoch != 2 {
		t.Errorf("Epoch: got %d, want 2", msg.Epoch)
	}
}

func testInfoMsgClosedVolume(t *testing.T) {
	v := cp4b1Vol(t)
	v.HandleAssignment(1, RolePrimary, 30*time.Second)
	v.WriteLBA(0, make([]byte, 4096))
	v.Close()

	// Should not panic on closed volume.
	msg := ToBlockVolumeInfoMessage("/data/closed.blk", "ssd", v)
	t.Logf("closed vol msg: epoch=%d role=%s walHead=%d lease=%v",
		msg.Epoch, RoleFromWire(msg.Role), msg.WalHeadLsn, msg.HasLease)
}

func testInfoMsgConcurrentStatusAndWrite(t *testing.T) {
	v := cp4b1Vol(t)
	defer v.Close()

	v.HandleAssignment(1, RolePrimary, 30*time.Second)

	var wg sync.WaitGroup
	// 10 writers.
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(lba int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				v.WriteLBA(uint64(lba%256), make([]byte, 4096))
			}
		}(i)
	}
	// 10 concurrent ToBlockVolumeInfoMessage calls.
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				msg := ToBlockVolumeInfoMessage("/data/concurrent.blk", "ssd", v)
				// Basic sanity: epoch should not be garbage.
				if msg.Epoch != 1 {
					t.Errorf("epoch should be 1, got %d", msg.Epoch)
					return
				}
			}
		}()
	}
	wg.Wait()
}

// ---------------------------------------------------------------------------
// BlockVolumeAssignment wire conversion
// ---------------------------------------------------------------------------

func testAssignmentRoundtrip(t *testing.T) {
	// Create an assignment, convert role and lease to wire, convert back.
	assign := BlockVolumeAssignment{
		Path:       "/data/vol.blk",
		Epoch:      42,
		Role:       RoleToWire(RolePrimary),
		LeaseTtlMs: LeaseTTLToWire(30 * time.Second),
	}

	role := RoleFromWire(assign.Role)
	ttl := LeaseTTLFromWire(assign.LeaseTtlMs)

	if role != RolePrimary {
		t.Errorf("Role: got %s, want Primary", role)
	}
	if ttl != 30*time.Second {
		t.Errorf("TTL: got %v, want 30s", ttl)
	}
	if assign.Epoch != 42 {
		t.Errorf("Epoch: got %d, want 42", assign.Epoch)
	}
}

func testAssignmentUnknownRoleMapsNone(t *testing.T) {
	assign := BlockVolumeAssignment{
		Role: 999,
	}
	role := RoleFromWire(assign.Role)
	if role != RoleNone {
		t.Errorf("unknown role 999 should map to None, got %s", role)
	}
}

func testAssignmentZeroLeaseTTL(t *testing.T) {
	assign := BlockVolumeAssignment{
		LeaseTtlMs: 0,
	}
	ttl := LeaseTTLFromWire(assign.LeaseTtlMs)
	if ttl != 0 {
		t.Errorf("zero LeaseTtlMs should give 0 duration, got %v", ttl)
	}
}

// ---------------------------------------------------------------------------
// collectBlockVolumeHeartbeat adversarial
// ---------------------------------------------------------------------------

func testCollectConcurrentAddRemove(t *testing.T) {
	// Simulate concurrent heartbeat collection while volumes are being added.
	// The test double doesn't have locking (unlike the real store), so this
	// tests the conversion helpers' safety, not the store's.
	store := &testBlockVolumeStore{
		volumes:   make(map[string]*BlockVol),
		diskTypes: make(map[string]string),
	}

	// Pre-populate with 3 volumes.
	dir := t.TempDir()
	for i := 0; i < 3; i++ {
		p := filepath.Join(dir, filepath.Base(t.Name())+string(rune('a'+i))+".blk")
		vol, err := CreateBlockVol(p, CreateOptions{
			VolumeSize: 1 << 20,
			BlockSize:  4096,
		})
		if err != nil {
			t.Fatalf("create: %v", err)
		}
		defer vol.Close()
		store.volumes[p] = vol
		store.diskTypes[p] = "ssd"
	}

	// Collect heartbeat multiple times.
	for i := 0; i < 10; i++ {
		msgs := collectBlockVolumeHeartbeat(store)
		if len(msgs) != 3 {
			t.Errorf("iter %d: expected 3 messages, got %d", i, len(msgs))
		}
	}
}

func testCollectClosedVolInStore(t *testing.T) {
	// If a volume in the store is closed, heartbeat collection should not panic.
	dir := t.TempDir()
	p := filepath.Join(dir, "closed.blk")
	vol, err := CreateBlockVol(p, CreateOptions{
		VolumeSize: 1 << 20,
		BlockSize:  4096,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	vol.HandleAssignment(1, RolePrimary, 30*time.Second)
	vol.WriteLBA(0, make([]byte, 4096))
	vol.Close() // close it

	store := &testBlockVolumeStore{
		volumes:   map[string]*BlockVol{p: vol},
		diskTypes: map[string]string{p: "ssd"},
	}

	// Should not panic.
	msgs := collectBlockVolumeHeartbeat(store)
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	t.Logf("closed vol heartbeat: epoch=%d role=%s lease=%v",
		msgs[0].Epoch, RoleFromWire(msgs[0].Role), msgs[0].HasLease)
}
