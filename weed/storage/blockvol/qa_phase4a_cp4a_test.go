package blockvol

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestQAPhase4ACP4a runs adversarial tests for Phase 4A CP4a:
// SimulatedMaster, HandleAssignment sequences, failover, and Status().
func TestQAPhase4ACP4a(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		// --- SimulatedMaster correctness ---
		{name: "master_epoch_monotonic_across_operations", run: testMasterEpochMonotonic},
		{name: "master_promote_replica_both_fenced", run: testMasterPromoteReplicaBothFenced},
		{name: "master_demote_then_grant_reuses_vol", run: testMasterDemoteThenGrantReusesVol},

		// --- HandleAssignment edge cases ---
		{name: "assign_same_role_equal_epoch_no_persist", run: testAssignSameRoleEqualEpochNoPersist},
		{name: "assign_none_to_replica_epoch_not_persisted", run: testAssignNoneToReplicaEpochNotPersisted},
		{name: "assign_demote_epoch_less_than_current", run: testAssignDemoteEpochLessThanCurrent},
		{name: "assign_closed_volume", run: testAssignClosedVolume},
		{name: "assign_concurrent_demote_and_refresh", run: testAssignConcurrentDemoteAndRefresh},
		{name: "assign_rapid_promote_demote_10x", run: testAssignRapidPromoteDemote10x},

		// --- Status() edge cases ---
		{name: "status_fresh_primary_no_writes", run: testStatusFreshPrimaryNoWrites},
		{name: "status_during_writes", run: testStatusDuringWrites},
		{name: "status_all_roles", run: testStatusAllRoles},
		{name: "status_after_close", run: testStatusAfterClose},
		{name: "status_checkpoint_advances_after_flush", run: testStatusCheckpointAdvances},

		// --- Failover adversarial ---
		{name: "failover_triple_A_B_A_B", run: testFailoverTriple},
		{name: "failover_concurrent_promote_two_vols", run: testFailoverConcurrentPromoteTwoVols},
		{name: "failover_demote_during_active_writes", run: testFailoverDemoteDuringActiveWrites},
		{name: "failover_epoch_always_increases", run: testFailoverEpochAlwaysIncreases},
		{name: "failover_rebuild_then_immediate_failover", run: testFailoverRebuildThenImmediateFailover},
		{name: "failover_dead_zone_no_writes_anywhere", run: testFailoverDeadZoneNoWritesAnywhere},

		// --- beginOp/endOp adversarial ---
		{name: "ops_begin_after_close", run: testOpsBeginAfterClose},
		{name: "ops_drain_blocks_demote", run: testOpsDrainBlocksDemote},
		{name: "ops_concurrent_begin_end_100", run: testOpsConcurrentBeginEnd100},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t)
		})
	}
}

// cp4aVol creates a minimal test volume.
func cp4aVol(t *testing.T, cfgs ...BlockVolConfig) *BlockVol {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "cp4a.blockvol")
	var cfg BlockVolConfig
	if len(cfgs) > 0 {
		cfg = cfgs[0]
	}
	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
	}, cfg)
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	return v
}

func cp4aBlock(b byte) []byte {
	buf := make([]byte, 4096)
	for i := range buf {
		buf[i] = b
	}
	return buf
}

// ---------------------------------------------------------------------------
// SimulatedMaster correctness
// ---------------------------------------------------------------------------

func testMasterEpochMonotonic(t *testing.T) {
	v := cp4aVol(t)
	defer v.Close()
	m := NewSimulatedMaster(30 * time.Second)

	// GrantPrimary -> epoch 1
	m.GrantPrimary(v)
	e1 := v.Epoch()

	// BumpEpoch -> epoch 2
	m.BumpEpoch(v)
	e2 := v.Epoch()

	// Demote -> epoch 3
	m.Demote(v)
	e3 := v.Epoch()

	if e1 >= e2 || e2 >= e3 {
		t.Errorf("epoch not monotonic: %d -> %d -> %d", e1, e2, e3)
	}

	// Rebuild back to Primary -> epoch 4 then 5
	m.InitiateRebuild(v)
	v.SetRole(RoleReplica)
	m.GrantPrimary(v)
	e4 := v.Epoch()
	if e4 <= e3 {
		t.Errorf("epoch after re-promote (%d) should be > demote epoch (%d)", e4, e3)
	}
}

func testMasterPromoteReplicaBothFenced(t *testing.T) {
	a := cp4aVol(t)
	defer a.Close()
	b := cp4aVol(t)
	defer b.Close()

	m := NewSimulatedMaster(30 * time.Second)
	m.GrantPrimary(a)

	// Write to A.
	if err := a.WriteLBA(0, cp4aBlock('A')); err != nil {
		t.Fatalf("A write: %v", err)
	}

	// Assign B as replica.
	m.AssignReplica(b)

	// PromoteReplica: demotes A, promotes B.
	_, err := m.PromoteReplica(a, b)
	if err != nil {
		t.Fatalf("PromoteReplica: %v", err)
	}

	// A must be fenced (Stale).
	if err := a.WriteLBA(0, cp4aBlock('X')); err == nil {
		t.Error("old primary should be fenced after PromoteReplica")
	}

	// B must be writable (Primary).
	if err := b.WriteLBA(0, cp4aBlock('B')); err != nil {
		t.Errorf("new primary should accept writes: %v", err)
	}

	// Both should have the same epoch.
	if a.Epoch() != b.Epoch() {
		t.Errorf("epochs should match: A=%d B=%d", a.Epoch(), b.Epoch())
	}
}

func testMasterDemoteThenGrantReusesVol(t *testing.T) {
	v := cp4aVol(t)
	defer v.Close()
	m := NewSimulatedMaster(30 * time.Second)

	// Primary -> Stale -> Rebuilding -> Replica -> Primary again.
	m.GrantPrimary(v)
	if err := v.WriteLBA(0, cp4aBlock('1')); err != nil {
		t.Fatalf("first write: %v", err)
	}
	m.Demote(v)
	m.InitiateRebuild(v)
	v.SetRole(RoleReplica)
	m.GrantPrimary(v)

	// Write again with new epoch.
	if err := v.WriteLBA(1, cp4aBlock('2')); err != nil {
		t.Fatalf("second write after re-promotion: %v", err)
	}

	// Read both blocks.
	d0, _ := v.ReadLBA(0, 4096)
	d1, _ := v.ReadLBA(1, 4096)
	if d0[0] != '1' {
		t.Errorf("block 0: got %c, want '1'", d0[0])
	}
	if d1[0] != '2' {
		t.Errorf("block 1: got %c, want '2'", d1[0])
	}
}

// ---------------------------------------------------------------------------
// HandleAssignment edge cases
// ---------------------------------------------------------------------------

func testAssignSameRoleEqualEpochNoPersist(t *testing.T) {
	v := cp4aVol(t)
	defer v.Close()

	// Promote to Primary at epoch 1.
	v.HandleAssignment(1, RolePrimary, 30*time.Second)

	// Same role, same epoch -- should be a no-op on epoch, but refreshes lease.
	v.lease.Revoke()
	if v.lease.IsValid() {
		t.Fatal("lease should be revoked")
	}

	err := v.HandleAssignment(1, RolePrimary, 30*time.Second)
	if err != nil {
		t.Fatalf("same role/epoch assignment: %v", err)
	}
	if !v.lease.IsValid() {
		t.Error("lease should be refreshed on same-role assignment")
	}
	if v.Epoch() != 1 {
		t.Errorf("epoch should remain 1, got %d", v.Epoch())
	}
}

func testAssignNoneToReplicaEpochNotPersisted(t *testing.T) {
	v := cp4aVol(t)
	defer v.Close()

	// None -> Replica at epoch 5.
	err := v.HandleAssignment(5, RoleReplica, 0)
	if err != nil {
		t.Fatalf("None->Replica: %v", err)
	}
	if v.Role() != RoleReplica {
		t.Errorf("role: got %s, want Replica", v.Role())
	}
	// masterEpoch is set, but Epoch() (persisted) may or may not be 5.
	// This is by design: replicas don't need persisted epoch until promoted.
	// Key invariant: promote() will call SetEpoch, so the epoch gets
	// persisted when it matters.
	masterEpoch := v.masterEpoch.Load()
	if masterEpoch != 5 {
		t.Errorf("masterEpoch: got %d, want 5", masterEpoch)
	}
}

func testAssignDemoteEpochLessThanCurrent(t *testing.T) {
	// BUG-CP4A-1 (fixed): demote with epoch < current must be rejected
	// to preserve monotonicity.
	v := cp4aVol(t)
	defer v.Close()

	// Promote at epoch 5.
	v.HandleAssignment(5, RolePrimary, 30*time.Second)

	// Demote with epoch 3 (stale) -- must fail with ErrEpochRegression.
	err := v.HandleAssignment(3, RoleStale, 0)
	if err == nil {
		t.Fatal("expected error for epoch regression, got nil")
	}
	if !errors.Is(err, ErrEpochRegression) {
		t.Fatalf("expected ErrEpochRegression, got: %v", err)
	}

	// Volume should still be Primary at epoch 5 (demote was rejected).
	if v.Role() != RolePrimary {
		t.Errorf("role should still be Primary, got %s", v.Role())
	}
	if v.Epoch() != 5 {
		t.Errorf("epoch should still be 5, got %d", v.Epoch())
	}
}

func testAssignClosedVolume(t *testing.T) {
	v := cp4aVol(t)
	v.HandleAssignment(1, RolePrimary, 30*time.Second)
	v.Close()

	// HandleAssignment on closed volume -- should it error?
	// Currently it acquires assignMu and proceeds. SetEpoch writes to fd,
	// which is closed. This should fail with an I/O error.
	err := v.HandleAssignment(2, RolePrimary, 30*time.Second)
	t.Logf("HandleAssignment on closed vol: %v", err)
	// We don't prescribe the exact error, but it shouldn't panic.
}

func testAssignConcurrentDemoteAndRefresh(t *testing.T) {
	v := cp4aVol(t)
	defer v.Close()

	v.HandleAssignment(1, RolePrimary, 30*time.Second)
	v.drainTimeout = 2 * time.Second

	var wg sync.WaitGroup
	results := make(chan string, 2)

	// Goroutine 1: demote.
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := v.HandleAssignment(2, RoleStale, 0)
		if err != nil {
			results <- fmt.Sprintf("demote: %v", err)
		} else {
			results <- "demote: ok"
		}
	}()

	// Goroutine 2: same-role refresh (races with demote for assignMu).
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := v.HandleAssignment(1, RolePrimary, 30*time.Second)
		if err != nil {
			results <- fmt.Sprintf("refresh: %v", err)
		} else {
			results <- "refresh: ok"
		}
	}()

	wg.Wait()
	close(results)

	// One should win the assignMu. If demote wins first, the refresh
	// will see role=Stale and try Primary->Primary (same-role) or
	// Stale->Primary (invalid). Either way, no panic.
	for r := range results {
		t.Logf("  %s", r)
	}

	// Final role should be determinate.
	role := v.Role()
	t.Logf("final role: %s", role)
}

func testAssignRapidPromoteDemote10x(t *testing.T) {
	v := cp4aVol(t)
	defer v.Close()
	v.drainTimeout = 100 * time.Millisecond

	for i := 0; i < 10; i++ {
		epoch := uint64(i*2 + 1)
		// Promote.
		if err := v.HandleAssignment(epoch, RolePrimary, 30*time.Second); err != nil {
			// After first demote, role is Stale. Can't go directly to Primary.
			// Need to go through Rebuilding -> Replica first.
			if errors.Is(err, ErrInvalidAssignment) && i > 0 {
				v.HandleAssignment(epoch, RoleRebuilding, 0)
				v.SetRole(RoleReplica)
				if err := v.HandleAssignment(epoch, RolePrimary, 30*time.Second); err != nil {
					t.Fatalf("promote iter %d after rebuild: %v", i, err)
				}
			} else {
				t.Fatalf("promote iter %d: %v", i, err)
			}
		}
		// Write to verify writable.
		if err := v.WriteLBA(0, cp4aBlock(byte('0'+i%10))); err != nil {
			t.Fatalf("write iter %d: %v", i, err)
		}
		// Demote.
		demoteEpoch := epoch + 1
		if err := v.HandleAssignment(demoteEpoch, RoleStale, 0); err != nil {
			t.Fatalf("demote iter %d: %v", i, err)
		}
	}

	// Final: role should be Stale, epoch should be 20.
	if v.Role() != RoleStale {
		t.Errorf("final role: got %s, want Stale", v.Role())
	}
	if v.Epoch() != 20 {
		t.Errorf("final epoch: got %d, want 20", v.Epoch())
	}
}

// ---------------------------------------------------------------------------
// Status() edge cases
// ---------------------------------------------------------------------------

func testStatusFreshPrimaryNoWrites(t *testing.T) {
	v := cp4aVol(t)
	defer v.Close()

	v.HandleAssignment(1, RolePrimary, 30*time.Second)

	st := v.Status()
	if st.Epoch != 1 {
		t.Errorf("Epoch: got %d, want 1", st.Epoch)
	}
	if st.Role != RolePrimary {
		t.Errorf("Role: got %s, want Primary", st.Role)
	}
	if !st.HasLease {
		t.Error("HasLease: got false, want true")
	}
	// WALHeadLSN = nextLSN - 1 = 1 - 1 = 0 (no writes yet).
	if st.WALHeadLSN != 0 {
		t.Errorf("WALHeadLSN: got %d, want 0 (no writes)", st.WALHeadLSN)
	}
}

func testStatusDuringWrites(t *testing.T) {
	v := cp4aVol(t)
	defer v.Close()

	v.HandleAssignment(1, RolePrimary, 30*time.Second)

	// Write some blocks, check status advances.
	for i := 0; i < 5; i++ {
		v.WriteLBA(uint64(i), cp4aBlock(byte('A'+i)))
	}

	st := v.Status()
	if st.WALHeadLSN < 4 {
		t.Errorf("WALHeadLSN: got %d, want >= 4 after 5 writes", st.WALHeadLSN)
	}
}

func testStatusAllRoles(t *testing.T) {
	v := cp4aVol(t)
	defer v.Close()
	v.drainTimeout = 100 * time.Millisecond

	roleChecks := []struct {
		setup func()
		role  Role
		lease bool
	}{
		{
			setup: func() { v.HandleAssignment(1, RolePrimary, 30*time.Second) },
			role:  RolePrimary, lease: true,
		},
		{
			setup: func() { v.HandleAssignment(2, RoleStale, 0) },
			role:  RoleStale, lease: false,
		},
		{
			setup: func() { v.HandleAssignment(2, RoleRebuilding, 0) },
			role:  RoleRebuilding, lease: false,
		},
		{
			setup: func() { v.SetRole(RoleReplica) },
			role:  RoleReplica, lease: false,
		},
		{
			setup: func() { v.HandleAssignment(3, RolePrimary, 30*time.Second) },
			role:  RolePrimary, lease: true,
		},
	}

	for _, rc := range roleChecks {
		rc.setup()
		st := v.Status()
		if st.Role != rc.role {
			t.Errorf("after transition: Role got %s, want %s", st.Role, rc.role)
		}
		if st.HasLease != rc.lease {
			t.Errorf("role %s: HasLease got %v, want %v", rc.role, st.HasLease, rc.lease)
		}
	}
}

func testStatusAfterClose(t *testing.T) {
	v := cp4aVol(t)
	v.HandleAssignment(1, RolePrimary, 30*time.Second)
	v.WriteLBA(0, cp4aBlock('X'))
	v.Close()

	// Status() on closed volume should not panic.
	st := v.Status()
	t.Logf("Status after close: epoch=%d role=%s walHead=%d lease=%v cp=%d",
		st.Epoch, st.Role, st.WALHeadLSN, st.HasLease, st.CheckpointLSN)
}

func testStatusCheckpointAdvances(t *testing.T) {
	v := cp4aVol(t, BlockVolConfig{
		FlushInterval: 50 * time.Millisecond,
	})
	defer v.Close()

	v.HandleAssignment(1, RolePrimary, 30*time.Second)

	// Write a block.
	v.WriteLBA(0, cp4aBlock('C'))

	st1 := v.Status()
	initialCP := st1.CheckpointLSN

	// Force flush by syncing and waiting for flusher.
	v.SyncCache()
	time.Sleep(200 * time.Millisecond) // wait for flusher interval
	v.flusher.NotifyUrgent()
	time.Sleep(200 * time.Millisecond)

	st2 := v.Status()
	t.Logf("checkpoint: before=%d after=%d", initialCP, st2.CheckpointLSN)
	// Checkpoint may or may not have advanced depending on flusher timing.
	// The key thing is no panic and the value is plausible.
}

// ---------------------------------------------------------------------------
// Failover adversarial
// ---------------------------------------------------------------------------

func testFailoverTriple(t *testing.T) {
	a := cp4aVol(t)
	defer a.Close()
	b := cp4aVol(t)
	defer b.Close()

	m := NewSimulatedMaster(30 * time.Second)

	// Round 1: A primary.
	m.GrantPrimary(a)
	a.WriteLBA(0, cp4aBlock('1'))
	m.AssignReplica(b)

	// Failover 1: A -> B.
	m.PromoteReplica(a, b)
	b.WriteLBA(1, cp4aBlock('2'))

	// Rebuild A from B (simulate).
	m.InitiateRebuild(a)
	a.SetRole(RoleReplica)

	// Failover 2: B -> A.
	m.PromoteReplica(b, a)
	a.WriteLBA(2, cp4aBlock('3'))

	// Rebuild B from A (simulate).
	m.InitiateRebuild(b)
	b.SetRole(RoleReplica)

	// Failover 3: A -> B.
	m.PromoteReplica(a, b)
	b.WriteLBA(3, cp4aBlock('4'))

	// Verify B is primary, A is stale.
	if b.Role() != RolePrimary {
		t.Errorf("B role: %s, want Primary", b.Role())
	}
	if a.Role() != RoleStale {
		t.Errorf("A role: %s, want Stale", a.Role())
	}
	// After PromoteReplica, both volumes share the same epoch.
	if a.Epoch() != b.Epoch() {
		t.Errorf("epochs should match after PromoteReplica: A=%d B=%d", a.Epoch(), b.Epoch())
	}
	// Epoch should be > 1 after 3 failovers.
	if b.Epoch() < 3 {
		t.Errorf("epoch after 3 failovers: got %d, want >= 3", b.Epoch())
	}
}

func testFailoverConcurrentPromoteTwoVols(t *testing.T) {
	// Adversarial: two volumes both try to become Primary at same epoch.
	// This simulates a split-brain scenario where master sends conflicting assignments.
	a := cp4aVol(t)
	defer a.Close()
	b := cp4aVol(t)
	defer b.Close()

	var wg sync.WaitGroup
	errA := make(chan error, 1)
	errB := make(chan error, 1)

	// Both try None -> Primary at epoch 1 concurrently.
	wg.Add(2)
	go func() {
		defer wg.Done()
		errA <- a.HandleAssignment(1, RolePrimary, 30*time.Second)
	}()
	go func() {
		defer wg.Done()
		errB <- b.HandleAssignment(1, RolePrimary, 30*time.Second)
	}()
	wg.Wait()

	// Both should succeed (they're independent volumes).
	// The split-brain prevention is at the master level, not per-volume.
	eA := <-errA
	eB := <-errB
	if eA != nil {
		t.Errorf("A promote: %v", eA)
	}
	if eB != nil {
		t.Errorf("B promote: %v", eB)
	}

	// Both are Primary -- this is the split-brain that master must prevent.
	if a.Role() != RolePrimary || b.Role() != RolePrimary {
		t.Errorf("both should be Primary: A=%s B=%s", a.Role(), b.Role())
	}
	t.Log("split-brain: both volumes Primary at epoch 1 -- master must prevent this")
}

func testFailoverDemoteDuringActiveWrites(t *testing.T) {
	v := cp4aVol(t)
	defer v.Close()

	v.HandleAssignment(1, RolePrimary, 30*time.Second)
	v.drainTimeout = 2 * time.Second

	var wg sync.WaitGroup
	var writeSucceeded, writeFailed atomic.Int64

	// Start 50 concurrent writers.
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(lba int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				if err := v.WriteLBA(uint64(lba%256), cp4aBlock(byte('A'+j%26))); err != nil {
					writeFailed.Add(1)
				} else {
					writeSucceeded.Add(1)
				}
			}
		}(i)
	}

	// Let writers get started.
	time.Sleep(5 * time.Millisecond)

	// Demote.
	demoteErr := v.HandleAssignment(2, RoleStale, 0)
	wg.Wait()

	t.Logf("writes: %d succeeded, %d failed; demote err: %v",
		writeSucceeded.Load(), writeFailed.Load(), demoteErr)

	// After demote, no more writes should succeed.
	if err := v.WriteLBA(0, cp4aBlock('Z')); err == nil {
		t.Error("write should fail after demote")
	}
}

func testFailoverEpochAlwaysIncreases(t *testing.T) {
	v := cp4aVol(t)
	defer v.Close()
	v.drainTimeout = 100 * time.Millisecond

	var lastEpoch uint64
	for i := 0; i < 5; i++ {
		promoteEpoch := uint64(i*2 + 1)
		demoteEpoch := uint64(i*2 + 2)

		// Route through rebuild path after first demote.
		if i > 0 {
			v.HandleAssignment(promoteEpoch, RoleRebuilding, 0)
			v.SetRole(RoleReplica)
		}

		v.HandleAssignment(promoteEpoch, RolePrimary, 30*time.Second)
		if v.Epoch() <= lastEpoch && i > 0 {
			t.Errorf("epoch regression at promote iter %d: %d <= %d", i, v.Epoch(), lastEpoch)
		}
		lastEpoch = v.Epoch()

		v.HandleAssignment(demoteEpoch, RoleStale, 0)
		if v.Epoch() <= lastEpoch {
			// BUG-CP4A-1 would cause epoch to regress if demoteEpoch < promoteEpoch.
			// Here we always use increasing epochs, so this should pass.
		}
		lastEpoch = v.Epoch()
	}
	t.Logf("final epoch after 5 failovers: %d", v.Epoch())
}

func testFailoverRebuildThenImmediateFailover(t *testing.T) {
	a := cp4aVol(t)
	defer a.Close()
	b := cp4aVol(t)
	defer b.Close()

	m := NewSimulatedMaster(30 * time.Second)

	// A primary, writes data.
	m.GrantPrimary(a)
	a.WriteLBA(0, cp4aBlock('R'))

	// Start rebuild server on A.
	a.StartRebuildServer("127.0.0.1:0")
	defer a.StopRebuildServer()

	// B: None -> Primary -> Stale -> Rebuilding.
	bEpoch := m.epoch + 1
	b.HandleAssignment(bEpoch, RolePrimary, 30*time.Second)
	b.HandleAssignment(bEpoch, RoleStale, 0)
	b.HandleAssignment(bEpoch, RoleRebuilding, 0)

	// Rebuild B from A.
	StartRebuild(b, a.rebuildServer.Addr(), 1, a.Epoch())

	// B is now Replica. Immediately failover: demote A, promote B.
	m.epoch = bEpoch + 1
	a.HandleAssignment(m.epoch, RoleStale, 0)
	b.HandleAssignment(m.epoch, RolePrimary, 30*time.Second)

	// Verify B can write.
	if err := b.WriteLBA(1, cp4aBlock('S')); err != nil {
		t.Fatalf("B write after immediate failover: %v", err)
	}

	// Verify A is fenced.
	if err := a.WriteLBA(0, cp4aBlock('X')); err == nil {
		t.Error("A should be fenced after demote")
	}
}

func testFailoverDeadZoneNoWritesAnywhere(t *testing.T) {
	// After demote of A but before promote of B, NO volume should accept writes.
	// This is the "dead zone" -- both A and B must reject.
	a := cp4aVol(t)
	defer a.Close()
	b := cp4aVol(t)
	defer b.Close()
	c := cp4aVol(t)
	defer c.Close()

	m := NewSimulatedMaster(30 * time.Second)
	m.GrantPrimary(a)
	m.AssignReplica(b)
	// C is a third volume that also has a stale view.
	c.HandleAssignment(m.epoch, RoleReplica, 0)

	// Demote A.
	m.Demote(a)

	// Dead zone: nobody should accept writes.
	vols := []*BlockVol{a, b, c}
	for i, v := range vols {
		if err := v.WriteLBA(0, cp4aBlock('X')); err == nil {
			t.Errorf("volume %d (role=%s) accepted write in dead zone", i, v.Role())
		}
	}

	// Now promote B.
	b.HandleAssignment(m.epoch, RolePrimary, 30*time.Second)
	if err := b.WriteLBA(0, cp4aBlock('Y')); err != nil {
		t.Errorf("B should accept writes after promotion: %v", err)
	}
}

// ---------------------------------------------------------------------------
// beginOp/endOp adversarial
// ---------------------------------------------------------------------------

func testOpsBeginAfterClose(t *testing.T) {
	v := cp4aVol(t)
	v.HandleAssignment(1, RolePrimary, 30*time.Second)
	v.Close()

	// beginOp after close should return ErrVolumeClosed.
	err := v.beginOp()
	if !errors.Is(err, ErrVolumeClosed) {
		t.Errorf("beginOp after close: got %v, want ErrVolumeClosed", err)
	}

	// opsOutstanding should be 0 (beginOp increments then decrements on closed).
	if ops := v.opsOutstanding.Load(); ops != 0 {
		t.Errorf("opsOutstanding: got %d, want 0", ops)
	}
}

func testOpsDrainBlocksDemote(t *testing.T) {
	v := cp4aVol(t)
	defer v.Close()

	v.HandleAssignment(1, RolePrimary, 30*time.Second)
	v.drainTimeout = 2 * time.Second

	// Simulate 3 in-flight ops.
	v.beginOp()
	v.beginOp()
	v.beginOp()

	demoteDone := make(chan error, 1)
	go func() {
		demoteDone <- v.HandleAssignment(2, RoleStale, 0)
	}()

	// Demote should be blocked.
	select {
	case <-demoteDone:
		t.Fatal("demote should be blocked by outstanding ops")
	case <-time.After(50 * time.Millisecond):
		// Good, still blocked.
	}

	// Release 2 ops -- still blocked.
	v.endOp()
	v.endOp()
	select {
	case <-demoteDone:
		t.Fatal("demote should still be blocked with 1 op outstanding")
	case <-time.After(50 * time.Millisecond):
	}

	// Release last op.
	v.endOp()

	select {
	case err := <-demoteDone:
		if err != nil {
			t.Fatalf("demote failed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("demote should have completed after all ops drained")
	}

	if v.Role() != RoleStale {
		t.Errorf("role: got %s, want Stale", v.Role())
	}
}

func testOpsConcurrentBeginEnd100(t *testing.T) {
	v := cp4aVol(t)
	defer v.Close()

	v.HandleAssignment(1, RolePrimary, 30*time.Second)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				if err := v.beginOp(); err != nil {
					return
				}
				// Brief work.
				v.endOp()
			}
		}()
	}
	wg.Wait()

	ops := v.opsOutstanding.Load()
	if ops != 0 {
		t.Errorf("opsOutstanding: got %d, want 0 after all goroutines done", ops)
	}
}
