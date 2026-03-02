package blockvol

import (
	"errors"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestQAPhase4ACP1 tests Phase 4A CP1 fencing primitives adversarially:
// epoch, lease, role state machine, write gate, and full integration.
func TestQAPhase4ACP1(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		// QA-4A-1: Epoch Adversarial
		{name: "epoch_concurrent_set_epoch", run: testQAEpochConcurrentSetEpoch},
		{name: "epoch_set_during_write", run: testQAEpochSetDuringWrite},
		{name: "epoch_max_uint64", run: testQAEpochMaxUint64},
		{name: "epoch_reopen_after_concurrent_sets", run: testQAEpochReopenAfterConcurrentSets},

		// QA-4A-2: Lease Adversarial
		{name: "lease_grant_zero_ttl", run: testQALeaseGrantZeroTTL},
		{name: "lease_grant_negative_ttl", run: testQALeaseGrantNegativeTTL},
		{name: "lease_concurrent_grant_revoke", run: testQALeaseConcurrentGrantRevoke},
		{name: "lease_expiry_boundary_1ms", run: testQALeaseExpiryBoundary1ms},
		{name: "lease_revoke_during_write", run: testQALeaseRevokeDuringWrite},

		// QA-4A-3: Role State Machine Adversarial
		{name: "role_concurrent_transitions", run: testQARoleConcurrentTransitions},
		{name: "role_rapid_cycle_100x", run: testQARoleRapidCycle100x},
		{name: "role_callback_panic_recovery", run: testQARoleCallbackPanicRecovery},
		{name: "role_callback_slow_blocks_transition", run: testQARoleCallbackSlowBlocksTransition},
		{name: "role_unknown_value", run: testQARoleUnknownValue},

		// QA-4A-4: Write Gate Adversarial
		{name: "gate_role_change_during_write", run: testQAGateRoleChangeDuringWrite},
		{name: "gate_epoch_bump_during_write", run: testQAGateEpochBumpDuringWrite},
		{name: "gate_lease_expire_during_write", run: testQAGateLeaseExpireDuringWrite},
		{name: "gate_concurrent_100_writers_role_flip", run: testQAGateConcurrent100WritersRoleFlip},
		{name: "gate_gotcha_a_lease_expires_after_wal_before_sync", run: testQAGateGotchaA},

		// QA-4A-5: Integration Adversarial
		{name: "fencing_full_cycle_primary_to_stale", run: testQAFencingFullCycle},
		{name: "fencing_writes_rejected_after_demotion", run: testQAFencingWritesRejectedAfterDemotion},
		{name: "fencing_read_always_works", run: testQAFencingReadAlwaysWorks},
		{name: "fencing_close_during_role_transition", run: testQAFencingCloseDuringRoleTransition},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t)
		})
	}
}

// --- helpers ---

// createFencedVol creates a blockvol configured as Primary with matching epochs and a valid lease.
func createFencedVol(t *testing.T, opts ...CreateOptions) *BlockVol {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "fenced.blockvol")

	o := CreateOptions{
		VolumeSize: 1 * 1024 * 1024, // 1MB
		BlockSize:  4096,
		WALSize:    256 * 1024, // 256KB
	}
	if len(opts) > 0 {
		o = opts[0]
	}

	cfg := DefaultConfig()
	cfg.FlushInterval = 5 * time.Millisecond

	v, err := CreateBlockVol(path, o, cfg)
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}

	// Set up fencing: epoch=1, masterEpoch=1, role=Primary, lease=10s.
	if err := v.SetEpoch(1); err != nil {
		t.Fatalf("SetEpoch: %v", err)
	}
	v.SetMasterEpoch(1)
	if err := v.SetRole(RolePrimary); err != nil {
		t.Fatalf("SetRole(Primary): %v", err)
	}
	v.lease.Grant(10 * time.Second)

	return v
}

// --- QA-4A-1: Epoch Adversarial ---

func testQAEpochConcurrentSetEpoch(t *testing.T) {
	// BUG-4A-1 + BUG-4A-3: concurrent SetEpoch + SetMasterEpoch + WriteLBA.
	// Under -race, expects data race on v.epoch / v.masterEpoch (plain uint64).
	v := createFencedVol(t)
	defer v.Close()

	const goroutines = 8
	const iterations = 50

	var wg sync.WaitGroup

	// Epoch/masterEpoch writers.
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				epoch := uint64(id*iterations + i + 1)
				_ = v.SetEpoch(epoch)
				v.SetMasterEpoch(epoch)
			}
		}(g)
	}

	// Concurrent writer.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations*2; i++ {
			_ = v.WriteLBA(uint64(i%256), makeBlock(byte('A'+i%26)))
		}
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent SetEpoch + WriteLBA hung for 10s")
	}

	// Verify: superblock is readable and epoch is a valid value (not corrupted).
	finalEpoch := v.Epoch()
	if finalEpoch == 0 || finalEpoch > uint64(goroutines*iterations+1) {
		t.Errorf("epoch = %d, want [1, %d]", finalEpoch, goroutines*iterations+1)
	}

	f, err := os.Open(v.Path())
	if err != nil {
		t.Fatalf("open for verify: %v", err)
	}
	defer f.Close()
	sb, err := ReadSuperblock(f)
	if err != nil {
		t.Fatalf("ReadSuperblock after concurrent sets: %v (superblock corrupted)", err)
	}
	if err := sb.Validate(); err != nil {
		t.Fatalf("superblock validation failed: %v", err)
	}
	t.Logf("final superblock epoch: %d", sb.Epoch)
}

func testQAEpochSetDuringWrite(t *testing.T) {
	// BUG-4A-1: WriteLBA reads v.epoch (plain uint64) while SetEpoch writes it.
	v := createFencedVol(t)
	defer v.Close()

	const writeIter = 200
	const epochBumps = 100

	var wg sync.WaitGroup

	// Writer loop.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < writeIter; i++ {
			_ = v.WriteLBA(uint64(i%256), makeBlock(byte('W')))
		}
	}()

	// Epoch bumper.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < epochBumps; i++ {
			epoch := uint64(i + 2)
			_ = v.SetEpoch(epoch)
			v.SetMasterEpoch(epoch)
			time.Sleep(100 * time.Microsecond)
		}
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("epoch_set_during_write hung for 10s")
	}

	// Close and reopen — verify no corruption and epoch in expected range.
	path := v.Path()
	v.Close()

	v2, err := OpenBlockVol(path)
	if err != nil {
		t.Fatalf("reopen: %v (superblock may be corrupted)", err)
	}
	defer v2.Close()
	e := v2.Epoch()
	if e < 2 || e > uint64(epochBumps+1) {
		t.Errorf("reopened epoch = %d, want [2, %d]", e, epochBumps+1)
	}
	t.Logf("reopened epoch: %d", e)
}

func testQAEpochMaxUint64(t *testing.T) {
	v := createFencedVol(t)
	defer v.Close()

	// Set both epochs to MaxUint64.
	if err := v.SetEpoch(math.MaxUint64); err != nil {
		t.Fatalf("SetEpoch(MaxUint64): %v", err)
	}
	v.SetMasterEpoch(math.MaxUint64)

	if v.Epoch() != math.MaxUint64 {
		t.Errorf("Epoch() = %d, want MaxUint64", v.Epoch())
	}

	// WriteLBA should succeed (epochs match, role=Primary, lease valid).
	if err := v.WriteLBA(0, makeBlock('M')); err != nil {
		t.Fatalf("WriteLBA at MaxUint64 epoch: %v", err)
	}

	// Verify read.
	data, err := v.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA: %v", err)
	}
	if data[0] != 'M' {
		t.Errorf("data[0] = %c, want M", data[0])
	}

	// Close, reopen, verify epoch persisted.
	path := v.Path()
	v.Close()

	v2, err := OpenBlockVol(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer v2.Close()
	if v2.Epoch() != math.MaxUint64 {
		t.Errorf("reopened Epoch() = %d, want MaxUint64", v2.Epoch())
	}
}

func testQAEpochReopenAfterConcurrentSets(t *testing.T) {
	// BUG-4A-3: concurrent SetEpoch → close → reopen → verify no corruption.
	dir := t.TempDir()
	path := filepath.Join(dir, "epoch_reopen.blockvol")

	cfg := DefaultConfig()
	cfg.FlushInterval = 5 * time.Millisecond

	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    256 * 1024,
	}, cfg)
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}

	const goroutines = 4
	const iterations = 50
	validEpochs := make(map[uint64]bool)
	for g := 0; g < goroutines; g++ {
		for i := 0; i < iterations; i++ {
			validEpochs[uint64(g*iterations+i+1)] = true
		}
	}

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				_ = v.SetEpoch(uint64(id*iterations + i + 1))
			}
		}(g)
	}
	wg.Wait()

	v.Close()

	// Reopen and verify epoch is one of the valid values.
	v2, err := OpenBlockVol(path)
	if err != nil {
		t.Fatalf("reopen after concurrent sets: %v", err)
	}
	defer v2.Close()

	e := v2.Epoch()
	if !validEpochs[e] && e != 0 {
		t.Errorf("reopened epoch %d is not a valid epoch from any goroutine", e)
	}

	// Also verify superblock raw decode works.
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open raw: %v", err)
	}
	defer f.Close()
	sb, err := ReadSuperblock(f)
	if err != nil {
		t.Fatalf("raw ReadSuperblock: %v", err)
	}
	if err := sb.Validate(); err != nil {
		t.Fatalf("superblock validation: %v", err)
	}
	t.Logf("reopened epoch: %d, superblock valid", e)
}

// --- QA-4A-2: Lease Adversarial ---

func testQALeaseGrantZeroTTL(t *testing.T) {
	// BUG-4A-5: Grant(0) stores time.Now() — immediately expired.
	var l Lease
	l.Grant(0)
	if l.IsValid() {
		t.Error("Grant(0): IsValid() = true, want false (immediately expired)")
	}

	// Grant(1ns) — may or may not be valid, but must not panic.
	l.Grant(time.Nanosecond)
	_ = l.IsValid() // just verify no panic
}

func testQALeaseGrantNegativeTTL(t *testing.T) {
	// BUG-4A-5: Grant(-1s) stores past time — always expired.
	var l Lease
	l.Grant(-1 * time.Second)
	if l.IsValid() {
		t.Error("Grant(-1s): IsValid() = true, want false")
	}

	// Verify WriteLBA returns ErrLeaseExpired with a negative-TTL lease.
	v := createFencedVol(t)
	defer v.Close()
	v.lease.Grant(-1 * time.Second)

	err := v.WriteLBA(0, makeBlock('N'))
	if !errors.Is(err, ErrLeaseExpired) {
		t.Errorf("WriteLBA with negative lease: got %v, want ErrLeaseExpired", err)
	}
}

func testQALeaseConcurrentGrantRevoke(t *testing.T) {
	// Concurrent Grant + Revoke + IsValid: no panic, no data race.
	var l Lease

	const granters = 4
	const revokers = 4
	const duration = 500 * time.Millisecond

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Granters.
	wg.Add(granters)
	for i := 0; i < granters; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					l.Grant(1 * time.Second)
				}
			}
		}()
	}

	// Revokers.
	wg.Add(revokers)
	for i := 0; i < revokers; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					l.Revoke()
				}
			}
		}()
	}

	// IsValid checker.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = l.IsValid()
			}
		}
	}()

	time.Sleep(duration)
	close(stop)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent grant/revoke hung for 5s")
	}
}

func testQALeaseExpiryBoundary1ms(t *testing.T) {
	var l Lease
	l.Grant(1 * time.Millisecond)

	// Busy-poll until expired.
	deadline := time.After(50 * time.Millisecond)
	for l.IsValid() {
		select {
		case <-deadline:
			t.Fatal("lease still valid after 50ms (granted 1ms)")
		default:
			// spin
		}
	}

	// Now verify WriteLBA fails.
	v := createFencedVol(t)
	defer v.Close()
	v.lease.Grant(1 * time.Millisecond)
	time.Sleep(5 * time.Millisecond) // ensure expired

	err := v.WriteLBA(0, makeBlock('E'))
	if !errors.Is(err, ErrLeaseExpired) {
		t.Errorf("WriteLBA after 1ms lease expiry: got %v, want ErrLeaseExpired", err)
	}
}

func testQALeaseRevokeDuringWrite(t *testing.T) {
	v := createFencedVol(t)
	defer v.Close()

	// Write some data first.
	for i := 0; i < 50; i++ {
		if err := v.WriteLBA(uint64(i), makeBlock(byte('A'+i%26))); err != nil {
			t.Fatalf("pre-revoke WriteLBA(%d): %v", i, err)
		}
	}

	// Revoke lease.
	v.lease.Revoke()

	// Subsequent writes must fail.
	err := v.WriteLBA(50, makeBlock('Z'))
	if !errors.Is(err, ErrLeaseExpired) {
		t.Errorf("post-revoke WriteLBA: got %v, want ErrLeaseExpired", err)
	}

	// Reads should still work (ReadLBA doesn't check writeGate).
	for i := 0; i < 50; i++ {
		data, err := v.ReadLBA(uint64(i), 4096)
		if err != nil {
			t.Fatalf("post-revoke ReadLBA(%d): %v", i, err)
		}
		if data[0] != byte('A'+i%26) {
			t.Errorf("LBA %d: data[0] = %c, want %c", i, data[0], byte('A'+i%26))
		}
	}
}

// --- QA-4A-3: Role State Machine Adversarial ---

func testQARoleConcurrentTransitions(t *testing.T) {
	// BUG-4A-2: TOCTOU in SetRole. Two goroutines both see old=Stale,
	// both pass validation, both Store. One should fail (ideally).
	v := createFencedVol(t)
	defer v.Close()

	// Get to Stale: Primary → Draining → Stale.
	if err := v.SetRole(RoleDraining); err != nil {
		t.Fatalf("SetRole(Draining): %v", err)
	}
	if err := v.SetRole(RoleStale); err != nil {
		t.Fatalf("SetRole(Stale): %v", err)
	}

	// Now from Stale, both Rebuilding and Replica are valid transitions.
	var wg sync.WaitGroup
	var errA, errB error
	wg.Add(2)
	go func() {
		defer wg.Done()
		errA = v.SetRole(RoleRebuilding)
	}()
	go func() {
		defer wg.Done()
		errB = v.SetRole(RoleReplica)
	}()
	wg.Wait()

	// With CAS fix: exactly one must succeed, one must get ErrInvalidRoleTransition.
	// If both succeed, CAS was reverted to Load+Store (BUG-4A-2 regression).
	finalRole := v.Role()
	t.Logf("concurrent transitions from Stale: errA=%v errB=%v finalRole=%s",
		errA, errB, finalRole)

	if errA == nil && errB == nil {
		t.Error("BUG-4A-2 REGRESSION: both transitions succeeded — CAS not working")
	}
	if errA != nil && errB != nil {
		t.Error("both transitions failed — unexpected, at least one should succeed")
	}
	if finalRole != RoleRebuilding && finalRole != RoleReplica {
		t.Errorf("final role = %s, want Rebuilding or Replica", finalRole)
	}
}

func testQARoleRapidCycle100x(t *testing.T) {
	v := createFencedVol(t)
	defer v.Close()

	var callbackCount atomic.Int64
	v.SetRoleCallback(func(old, new Role) {
		callbackCount.Add(1)
	})

	// Full cycle: Primary→Draining→Stale→Rebuilding→Replica→Primary
	cycle := []Role{RoleDraining, RoleStale, RoleRebuilding, RoleReplica, RolePrimary}

	const rounds = 100
	expectedCallbacks := int64(0)
	for r := 0; r < rounds; r++ {
		for _, target := range cycle {
			if err := v.SetRole(target); err != nil {
				t.Fatalf("round %d: SetRole(%s): %v", r, target, err)
			}
			expectedCallbacks++
		}
	}

	if v.Role() != RolePrimary {
		t.Errorf("final role = %s, want Primary", v.Role())
	}

	got := callbackCount.Load()
	if got != expectedCallbacks {
		t.Errorf("callback count = %d, want %d", got, expectedCallbacks)
	}
	t.Logf("rapid cycle: %d rounds, %d callbacks", rounds, got)
}

func testQARoleCallbackPanicRecovery(t *testing.T) {
	// BUG-4A-6 FIXED: callback panic is now recovered by safeCallback.
	v := createFencedVol(t)
	defer v.Close()

	v.SetRoleCallback(func(old, new Role) {
		panic("callback panic!")
	})

	// SetRole(Draining) — callback panics but safeCallback recovers it.
	// SetRole should return nil (CAS succeeded), not propagate the panic.
	err := v.SetRole(RoleDraining)
	if err != nil {
		t.Fatalf("SetRole(Draining) returned error: %v (expected nil after panic recovery)", err)
	}

	// Role should be Draining (CAS happened before callback).
	if v.Role() != RoleDraining {
		t.Errorf("role after recovered panic = %s, want Draining", v.Role())
	}
}

func testQARoleCallbackSlowBlocksTransition(t *testing.T) {
	v := createFencedVol(t)
	defer v.Close()

	callbackDone := make(chan struct{})
	v.SetRoleCallback(func(old, new Role) {
		time.Sleep(100 * time.Millisecond)
		close(callbackDone)
	})

	// SetRole in goroutine (will be slow due to callback).
	transitionDone := make(chan error, 1)
	go func() {
		transitionDone <- v.SetRole(RoleDraining)
	}()

	// WriteLBA checks role atomically. Since CAS stores the new role before
	// callback runs, writes must see Draining after a short wait.
	time.Sleep(10 * time.Millisecond) // let CAS+Store happen

	err := v.WriteLBA(0, makeBlock('S'))
	if !errors.Is(err, ErrNotPrimary) {
		t.Errorf("write during slow callback: got %v, want ErrNotPrimary (role should already be Draining)", err)
	}

	// Wait for callback to complete.
	select {
	case <-callbackDone:
	case <-time.After(5 * time.Second):
		t.Fatal("slow callback hung for 5s")
	}

	select {
	case err := <-transitionDone:
		if err != nil {
			t.Errorf("SetRole(Draining): %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SetRole hung for 5s")
	}
}

func testQARoleUnknownValue(t *testing.T) {
	v := createFencedVol(t)
	defer v.Close()

	// Force an unknown role value (255) via atomic store.
	v.role.Store(255)

	// writeGate should reject (255 != RolePrimary).
	err := v.writeGate()
	if !errors.Is(err, ErrNotPrimary) {
		t.Errorf("writeGate with role=255: got %v, want ErrNotPrimary", err)
	}

	// SetRole(Primary) from unknown role — not in transition table.
	err = v.SetRole(RolePrimary)
	if !errors.Is(err, ErrInvalidRoleTransition) {
		t.Errorf("SetRole(Primary) from role=255: got %v, want ErrInvalidRoleTransition", err)
	}

	// WriteLBA should also fail.
	err = v.WriteLBA(0, makeBlock('U'))
	if !errors.Is(err, ErrNotPrimary) {
		t.Errorf("WriteLBA with role=255: got %v, want ErrNotPrimary", err)
	}
}

// --- QA-4A-4: Write Gate Adversarial ---

func testQAGateRoleChangeDuringWrite(t *testing.T) {
	v := createFencedVol(t)
	defer v.Close()

	const writers = 16
	const writesPerGoroutine = 50

	var wg sync.WaitGroup
	var succeeded, rejected atomic.Int64

	// Many concurrent writers.
	wg.Add(writers)
	for g := 0; g < writers; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < writesPerGoroutine; i++ {
				lba := uint64((id*writesPerGoroutine + i) % 256)
				err := v.WriteLBA(lba, makeBlock(byte('A'+id%26)))
				if err == nil {
					succeeded.Add(1)
				} else if errors.Is(err, ErrNotPrimary) {
					rejected.Add(1)
				}
				// other errors (WAL full, etc.) are ok
			}
		}(g)
	}

	// After 10ms, flip role.
	time.Sleep(10 * time.Millisecond)
	if err := v.SetRole(RoleDraining); err != nil {
		t.Logf("SetRole(Draining): %v", err)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("gate_role_change_during_write hung for 10s")
	}

	t.Logf("role flip: %d succeeded, %d rejected (ErrNotPrimary)",
		succeeded.Load(), rejected.Load())

	// After transition, ALL new writes must fail.
	err := v.WriteLBA(0, makeBlock('X'))
	if !errors.Is(err, ErrNotPrimary) {
		t.Errorf("write after Draining: got %v, want ErrNotPrimary", err)
	}
}

func testQAGateEpochBumpDuringWrite(t *testing.T) {
	// BUG-4A-1: data race on v.epoch reads in writeGate/WriteLBA vs SetEpoch writes.
	// Use large WAL + short WALFullTimeout to avoid writers blocking on WAL-full.
	dir := t.TempDir()
	path := filepath.Join(dir, "epoch_bump.blockvol")
	cfg := DefaultConfig()
	cfg.FlushInterval = 5 * time.Millisecond
	cfg.WALFullTimeout = 200 * time.Millisecond

	v, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    1 * 1024 * 1024, // 1MB WAL — plenty of room
	}, cfg)
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	defer v.Close()

	// Set up fencing.
	if err := v.SetEpoch(1); err != nil {
		t.Fatalf("SetEpoch: %v", err)
	}
	v.SetMasterEpoch(1)
	if err := v.SetRole(RolePrimary); err != nil {
		t.Fatalf("SetRole: %v", err)
	}
	v.lease.Grant(10 * time.Second)

	const writers = 8
	const iterations = 30

	var wg sync.WaitGroup

	// Writers.
	wg.Add(writers)
	for g := 0; g < writers; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				_ = v.WriteLBA(uint64((id*iterations+i)%256), makeBlock(byte('A'+id%26)))
			}
		}(g)
	}

	// Epoch bumper: set epoch=2, masterEpoch=2.
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(5 * time.Millisecond)
		_ = v.SetEpoch(2)
		v.SetMasterEpoch(2)
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("gate_epoch_bump_during_write hung for 10s")
	}

	// After bump, both epochs are 2 — writes must succeed.
	if v.Epoch() != 2 {
		t.Errorf("Epoch() = %d after bump, want 2", v.Epoch())
	}
	if writeErr := v.WriteLBA(0, makeBlock('Z')); writeErr != nil {
		t.Errorf("write after epoch bump: %v (epochs should match)", writeErr)
	}
}

func testQAGateLeaseExpireDuringWrite(t *testing.T) {
	v := createFencedVol(t)
	defer v.Close()

	// Short lease — write in a loop that spans the expiry window.
	v.lease.Grant(10 * time.Millisecond)

	var succeeded, expired int
	deadline := time.After(200 * time.Millisecond)
	i := 0
loop:
	for {
		select {
		case <-deadline:
			break loop
		default:
		}
		writeErr := v.WriteLBA(uint64(i%256), makeBlock(byte('W')))
		if writeErr == nil {
			succeeded++
		} else if errors.Is(writeErr, ErrLeaseExpired) {
			expired++
		}
		i++
		time.Sleep(500 * time.Microsecond)
	}

	t.Logf("lease expire: %d succeeded, %d expired (out of %d)", succeeded, expired, i)

	if expired == 0 {
		t.Error("no writes expired — lease didn't expire in time?")
	}
	if succeeded == 0 {
		t.Error("no writes succeeded — lease expired immediately?")
	}

	// Verify monotonicity: once expired, no more successes.
	v.lease.Grant(0) // force expired
	for j := 0; j < 10; j++ {
		writeErr := v.WriteLBA(uint64(j), makeBlock('X'))
		if !errors.Is(writeErr, ErrLeaseExpired) {
			t.Errorf("write %d after forced expiry: got %v, want ErrLeaseExpired", j, writeErr)
		}
	}
}

func testQAGateConcurrent100WritersRoleFlip(t *testing.T) {
	// Use large WAL + short timeout to avoid writers blocking on WAL-full.
	dir := t.TempDir()
	path := filepath.Join(dir, "100writers.blockvol")
	cfg := DefaultConfig()
	cfg.FlushInterval = 5 * time.Millisecond
	cfg.WALFullTimeout = 200 * time.Millisecond

	vol, err := CreateBlockVol(path, CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    2 * 1024 * 1024, // 2MB WAL
	}, cfg)
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	if err := vol.SetEpoch(1); err != nil {
		t.Fatalf("SetEpoch: %v", err)
	}
	vol.SetMasterEpoch(1)
	if err := vol.SetRole(RolePrimary); err != nil {
		t.Fatalf("SetRole: %v", err)
	}
	vol.lease.Grant(10 * time.Second)
	v := vol
	defer v.Close()

	const writers = 100
	const blocksPerWriter = 10

	var wg sync.WaitGroup
	var succeeded, notPrimary atomic.Int64

	// 100 writers.
	wg.Add(writers)
	for g := 0; g < writers; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < blocksPerWriter; i++ {
				lba := uint64((id*blocksPerWriter + i) % 256)
				err := v.WriteLBA(lba, makeBlock(byte('A'+id%26)))
				if err == nil {
					succeeded.Add(1)
				} else if errors.Is(err, ErrNotPrimary) {
					notPrimary.Add(1)
				}
			}
		}(g)
	}

	// Flip role mid-flight.
	time.Sleep(50 * time.Millisecond)
	_ = v.SetRole(RoleDraining)
	time.Sleep(50 * time.Millisecond)
	_ = v.SetRole(RoleStale)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("100 writers + role flip hung for 10s")
	}

	s, np := succeeded.Load(), notPrimary.Load()
	t.Logf("100 writers: %d succeeded, %d ErrNotPrimary", s, np)

	// Verify data integrity for successful writes: each block is readable.
	// We can't know which exact writes succeeded, but ReadLBA should not error.
	for i := 0; i < 256; i++ {
		_, err := v.ReadLBA(uint64(i), 4096)
		if err != nil {
			t.Fatalf("ReadLBA(%d) after stress: %v", i, err)
		}
	}
}

func testQAGateGotchaA(t *testing.T) {
	// Gotcha A: lease expires between WAL append and SyncCache.
	// Data IS in WAL, but SyncCache returns ErrLeaseExpired.
	v := createFencedVol(t)
	defer v.Close()

	// Grant a short lease.
	v.lease.Grant(50 * time.Millisecond)

	// Write data (succeeds — lease still valid).
	if err := v.WriteLBA(0, makeBlock('G')); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}

	// Wait for lease to expire.
	time.Sleep(100 * time.Millisecond)

	// SyncCache must fail — PostSyncCheck calls writeGate, which checks lease.
	err := v.SyncCache()
	if !errors.Is(err, ErrLeaseExpired) {
		t.Errorf("SyncCache after lease expiry: got %v, want ErrLeaseExpired (Gotcha A)", err)
	}

	// Key verification: data IS readable from dirty map / WAL.
	data, err := v.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA after Gotcha A: %v", err)
	}
	if data[0] != 'G' {
		t.Errorf("data[0] = %c, want G (data should be in WAL/dirty map)", data[0])
	}
	t.Log("Gotcha A: data in WAL, SyncCache fenced, ReadLBA returns written data")
}

// --- QA-4A-5: Integration Adversarial ---

func testQAFencingFullCycle(t *testing.T) {
	v := createFencedVol(t)
	defer v.Close()

	// Write 100 blocks as Primary.
	for i := 0; i < 100; i++ {
		if err := v.WriteLBA(uint64(i), makeBlock(byte('A'+i%26))); err != nil {
			t.Fatalf("WriteLBA(%d): %v", i, err)
		}
	}
	if err := v.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}

	// Verify all 100 blocks.
	for i := 0; i < 100; i++ {
		data, err := v.ReadLBA(uint64(i), 4096)
		if err != nil {
			t.Fatalf("ReadLBA(%d): %v", i, err)
		}
		if data[0] != byte('A'+i%26) {
			t.Errorf("LBA %d: data[0] = %c, want %c", i, data[0], byte('A'+i%26))
		}
	}

	// Primary → Draining — writes should fail.
	if err := v.SetRole(RoleDraining); err != nil {
		t.Fatalf("SetRole(Draining): %v", err)
	}
	if err := v.WriteLBA(0, makeBlock('X')); !errors.Is(err, ErrNotPrimary) {
		t.Errorf("write as Draining: got %v, want ErrNotPrimary", err)
	}

	// Draining → Stale — writes rejected.
	if err := v.SetRole(RoleStale); err != nil {
		t.Fatalf("SetRole(Stale): %v", err)
	}
	if err := v.WriteLBA(0, makeBlock('X')); !errors.Is(err, ErrNotPrimary) {
		t.Errorf("write as Stale: got %v, want ErrNotPrimary", err)
	}

	// Bump epoch, transition through Rebuilding → Replica.
	if err := v.SetEpoch(2); err != nil {
		t.Fatalf("SetEpoch(2): %v", err)
	}
	v.SetMasterEpoch(2)

	if err := v.SetRole(RoleRebuilding); err != nil {
		t.Fatalf("SetRole(Rebuilding): %v", err)
	}
	if err := v.WriteLBA(0, makeBlock('X')); !errors.Is(err, ErrNotPrimary) {
		t.Errorf("write as Rebuilding: got %v, want ErrNotPrimary", err)
	}

	if err := v.SetRole(RoleReplica); err != nil {
		t.Fatalf("SetRole(Replica): %v", err)
	}
	if err := v.WriteLBA(0, makeBlock('X')); !errors.Is(err, ErrNotPrimary) {
		t.Errorf("write as Replica: got %v, want ErrNotPrimary", err)
	}

	// Replica → Primary with fresh lease — writes succeed again.
	if err := v.SetRole(RolePrimary); err != nil {
		t.Fatalf("SetRole(Primary): %v", err)
	}
	v.lease.Grant(10 * time.Second)

	if err := v.WriteLBA(200, makeBlock('N')); err != nil {
		t.Fatalf("write as new Primary: %v", err)
	}

	// Original 100 blocks still readable.
	for i := 0; i < 100; i++ {
		data, err := v.ReadLBA(uint64(i), 4096)
		if err != nil {
			t.Fatalf("final ReadLBA(%d): %v", i, err)
		}
		if data[0] != byte('A'+i%26) {
			t.Errorf("final LBA %d: data[0] = %c, want %c", i, data[0], byte('A'+i%26))
		}
	}
	t.Log("full cycle: Primary→Draining→Stale→Rebuilding→Replica→Primary complete")
}

func testQAFencingWritesRejectedAfterDemotion(t *testing.T) {
	v := createFencedVol(t)
	defer v.Close()

	// Write initial data.
	if err := v.WriteLBA(0, makeBlock('D')); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}
	if err := v.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}

	// Revoke lease → ErrLeaseExpired.
	v.lease.Revoke()
	err := v.WriteLBA(1, makeBlock('X'))
	if !errors.Is(err, ErrLeaseExpired) {
		t.Errorf("after Revoke: got %v, want ErrLeaseExpired", err)
	}

	// Re-grant lease for role checks, then demote.
	v.lease.Grant(10 * time.Second)

	// Primary → Draining → ErrNotPrimary.
	if err := v.SetRole(RoleDraining); err != nil {
		t.Fatalf("SetRole(Draining): %v", err)
	}
	err = v.WriteLBA(1, makeBlock('X'))
	if !errors.Is(err, ErrNotPrimary) {
		t.Errorf("after Draining: got %v, want ErrNotPrimary", err)
	}

	// Draining → Stale → ErrNotPrimary.
	if err := v.SetRole(RoleStale); err != nil {
		t.Fatalf("SetRole(Stale): %v", err)
	}
	err = v.WriteLBA(1, makeBlock('X'))
	if !errors.Is(err, ErrNotPrimary) {
		t.Errorf("after Stale: got %v, want ErrNotPrimary", err)
	}

	// Verify: LBA 0 has original data, LBA 1 was never written.
	data, readErr := v.ReadLBA(0, 4096)
	if readErr != nil {
		t.Fatalf("ReadLBA(0): %v", readErr)
	}
	if data[0] != 'D' {
		t.Errorf("LBA 0: data[0] = %c, want D", data[0])
	}
}

func testQAFencingReadAlwaysWorks(t *testing.T) {
	v := createFencedVol(t)
	defer v.Close()

	// Write data as Primary.
	for i := 0; i < 10; i++ {
		if err := v.WriteLBA(uint64(i), makeBlock(byte('R'+i%6))); err != nil {
			t.Fatalf("WriteLBA(%d): %v", i, err)
		}
	}

	verifyReads := func(label string) {
		for i := 0; i < 10; i++ {
			data, err := v.ReadLBA(uint64(i), 4096)
			if err != nil {
				t.Fatalf("[%s] ReadLBA(%d): %v", label, i, err)
			}
			if data[0] != byte('R'+i%6) {
				t.Errorf("[%s] LBA %d: data[0] = %c, want %c", label, i, data[0], byte('R'+i%6))
			}
		}
	}

	// Reads work as Primary.
	verifyReads("Primary")

	// Draining.
	if err := v.SetRole(RoleDraining); err != nil {
		t.Fatalf("SetRole(Draining): %v", err)
	}
	verifyReads("Draining")

	// Stale.
	if err := v.SetRole(RoleStale); err != nil {
		t.Fatalf("SetRole(Stale): %v", err)
	}
	verifyReads("Stale")

	// Rebuilding.
	if err := v.SetRole(RoleRebuilding); err != nil {
		t.Fatalf("SetRole(Rebuilding): %v", err)
	}
	verifyReads("Rebuilding")

	// Replica.
	if err := v.SetRole(RoleReplica); err != nil {
		t.Fatalf("SetRole(Replica): %v", err)
	}
	verifyReads("Replica")

	// Expired lease — reads still work.
	v.lease.Revoke()
	verifyReads("expired_lease")

	// Stale epoch — reads still work.
	v.SetMasterEpoch(999)
	verifyReads("stale_epoch")

	t.Log("reads work in all roles, with expired lease and stale epoch")
}

func testQAFencingCloseDuringRoleTransition(t *testing.T) {
	v := createFencedVol(t)

	// Start some writes.
	for i := 0; i < 20; i++ {
		_ = v.WriteLBA(uint64(i), makeBlock(byte('C')))
	}

	// Concurrent Close + SetRole.
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		_ = v.Close()
	}()

	go func() {
		defer wg.Done()
		_ = v.SetRole(RoleDraining)
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		t.Log("Close + SetRole concurrent: no deadlock, no panic")
	case <-time.After(5 * time.Second):
		t.Fatal("Close + SetRole deadlocked for 5s")
	}
}
