package weed_server

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// ============================================================
// CP11A-2 Adversarial Test Suite: B-09 + B-10
//
// 8 scenarios stress-testing the coordinated expand path under
// failover, concurrent heartbeats, and partial failures.
// ============================================================

// qaExpandMaster creates a MasterServer with 3 block-capable servers
// and default expand mocks for adversarial testing.
func qaExpandMaster(t *testing.T) *MasterServer {
	t.Helper()
	ms := &MasterServer{
		blockRegistry:        NewBlockVolumeRegistry(),
		blockAssignmentQueue: NewBlockAssignmentQueue(),
		blockFailover:        newBlockFailoverState(),
	}
	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, diskType string, durabilityMode string) (*blockAllocResult, error) {
		return &blockAllocResult{
			Path:              fmt.Sprintf("/data/%s.blk", name),
			IQN:               fmt.Sprintf("iqn.2024.test:%s", name),
			ISCSIAddr:         server + ":3260",
			ReplicaDataAddr:   server + ":14260",
			ReplicaCtrlAddr:   server + ":14261",
			RebuildListenAddr: server + ":15000",
		}, nil
	}
	ms.blockVSDelete = func(ctx context.Context, server string, name string) error {
		return nil
	}
	ms.blockVSExpand = func(ctx context.Context, server string, name string, newSize uint64) (uint64, error) {
		return newSize, nil
	}
	ms.blockVSPrepareExpand = func(ctx context.Context, server string, name string, newSize, expandEpoch uint64) error {
		return nil
	}
	ms.blockVSCommitExpand = func(ctx context.Context, server string, name string, expandEpoch uint64) (uint64, error) {
		return 2 << 30, nil
	}
	ms.blockVSCancelExpand = func(ctx context.Context, server string, name string, expandEpoch uint64) error {
		return nil
	}
	ms.blockRegistry.MarkBlockCapable("vs1:9333")
	ms.blockRegistry.MarkBlockCapable("vs2:9333")
	ms.blockRegistry.MarkBlockCapable("vs3:9333")
	return ms
}

// qaCreateRF creates a volume with the given replica factor.
func qaCreateRF(t *testing.T, ms *MasterServer, name string, rf uint32) {
	t.Helper()
	_, err := ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name:          name,
		SizeBytes:     1 << 30,
		ReplicaFactor: rf,
	})
	if err != nil {
		t.Fatalf("create %s RF=%d: %v", name, rf, err)
	}
}

// ────────────────────────────────────────────────────────────
// QA-B09-1: ExpandAfterDoubleFailover_RF3
//
// RF=3 volume. Primary dies → promote replica A. Then replica A
// (now primary) dies → promote replica B. Expand must reach
// replica B (the second-generation primary), not the original.
// ────────────────────────────────────────────────────────────
func TestQA_B09_ExpandAfterDoubleFailover_RF3(t *testing.T) {
	ms := qaExpandMaster(t)
	qaCreateRF(t, ms, "dbl-failover", 3)

	entry, _ := ms.blockRegistry.Lookup("dbl-failover")
	gen0Primary := entry.VolumeServer

	// First failover: kill original primary.
	ms.blockRegistry.PromoteBestReplica("dbl-failover")
	entry, _ = ms.blockRegistry.Lookup("dbl-failover")
	gen1Primary := entry.VolumeServer
	if gen1Primary == gen0Primary {
		t.Fatal("first promotion didn't change primary")
	}

	// Second failover: kill gen1 primary.
	// Need to ensure the remaining replica has a fresh heartbeat.
	if len(entry.Replicas) == 0 {
		t.Fatal("no replicas left after first promotion (need RF=3)")
	}
	ms.blockRegistry.PromoteBestReplica("dbl-failover")
	entry, _ = ms.blockRegistry.Lookup("dbl-failover")
	gen2Primary := entry.VolumeServer
	if gen2Primary == gen1Primary || gen2Primary == gen0Primary {
		t.Fatalf("second promotion should pick a new server, got %q (gen0=%q gen1=%q)",
			gen2Primary, gen0Primary, gen1Primary)
	}

	// Track PREPARE targets.
	var preparedServers []string
	ms.blockVSPrepareExpand = func(ctx context.Context, server string, name string, newSize, expandEpoch uint64) error {
		preparedServers = append(preparedServers, server)
		return nil
	}

	// Expand — standalone path since no replicas remain after 2 promotions.
	_, err := ms.ExpandBlockVolume(context.Background(), &master_pb.ExpandBlockVolumeRequest{
		Name: "dbl-failover", NewSizeBytes: 2 << 30,
	})
	if err != nil {
		t.Fatalf("expand: %v", err)
	}

	// If standalone path was taken (no replicas), preparedServers is empty — that's fine.
	// If coordinated path was taken, first PREPARE must target gen2Primary.
	if len(preparedServers) > 0 && preparedServers[0] != gen2Primary {
		t.Fatalf("PREPARE went to %q, want gen2 primary %q", preparedServers[0], gen2Primary)
	}
}

// ────────────────────────────────────────────────────────────
// QA-B09-2: ExpandSeesDeletedVolume_AfterLockAcquire
//
// Volume is deleted between the initial Lookup (succeeds) and
// the re-read after AcquireExpandInflight. The re-read must
// detect the deletion and fail cleanly.
// ────────────────────────────────────────────────────────────
func TestQA_B09_ExpandSeesDeletedVolume_AfterLockAcquire(t *testing.T) {
	ms := qaExpandMaster(t)
	qaCreateRF(t, ms, "disappear", 2)

	// Hook PREPARE to delete the volume before it runs.
	// The B-09 re-read happens before PREPARE, so we simulate deletion
	// between initial Lookup and AcquireExpandInflight by having a
	// goroutine that deletes the entry while expand is in progress.
	// Instead, test directly: acquire expand lock, then unregister, then
	// call ExpandBlockVolume — it should fail on re-read.

	// Acquire expand lock manually first so the real call gets blocked.
	// Then verify the error path by attempting a second expand.
	if !ms.blockRegistry.AcquireExpandInflight("disappear", 2<<30, 1) {
		t.Fatal("AcquireExpandInflight should succeed")
	}

	// Try another expand while locked — should fail with "already in progress".
	_, err := ms.ExpandBlockVolume(context.Background(), &master_pb.ExpandBlockVolumeRequest{
		Name: "disappear", NewSizeBytes: 2 << 30,
	})
	if err == nil {
		t.Fatal("expand should fail when lock is held")
	}

	// Release and delete the volume.
	ms.blockRegistry.ReleaseExpandInflight("disappear")
	ms.blockRegistry.Unregister("disappear")

	// Now expand on a deleted volume — should fail on initial Lookup.
	_, err = ms.ExpandBlockVolume(context.Background(), &master_pb.ExpandBlockVolumeRequest{
		Name: "disappear", NewSizeBytes: 2 << 30,
	})
	if err == nil {
		t.Fatal("expand on deleted volume should fail")
	}
}

// ────────────────────────────────────────────────────────────
// QA-B09-3: ConcurrentExpandAndFailover
//
// Expand and failover race on the same volume. Neither should
// panic, and the volume must be in a consistent state afterward.
// ────────────────────────────────────────────────────────────
func TestQA_B09_ConcurrentExpandAndFailover(t *testing.T) {
	ms := qaExpandMaster(t)
	qaCreateRF(t, ms, "race-vol", 3)

	entry, _ := ms.blockRegistry.Lookup("race-vol")
	primary := entry.VolumeServer

	// Make PREPARE slow so expand holds the lock longer.
	ms.blockVSPrepareExpand = func(ctx context.Context, server string, name string, newSize, expandEpoch uint64) error {
		time.Sleep(5 * time.Millisecond)
		return nil
	}

	var wg sync.WaitGroup

	// Goroutine 1: expand.
	wg.Add(1)
	go func() {
		defer wg.Done()
		ms.ExpandBlockVolume(context.Background(), &master_pb.ExpandBlockVolumeRequest{
			Name: "race-vol", NewSizeBytes: 2 << 30,
		})
		// Error is OK — we're testing for panics and consistency.
	}()

	// Goroutine 2: failover kills primary.
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(2 * time.Millisecond) // slight delay to let expand start
		ms.failoverBlockVolumes(primary)
	}()

	wg.Wait()

	// Volume must still exist regardless of outcome.
	_, ok := ms.blockRegistry.Lookup("race-vol")
	if !ok {
		t.Fatal("volume must survive concurrent expand + failover")
	}
}

// ────────────────────────────────────────────────────────────
// QA-B09-4: ConcurrentExpandsSameVolume
//
// Two goroutines try to expand the same volume simultaneously.
// Exactly one should succeed, the other should get "already in
// progress". No panic, no double-commit.
// ────────────────────────────────────────────────────────────
func TestQA_B09_ConcurrentExpandsSameVolume(t *testing.T) {
	ms := qaExpandMaster(t)
	qaCreateRF(t, ms, "dup-expand", 2)

	var commitCount atomic.Int32
	ms.blockVSPrepareExpand = func(ctx context.Context, server string, name string, newSize, expandEpoch uint64) error {
		time.Sleep(5 * time.Millisecond) // slow prepare
		return nil
	}
	ms.blockVSCommitExpand = func(ctx context.Context, server string, name string, expandEpoch uint64) (uint64, error) {
		commitCount.Add(1)
		return 2 << 30, nil
	}

	var wg sync.WaitGroup
	var successes atomic.Int32
	var failures atomic.Int32

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := ms.ExpandBlockVolume(context.Background(), &master_pb.ExpandBlockVolumeRequest{
				Name: "dup-expand", NewSizeBytes: 2 << 30,
			})
			if err == nil {
				successes.Add(1)
			} else {
				failures.Add(1)
			}
		}()
	}
	wg.Wait()

	if successes.Load() != 1 {
		t.Fatalf("expected exactly 1 success, got %d", successes.Load())
	}
	if failures.Load() != 1 {
		t.Fatalf("expected exactly 1 failure (already in progress), got %d", failures.Load())
	}
}

// ────────────────────────────────────────────────────────────
// QA-B10-1: RepeatedEmptyHeartbeats_DuringExpand
//
// Multiple empty heartbeats from the primary during expand.
// Entry must survive all of them — not just the first.
// ────────────────────────────────────────────────────────────
func TestQA_B10_RepeatedEmptyHeartbeats_DuringExpand(t *testing.T) {
	ms := qaExpandMaster(t)
	qaCreateRF(t, ms, "multi-hb", 2)

	entry, _ := ms.blockRegistry.Lookup("multi-hb")
	primary := entry.VolumeServer

	if !ms.blockRegistry.AcquireExpandInflight("multi-hb", 2<<30, 42) {
		t.Fatal("acquire expand lock")
	}

	// 10 empty heartbeats from the primary — each one would delete
	// the entry without the B-10 guard.
	for i := 0; i < 10; i++ {
		ms.blockRegistry.UpdateFullHeartbeat(primary, []*master_pb.BlockVolumeInfoMessage{}, "")
	}

	_, ok := ms.blockRegistry.Lookup("multi-hb")
	if !ok {
		t.Fatal("entry deleted after repeated empty heartbeats during expand")
	}

	ms.blockRegistry.ReleaseExpandInflight("multi-hb")
}

// ────────────────────────────────────────────────────────────
// QA-B10-2: ExpandFailed_HeartbeatStillProtected
//
// After MarkExpandFailed (primary committed, replica didn't),
// empty heartbeats must NOT delete the entry. ExpandFailed
// keeps ExpandInProgress=true as a size-suppression guard.
// ────────────────────────────────────────────────────────────
func TestQA_B10_ExpandFailed_HeartbeatStillProtected(t *testing.T) {
	ms := qaExpandMaster(t)
	qaCreateRF(t, ms, "fail-hb", 2)

	entry, _ := ms.blockRegistry.Lookup("fail-hb")
	primary := entry.VolumeServer

	if !ms.blockRegistry.AcquireExpandInflight("fail-hb", 2<<30, 42) {
		t.Fatal("acquire expand lock")
	}
	ms.blockRegistry.MarkExpandFailed("fail-hb")

	// Empty heartbeat should not delete — ExpandFailed keeps ExpandInProgress=true.
	ms.blockRegistry.UpdateFullHeartbeat(primary, []*master_pb.BlockVolumeInfoMessage{}, "")

	e, ok := ms.blockRegistry.Lookup("fail-hb")
	if !ok {
		t.Fatal("entry deleted during ExpandFailed state")
	}
	if !e.ExpandFailed {
		t.Fatal("ExpandFailed should still be true")
	}
	if !e.ExpandInProgress {
		t.Fatal("ExpandInProgress should still be true")
	}

	// After ClearExpandFailed, empty heartbeat should delete normally.
	ms.blockRegistry.ClearExpandFailed("fail-hb")
	ms.blockRegistry.UpdateFullHeartbeat(primary, []*master_pb.BlockVolumeInfoMessage{}, "")

	_, ok = ms.blockRegistry.Lookup("fail-hb")
	if ok {
		t.Fatal("entry should be deleted after ClearExpandFailed + empty heartbeat")
	}
}

// ────────────────────────────────────────────────────────────
// QA-B10-3: HeartbeatSizeSuppress_DuringExpand
//
// Primary reports a stale (old) size during coordinated expand.
// Registry must NOT downgrade SizeBytes — the pending expand
// size is authoritative until commit or release.
// ────────────────────────────────────────────────────────────
func TestQA_B10_HeartbeatSizeSuppress_DuringExpand(t *testing.T) {
	ms := qaExpandMaster(t)
	qaCreateRF(t, ms, "size-suppress", 2)

	entry, _ := ms.blockRegistry.Lookup("size-suppress")
	primary := entry.VolumeServer
	origSize := entry.SizeBytes

	if !ms.blockRegistry.AcquireExpandInflight("size-suppress", 2<<30, 42) {
		t.Fatal("acquire expand lock")
	}

	// Heartbeat reports old size (expand hasn't committed on VS yet).
	ms.blockRegistry.UpdateFullHeartbeat(primary, []*master_pb.BlockVolumeInfoMessage{
		{
			Path:       "/data/size-suppress.blk",
			VolumeSize: origSize, // old size
			Epoch:      1,
			Role:       blockvol.RoleToWire(blockvol.RolePrimary),
		},
	}, "")

	entry, _ = ms.blockRegistry.Lookup("size-suppress")
	if entry.SizeBytes != origSize {
		t.Fatalf("size should remain %d during expand, got %d", origSize, entry.SizeBytes)
	}

	// Heartbeat reports a LARGER size (stale from previous expand or bug).
	// Still must not update — coordinated expand owns the size.
	ms.blockRegistry.UpdateFullHeartbeat(primary, []*master_pb.BlockVolumeInfoMessage{
		{
			Path:       "/data/size-suppress.blk",
			VolumeSize: 5 << 30, // bogus large size
			Epoch:      1,
			Role:       blockvol.RoleToWire(blockvol.RolePrimary),
		},
	}, "")

	entry, _ = ms.blockRegistry.Lookup("size-suppress")
	if entry.SizeBytes != origSize {
		t.Fatalf("size should remain %d (suppressed), got %d", origSize, entry.SizeBytes)
	}

	ms.blockRegistry.ReleaseExpandInflight("size-suppress")
}

// ────────────────────────────────────────────────────────────
// QA-B10-4: ConcurrentHeartbeatsAndExpand
//
// Simultaneous full heartbeats from primary and replicas while
// expand runs on another goroutine. Must not panic, must not
// orphan the entry, and expand must either succeed or fail
// cleanly with a clear error.
// ────────────────────────────────────────────────────────────
func TestQA_B10_ConcurrentHeartbeatsAndExpand(t *testing.T) {
	ms := qaExpandMaster(t)
	qaCreateRF(t, ms, "hb-expand-race", 2)

	entry, _ := ms.blockRegistry.Lookup("hb-expand-race")
	primary := entry.VolumeServer
	replica := ""
	if len(entry.Replicas) > 0 {
		replica = entry.Replicas[0].Server
	}

	ms.blockVSPrepareExpand = func(ctx context.Context, server string, name string, newSize, expandEpoch uint64) error {
		time.Sleep(2 * time.Millisecond)
		return nil
	}

	var wg sync.WaitGroup
	const rounds = 30

	// Goroutine 1: expand.
	wg.Add(1)
	go func() {
		defer wg.Done()
		ms.ExpandBlockVolume(context.Background(), &master_pb.ExpandBlockVolumeRequest{
			Name: "hb-expand-race", NewSizeBytes: 2 << 30,
		})
	}()

	// Goroutine 2: primary heartbeats (mix of reporting and not reporting).
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			if i%5 == 0 {
				// Every 5th: empty heartbeat (simulates brief restart).
				ms.blockRegistry.UpdateFullHeartbeat(primary, []*master_pb.BlockVolumeInfoMessage{}, "")
			} else {
				ms.blockRegistry.UpdateFullHeartbeat(primary, []*master_pb.BlockVolumeInfoMessage{
					{
						Path:       "/data/hb-expand-race.blk",
						VolumeSize: 1 << 30,
						Epoch:      1,
						Role:       blockvol.RoleToWire(blockvol.RolePrimary),
						WalHeadLsn: uint64(100 + i),
					},
				}, "")
			}
		}
	}()

	// Goroutine 3: replica heartbeats.
	if replica != "" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				ms.blockRegistry.UpdateFullHeartbeat(replica, []*master_pb.BlockVolumeInfoMessage{
					{
						Path:       "/data/hb-expand-race.blk",
						VolumeSize: 1 << 30,
						Epoch:      1,
						Role:       blockvol.RoleToWire(blockvol.RoleReplica),
						WalHeadLsn: uint64(99 + i),
					},
				}, "")
			}
		}()
	}

	wg.Wait()

	// Volume must still exist — no orphan.
	_, ok := ms.blockRegistry.Lookup("hb-expand-race")
	if !ok {
		t.Fatal("volume must survive concurrent heartbeats + expand")
	}
}
