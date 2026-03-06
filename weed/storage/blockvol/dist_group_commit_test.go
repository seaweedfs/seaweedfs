package blockvol

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// mockShipperGroup creates a ShipperGroup with N mock shippers.
// failIdxs specifies which shippers should return errors on Barrier.
func newTestVolWithMode(t *testing.T, mode DurabilityMode) (*BlockVol, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.blk")
	vol, err := CreateBlockVol(path, CreateOptions{
		VolumeSize:     4 * 1024 * 1024,
		DurabilityMode: mode,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol: %v", err)
	}
	// Write some data so nextLSN > 0.
	data := make([]byte, 4096)
	data[0] = 0xAB
	if err := vol.WriteLBA(0, data); err != nil {
		t.Fatalf("WriteLBA: %v", err)
	}
	return vol, dir
}

// fakeShipper creates a WALShipper that we can control degraded state for testing.
// We can't easily create mock WALShippers, but we can test MakeDistributedSync
// with nil/empty ShipperGroup for edge cases and real shippers for others.

func TestDistSync_BestEffort_NilGroup(t *testing.T) {
	vol, _ := newTestVolWithMode(t, DurabilityBestEffort)
	defer vol.Close()

	syncCalled := false
	fn := MakeDistributedSync(func() error {
		syncCalled = true
		return nil
	}, nil, vol)

	if err := fn(); err != nil {
		t.Fatalf("sync error: %v", err)
	}
	if !syncCalled {
		t.Error("walSync not called")
	}
}

func TestDistSync_SyncAll_NilGroup_Succeeds(t *testing.T) {
	// sync_all with nil group (no replicas configured) should succeed locally.
	vol, _ := newTestVolWithMode(t, DurabilitySyncAll)
	defer vol.Close()

	fn := MakeDistributedSync(func() error { return nil }, nil, vol)
	if err := fn(); err != nil {
		t.Fatalf("sync error: %v", err)
	}
}

func TestDistSync_SyncAll_AllDegraded_Fails(t *testing.T) {
	vol, _ := newTestVolWithMode(t, DurabilitySyncAll)
	defer vol.Close()

	// Create a shipper group with one degraded shipper.
	shipper := NewWALShipper("127.0.0.1:99999", "127.0.0.1:99998", func() uint64 {
		return vol.epoch.Load()
	}, vol.Metrics)
	shipper.degraded.Store(true)
	group := NewShipperGroup([]*WALShipper{shipper})

	fn := MakeDistributedSync(func() error { return nil }, group, vol)
	err := fn()
	if !errors.Is(err, ErrDurabilityBarrierFailed) {
		t.Fatalf("expected ErrDurabilityBarrierFailed, got: %v", err)
	}
}

func TestDistSync_SyncQuorum_AllDegraded_RF3_Fails(t *testing.T) {
	vol, _ := newTestVolWithMode(t, DurabilitySyncQuorum)
	defer vol.Close()

	s1 := NewWALShipper("127.0.0.1:99999", "127.0.0.1:99998", func() uint64 { return 0 }, vol.Metrics)
	s2 := NewWALShipper("127.0.0.1:99997", "127.0.0.1:99996", func() uint64 { return 0 }, vol.Metrics)
	s1.degraded.Store(true)
	s2.degraded.Store(true)
	group := NewShipperGroup([]*WALShipper{s1, s2})

	fn := MakeDistributedSync(func() error { return nil }, group, vol)
	err := fn()
	if !errors.Is(err, ErrDurabilityQuorumLost) {
		t.Fatalf("expected ErrDurabilityQuorumLost, got: %v", err)
	}
}

func TestDistSync_BestEffort_BackwardCompat(t *testing.T) {
	// best_effort with all-degraded group should still succeed (local fsync only).
	vol, _ := newTestVolWithMode(t, DurabilityBestEffort)
	defer vol.Close()

	s1 := NewWALShipper("127.0.0.1:99999", "127.0.0.1:99998", func() uint64 { return 0 }, vol.Metrics)
	s1.degraded.Store(true)
	group := NewShipperGroup([]*WALShipper{s1})

	fn := MakeDistributedSync(func() error { return nil }, group, vol)
	if err := fn(); err != nil {
		t.Fatalf("best_effort all-degraded should succeed: %v", err)
	}
}

func TestDistSync_Metrics_IncrementOnFailure(t *testing.T) {
	vol, _ := newTestVolWithMode(t, DurabilitySyncAll)
	defer vol.Close()

	s1 := NewWALShipper("127.0.0.1:99999", "127.0.0.1:99998", func() uint64 { return 0 }, vol.Metrics)
	s1.degraded.Store(true)
	group := NewShipperGroup([]*WALShipper{s1})

	fn := MakeDistributedSync(func() error { return nil }, group, vol)
	fn() // should fail

	if vol.Metrics.DurabilityBarrierFailedTotal.Load() == 0 {
		t.Error("expected DurabilityBarrierFailedTotal > 0")
	}
}

func TestDistSync_LocalFsyncFail_AlwaysErrors(t *testing.T) {
	// Regardless of mode, local fsync failure always returns error.
	for _, mode := range []DurabilityMode{DurabilityBestEffort, DurabilitySyncAll, DurabilitySyncQuorum} {
		t.Run(mode.String(), func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "test.blk")
			vol, err := CreateBlockVol(path, CreateOptions{
				VolumeSize:     4 * 1024 * 1024,
				DurabilityMode: mode,
			})
			if err != nil {
				t.Fatalf("Create: %v", err)
			}
			defer vol.Close()

			localErr := errors.New("disk I/O error")
			fn := MakeDistributedSync(func() error { return localErr }, nil, vol)
			err = fn()
			if !errors.Is(err, localErr) {
				t.Errorf("expected local error, got: %v", err)
			}
		})
	}
}

// Cleanup helper for tests that create files.
func cleanupFile(t *testing.T, path string) {
	t.Helper()
	os.Remove(path)
}
