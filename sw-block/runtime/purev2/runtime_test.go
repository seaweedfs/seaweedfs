package purev2

import (
	"bytes"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	engine "github.com/seaweedfs/seaweedfs/sw-block/engine/replication"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

func TestRuntime_RF1BootstrapCreateAssignSnapshot(t *testing.T) {
	tempDir := t.TempDir()
	rt := New(Config{})
	defer rt.Close()

	path := filepath.Join(tempDir, "rf1-primary.blk")
	if err := rt.BootstrapPrimary(path, testCreateOptions(), 1, 30*time.Second); err != nil {
		t.Fatalf("bootstrap primary: %v", err)
	}

	snap, err := rt.Snapshot(path)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if snap.Status.Role != blockvol.RolePrimary {
		t.Fatalf("role=%v", snap.Status.Role)
	}
	if snap.Status.Epoch != 1 {
		t.Fatalf("epoch=%d", snap.Status.Epoch)
	}
	if !snap.HasProjection {
		t.Fatal("expected published projection")
	}
	if snap.Projection.Role != engine.RolePrimary {
		t.Fatalf("projection role=%v", snap.Projection.Role)
	}
	if snap.Projection.Mode.Name != engine.ModeAllocatedOnly {
		t.Fatalf("projection mode=%s", snap.Projection.Mode.Name)
	}
	if snap.Projection.Publication.Reason != "allocated_only" {
		t.Fatalf("publication reason=%q", snap.Projection.Publication.Reason)
	}
	if !snap.Projection.Readiness.RoleApplied {
		t.Fatal("role_applied should be true after apply_role")
	}
	if !reflect.DeepEqual(snap.ExecutedCommands, []string{"apply_role"}) {
		t.Fatalf("executed=%v", snap.ExecutedCommands)
	}
}

func TestRuntime_RF1RestartPreservesLocalData(t *testing.T) {
	tempDir := t.TempDir()
	path := filepath.Join(tempDir, "rf1-restart.blk")
	payload := bytes.Repeat([]byte{0x5A}, 4096)

	func() {
		rt := New(Config{})
		defer rt.Close()

		if err := rt.BootstrapPrimary(path, testCreateOptions(), 1, 30*time.Second); err != nil {
			t.Fatalf("bootstrap primary: %v", err)
		}
		if err := rt.WriteLBA(path, 0, payload); err != nil {
			t.Fatalf("write: %v", err)
		}
		if err := rt.SyncCache(path); err != nil {
			t.Fatalf("sync cache: %v", err)
		}
	}()

	rt := New(Config{})
	defer rt.Close()

	if err := rt.OpenVolume(path); err != nil {
		t.Fatalf("open volume: %v", err)
	}
	readBack, err := rt.ReadLBA(path, 0, uint32(len(payload)))
	if err != nil {
		t.Fatalf("read after restart: %v", err)
	}
	if !bytes.Equal(readBack, payload) {
		t.Fatal("readback mismatch after restart")
	}

	if err := rt.ApplyPrimaryAssignment(path, 1, 30*time.Second); err != nil {
		t.Fatalf("re-apply primary: %v", err)
	}
	snap, err := rt.Snapshot(path)
	if err != nil {
		t.Fatalf("snapshot after reopen: %v", err)
	}
	if !snap.HasCoreState {
		t.Fatal("expected core state after re-apply")
	}
	if snap.CoreState.Readiness.RoleApplied != true {
		t.Fatal("role_applied should remain true after reopen assignment")
	}
}

func TestRuntime_LocalBoundaryObservationsAdvanceCoreState(t *testing.T) {
	tempDir := t.TempDir()
	rt := New(Config{})
	defer rt.Close()

	path := filepath.Join(tempDir, "rf1-boundaries.blk")
	if err := rt.BootstrapPrimary(path, testCreateOptions(), 1, 30*time.Second); err != nil {
		t.Fatalf("bootstrap primary: %v", err)
	}

	payload := bytes.Repeat([]byte{0x33}, 4096)
	if err := rt.WriteLBA(path, 0, payload); err != nil {
		t.Fatalf("write: %v", err)
	}

	snap, err := rt.Snapshot(path)
	if err != nil {
		t.Fatalf("snapshot after write: %v", err)
	}
	if !snap.HasCoreState {
		t.Fatal("expected core state after write")
	}
	if snap.CoreState.Boundary.CommittedLSN == 0 {
		t.Fatalf("committed_lsn=%d, want > 0", snap.CoreState.Boundary.CommittedLSN)
	}
	if snap.CoreState.Boundary.DurableLSN != 0 {
		t.Fatalf("durable_lsn=%d before sync, want 0", snap.CoreState.Boundary.DurableLSN)
	}

	if err := rt.SyncCache(path); err != nil {
		t.Fatalf("sync cache: %v", err)
	}

	snap, err = rt.Snapshot(path)
	if err != nil {
		t.Fatalf("snapshot after sync: %v", err)
	}
	if snap.CoreState.Boundary.DurableLSN < snap.CoreState.Boundary.CommittedLSN {
		t.Fatalf("durable_lsn=%d committed_lsn=%d", snap.CoreState.Boundary.DurableLSN, snap.CoreState.Boundary.CommittedLSN)
	}
	if snap.CoreState.Boundary.CheckpointLSN > snap.CoreState.Boundary.DurableLSN {
		t.Fatalf("checkpoint_lsn=%d durable_lsn=%d", snap.CoreState.Boundary.CheckpointLSN, snap.CoreState.Boundary.DurableLSN)
	}
}

func testCreateOptions() blockvol.CreateOptions {
	return blockvol.CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    256 * 1024,
	}
}
