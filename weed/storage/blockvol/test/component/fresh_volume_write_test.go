package component

import (
	"bytes"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// TestFreshRF2SyncAll_WriteImmediatelyAfterCreate tests whether a fresh
// RF=2 sync_all volume can accept writes immediately, or if it needs
// the shipper to bootstrap first.
//
// This reproduces the recovery-baseline-failover dd_write failure:
// fio 4K random writes succeed, but dd_write (2MB sequential) fails
// on a fresh volume without wait_volume_healthy.
func TestFreshRF2SyncAll_WriteImmediatelyAfterCreate(t *testing.T) {
	primaryPath := t.TempDir() + "/primary.blk"
	replicaPath := t.TempDir() + "/replica.blk"

	primary, err := blockvol.CreateBlockVol(primaryPath, blockvol.CreateOptions{
		VolumeSize:     64 * 1024 * 1024,
		BlockSize:      4096,
		WALSize:        16 * 1024 * 1024,
		DurabilityMode: blockvol.DurabilitySyncAll,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer primary.Close()

	replica, err := blockvol.CreateBlockVol(replicaPath, blockvol.CreateOptions{
		VolumeSize:     64 * 1024 * 1024,
		BlockSize:      4096,
		WALSize:        16 * 1024 * 1024,
		DurabilityMode: blockvol.DurabilitySyncAll,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer replica.Close()

	// Assign roles.
	primary.HandleAssignment(1, blockvol.RolePrimary, 30*time.Second)
	replica.HandleAssignment(1, blockvol.RoleReplica, 30*time.Second)

	// Start replica receiver.
	if err := replica.StartReplicaReceiver(":0", ":0"); err != nil {
		t.Fatal(err)
	}
	recvAddr := replica.ReplicaReceiverAddr()
	t.Logf("replica receiver: data=%s ctrl=%s", recvAddr.DataAddr, recvAddr.CtrlAddr)

	// Wire shipper.
	primary.SetReplicaAddr(recvAddr.DataAddr, recvAddr.CtrlAddr)

	// Test 1: Immediate 4K write (like fio).
	small := bytes.Repeat([]byte{0xAA}, 4096)
	if err := primary.WriteLBA(0, small); err != nil {
		t.Fatalf("immediate 4K write failed: %v", err)
	}
	t.Log("4K write: OK")

	// Test 2: Immediate 2MB write (like dd_write at offset).
	big := make([]byte, 2*1024*1024)
	for i := range big {
		big[i] = byte(i & 0xFF)
	}
	if err := primary.WriteLBA(1024, big); err != nil {
		t.Fatalf("immediate 2MB write failed: %v", err)
	}
	t.Log("2MB write: OK")

	// Test 3: After short delay, another 2MB write.
	time.Sleep(2 * time.Second)
	if err := primary.WriteLBA(2048, big); err != nil {
		t.Fatalf("delayed 2MB write failed: %v", err)
	}
	t.Log("delayed 2MB write: OK")

	// Test 4: Read back from primary.
	data, err := primary.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("read 4K: %v", err)
	}
	if data[0] != 0xAA {
		t.Fatalf("read mismatch: 0x%02x, want 0xAA", data[0])
	}
	t.Log("readback: OK")
}

// TestFreshRF2SyncAll_WriteDuringSyncAllBarrier tests write behavior
// when the sync_all barrier is active but the shipper hasn't connected yet.
func TestFreshRF2SyncAll_WriteDuringSyncAllBarrier(t *testing.T) {
	path := t.TempDir() + "/primary.blk"
	vol, err := blockvol.CreateBlockVol(path, blockvol.CreateOptions{
		VolumeSize:     64 * 1024 * 1024,
		BlockSize:      4096,
		WALSize:        16 * 1024 * 1024,
		DurabilityMode: blockvol.DurabilitySyncAll,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer vol.Close()

	// Assign as primary with replicas configured but NO receiver started.
	// This simulates the window where the shipper knows about a replica
	// but can't connect yet.
	vol.HandleAssignment(1, blockvol.RolePrimary, 30*time.Second)
	vol.SetReplicaAddr("127.0.0.1:1", "127.0.0.1:2") // dead addresses

	// Write should either succeed (degraded mode) or fail with a clear error.
	data := bytes.Repeat([]byte{0xBB}, 4096)
	err = vol.WriteLBA(0, data)
	if err != nil {
		t.Logf("write with dead replica: %v (sync_all barrier may block)", err)
		// This is the expected behavior — sync_all with unreachable replica
		// should return an error, not hang.
	} else {
		t.Log("write with dead replica succeeded — barrier must have degraded")
	}
}
