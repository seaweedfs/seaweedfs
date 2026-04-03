package component

import (
	"bytes"
	"net"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// TestReplicaReadAfterShip verifies that data shipped from primary to replica
// via WAL replication is readable on the replica via ReadLBA.
//
// This reproduces the CP13-8 bug: replica iSCSI reads zeros despite
// replicated data in WAL (sync_all barrier confirmed).
func TestReplicaReadAfterShip(t *testing.T) {
	primaryPath := t.TempDir() + "/primary.blk"
	replicaPath := t.TempDir() + "/replica.blk"

	primary, err := blockvol.CreateBlockVol(primaryPath, blockvol.CreateOptions{
		VolumeSize: 4 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    1 * 1024 * 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer primary.Close()

	replica, err := blockvol.CreateBlockVol(replicaPath, blockvol.CreateOptions{
		VolumeSize: 4 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    1 * 1024 * 1024,
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
	if recvAddr == nil {
		t.Fatal("replica receiver not started")
	}
	t.Logf("replica receiver: data=%s ctrl=%s", recvAddr.DataAddr, recvAddr.CtrlAddr)

	// Wire shipper from primary to replica.
	primary.SetReplicaAddr(recvAddr.DataAddr, recvAddr.CtrlAddr)

	// Write on primary — should ship to replica.
	writeData := bytes.Repeat([]byte{0xAB}, 4096)
	if err := primary.WriteLBA(0, writeData); err != nil {
		t.Fatalf("primary WriteLBA(0): %v", err)
	}

	// Give shipping + apply time.
	time.Sleep(2 * time.Second)

	// Read from REPLICA.
	replicaData, err := replica.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("replica ReadLBA(0): %v", err)
	}

	if replicaData[0] == 0x00 {
		t.Fatalf("BUG REPRODUCED: replica ReadLBA returns zeros (first byte=0x%02x, want 0xAB)"+
			"\nData is in replica WAL but ReadLBA returns zeros", replicaData[0])
	}
	if !bytes.Equal(replicaData, writeData) {
		t.Fatalf("replica data mismatch: first byte=0x%02x, want 0xAB", replicaData[0])
	}
	t.Log("replica ReadLBA after ship: OK (data matches primary)")
}

// TestReplicaReadDirectApply bypasses the shipper entirely and manually
// ships a WAL entry via TCP to the replica receiver, then reads it back.
func TestReplicaReadDirectApply(t *testing.T) {
	replicaPath := t.TempDir() + "/replica.blk"
	vol, err := blockvol.CreateBlockVol(replicaPath, blockvol.CreateOptions{
		VolumeSize: 4 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    1 * 1024 * 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer vol.Close()

	vol.HandleAssignment(1, blockvol.RoleReplica, 30*time.Second)

	if err := vol.StartReplicaReceiver(":0", ":0"); err != nil {
		t.Fatal(err)
	}
	recvAddr := vol.ReplicaReceiverAddr()
	t.Logf("replica: data=%s ctrl=%s", recvAddr.DataAddr, recvAddr.CtrlAddr)

	// Directly connect and ship a WAL entry.
	conn, err := net.DialTimeout("tcp", recvAddr.DataAddr, 3*time.Second)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	payload := bytes.Repeat([]byte{0xEF}, 4096)
	entry := blockvol.WALEntry{
		LSN:    1,
		Epoch:  1,
		Type:   blockvol.EntryTypeWrite,
		LBA:    0,
		Length: 4096,
		Data:   payload,
	}
	encoded, err := entry.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if err := blockvol.WriteFrame(conn, blockvol.MsgWALEntry, encoded); err != nil {
		t.Fatalf("ship: %v", err)
	}

	time.Sleep(1 * time.Second)

	// Read back via ReadLBA.
	data, err := vol.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA: %v", err)
	}

	if data[0] == 0x00 {
		t.Fatalf("BUG: ReadLBA returns zeros after direct WAL apply (0x%02x, want 0xEF)", data[0])
	}
	if data[0] != 0xEF {
		t.Fatalf("unexpected data: 0x%02x, want 0xEF", data[0])
	}
	t.Logf("direct apply ReadLBA: OK (0x%02x)", data[0])

	// Also read via adapter (same path as iSCSI).
	adapter := blockvol.NewBlockVolAdapter(vol)
	adapterData, err := adapter.ReadAt(0, 4096)
	if err != nil {
		t.Fatalf("adapter ReadAt: %v", err)
	}
	if adapterData[0] != 0xEF {
		t.Fatalf("adapter returns wrong data: 0x%02x, want 0xEF", adapterData[0])
	}
	t.Log("adapter ReadAt: OK")
}
