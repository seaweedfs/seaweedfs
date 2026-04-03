package weed_server

import (
	"bytes"
	"context"
"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// TestCP13_8A_ReplicaReadAfterReplication verifies that data replicated to
// a replica volume can be read back through the same BlockVol instance that
// the iSCSI adapter uses. This is the core CP13-8A investigation test.
//
// If this passes: the engine + weed integration is correct, and the CP13-8
// scenario failure is in the testrunner/cluster layer.
// If this fails: the bug is in the VS integration (adapter wiring, flusher, etc).
func TestCP13_8A_ReplicaReadAfterReplication(t *testing.T) {
	s := newSoakSetup(t)
	ctx := context.Background()

	// Create a volume (RF=2 sync_all).
	resp, err := s.ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name:           "cp13-8a-read",
		SizeBytes:      1 << 20,
		DurabilityMode: "sync_all",
	})
	if err != nil {
		t.Fatal(err)
	}
	primaryVS := resp.VolumeServer
	entry, _ := s.ms.blockRegistry.Lookup("cp13-8a-read")

	// Deliver assignments.
	s.bs.localServerID = primaryVS
	s.deliver(primaryVS)
	for _, ri := range entry.Replicas {
		s.deliver(ri.Server)
	}
	time.Sleep(200 * time.Millisecond)

	// Write data through the primary's BlockVol (simulating iSCSI write).
	primaryPath := entry.Path
	var primaryVol *blockvol.BlockVol
	s.store.IterateBlockVolumes(func(path string, vol *blockvol.BlockVol) {
		if path == primaryPath {
			primaryVol = vol
		}
	})
	if primaryVol == nil {
		t.Fatal("primary volume not found in store")
	}

	// Write test pattern.
	testData := make([]byte, 4096)
	for i := range testData {
		testData[i] = byte(i % 251) // distinctive non-zero pattern
	}
	if err := primaryVol.WriteLBA(0, testData); err != nil {
		t.Fatalf("WriteLBA on primary: %v", err)
	}
	if err := primaryVol.WriteLBA(1, testData); err != nil {
		t.Fatalf("WriteLBA on primary LBA 1: %v", err)
	}

	// SyncCache to trigger sync_all barrier (replicates + confirms durability).
	if err := primaryVol.SyncCache(); err != nil {
		t.Fatalf("SyncCache: %v", err)
	}

	// Now find the REPLICA volume and read from it.
	// In the soak setup, both primary and replica are in the same store.
	var replicaPath string
	for _, ri := range entry.Replicas {
		replicaPath = ri.Path
		break
	}
	if replicaPath == "" {
		t.Fatal("no replica path found")
	}

	var replicaVol *blockvol.BlockVol
	s.store.IterateBlockVolumes(func(path string, vol *blockvol.BlockVol) {
		if path == replicaPath {
			replicaVol = vol
		}
	})
	if replicaVol == nil {
		t.Fatal("replica volume not found in store")
	}

	// Let flusher run on replica (same as production).
	time.Sleep(200 * time.Millisecond)

	// Read from replica — this is what the iSCSI adapter would do.
	got, err := replicaVol.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA on replica: %v", err)
	}

	if bytes.Equal(got, make([]byte, 4096)) {
		t.Fatal("CP13-8A: replica ReadLBA returned all zeros — data not visible")
	}

	if !bytes.Equal(got, testData) {
		t.Fatalf("CP13-8A: replica data mismatch at LBA 0: got[0]=%d want[0]=%d", got[0], testData[0])
	}

	// Read LBA 1 too.
	got1, err := replicaVol.ReadLBA(1, 4096)
	if err != nil {
		t.Fatalf("ReadLBA on replica LBA 1: %v", err)
	}
	if !bytes.Equal(got1, testData) {
		t.Fatalf("CP13-8A: replica data mismatch at LBA 1")
	}

	// Also test through the adapter path (what iSCSI actually calls).
	adapter := blockvol.NewBlockVolAdapter(replicaVol)
	adapterGot, err := adapter.ReadAt(0, 4096)
	if err != nil {
		t.Fatalf("adapter ReadAt: %v", err)
	}
	if !bytes.Equal(adapterGot, testData) {
		t.Fatalf("CP13-8A: adapter data mismatch at LBA 0")
	}

	t.Log("CP13-8A: replica reads return correct data through both ReadLBA and adapter.ReadAt")
}

// TestCP13_8A_ReplicaReadAfterPromotion verifies that after promoting a
// replica to primary, the data is still readable.
func TestCP13_8A_ReplicaReadAfterPromotion(t *testing.T) {
	s := newSoakSetup(t)
	ctx := context.Background()

	resp, err := s.ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name:           "cp13-8a-promote",
		SizeBytes:      1 << 20,
		DurabilityMode: "sync_all",
	})
	if err != nil {
		t.Fatal(err)
	}
	primaryVS := resp.VolumeServer
	entry, _ := s.ms.blockRegistry.Lookup("cp13-8a-promote")

	s.bs.localServerID = primaryVS
	s.deliver(primaryVS)
	for _, ri := range entry.Replicas {
		s.deliver(ri.Server)
	}
	time.Sleep(200 * time.Millisecond)

	// Write through primary.
	var primaryVol *blockvol.BlockVol
	s.store.IterateBlockVolumes(func(path string, vol *blockvol.BlockVol) {
		if path == entry.Path {
			primaryVol = vol
		}
	})
	testData := make([]byte, 4096)
	for i := range testData {
		testData[i] = byte((i + 37) % 251)
	}
	if err := primaryVol.WriteLBA(0, testData); err != nil {
		t.Fatal(err)
	}
	if err := primaryVol.SyncCache(); err != nil {
		t.Fatal(err)
	}

	// Find replica.
	var replicaPath string
	for _, ri := range entry.Replicas {
		replicaPath = ri.Path
	}
	var replicaVol *blockvol.BlockVol
	s.store.IterateBlockVolumes(func(path string, vol *blockvol.BlockVol) {
		if path == replicaPath {
			replicaVol = vol
		}
	})

	// Promote replica to primary.
	if err := replicaVol.HandleAssignment(2, blockvol.RolePrimary, 30*time.Second); err != nil {
		t.Fatalf("promote replica: %v", err)
	}

	// Read after promotion (let flusher run).
	time.Sleep(200 * time.Millisecond)
	got, err := replicaVol.ReadLBA(0, 4096)
	if err != nil {
		t.Fatalf("ReadLBA after promotion: %v", err)
	}
	if bytes.Equal(got, make([]byte, 4096)) {
		t.Fatal("CP13-8A: promoted replica reads zeros after promotion")
	}
	if !bytes.Equal(got, testData) {
		t.Fatalf("CP13-8A: promoted replica data mismatch")
	}

	t.Log("CP13-8A: promoted replica reads correct data")
}
