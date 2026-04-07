package weed_server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	engine "github.com/seaweedfs/seaweedfs/sw-block/engine/replication"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/v2bridge"
)

// ============================================================
// Phase 10 P4: Master-driven control-loop closure
//
// These tests use REAL master assignment production (CreateBlockVolume,
// failoverBlockVolumes) with the allocator wired to produce paths that
// match REAL registered block volumes in the test BlockService.
// ============================================================

// p4Setup creates a master + BlockService where the master's allocator
// produces paths matching real volumes registered in the BlockService's store.
type p4Setup struct {
	ms      *MasterServer
	bs      *BlockService
	store   *storage.BlockVolumeStore
	dir     string
	volSeq  int
}

func newP4Setup(t *testing.T) *p4Setup {
	t.Helper()
	dir := t.TempDir()
	store := storage.NewBlockVolumeStore()

	bs := &BlockService{
		blockStore:     store,
		blockDir:       dir,
		listenAddr:     "127.0.0.1:3260",
		localServerID:  "vs1:9333",
		v2Bridge:       v2bridge.NewControlBridge(),
		v2Orchestrator: engine.NewRecoveryOrchestrator(),
		replStates:     make(map[string]*volReplState),
	}
	bs.v2Recovery = NewRecoveryManager(bs)

	ms := &MasterServer{
		blockRegistry:        NewBlockVolumeRegistry(),
		blockAssignmentQueue: NewBlockAssignmentQueue(),
		blockFailover:        newBlockFailoverState(),
	}
	ms.blockRegistry.MarkBlockCapable("vs1:9333")
	ms.blockRegistry.MarkBlockCapable("vs2:9333")

	setup := &p4Setup{ms: ms, bs: bs, store: store, dir: dir}

	// Wire allocator to create REAL block volumes at per-server paths.
	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, walSizeBytes uint64, diskType string, durabilityMode string) (*blockAllocResult, error) {
		// Per-server subdir (sanitize colons for Windows).
		sanitized := strings.ReplaceAll(server, ":", "_")
		serverDir := filepath.Join(dir, sanitized)
		if err := os.MkdirAll(serverDir, 0755); err != nil {
			return nil, err
		}
		volPath := filepath.Join(serverDir, fmt.Sprintf("%s.blk", name))
		vol, err := blockvol.CreateBlockVol(volPath, blockvol.CreateOptions{
			VolumeSize: 1 * 1024 * 1024,
			BlockSize:  4096,
			WALSize:    256 * 1024,
		})
		if err != nil {
			return nil, err
		}
		vol.Close()
		// Register ALL volumes in the shared store (single-process test
		// simulates both primary and promoted-replica VS).
		if _, err := store.AddBlockVolume(volPath, ""); err != nil {
			return nil, err
		}
		return &blockAllocResult{
			Path:              volPath,
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

	t.Cleanup(func() {
		bs.v2Recovery.Shutdown()
		store.Close()
	})

	return setup
}

// deliverAssignments simulates heartbeat delivery: queue → proto → decode → ProcessAssignments.
func (s *p4Setup) deliverAssignments(server string) int {
	pending := s.ms.blockAssignmentQueue.Peek(server)
	if len(pending) == 0 {
		return 0
	}
	protoAssignments := blockvol.AssignmentsToProto(pending)
	goAssignments := blockvol.AssignmentsFromProto(protoAssignments)
	s.bs.ProcessAssignments(goAssignments)
	return len(goAssignments)
}

// --- 1. Real master create → full delivery with real volume ---

func TestP10P4_MasterCreate_FullDelivery(t *testing.T) {
	s := newP4Setup(t)
	ctx := context.Background()

	createResp, err := s.ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name:      "pvc-data-1",
		SizeBytes: 1 << 30,
	})
	if err != nil {
		t.Fatalf("CreateBlockVolume: %v", err)
	}
	primaryVS := createResp.VolumeServer
	t.Logf("master created: primary=%s replica=%s", primaryVS, createResp.ReplicaServer)

	// Deliver master-produced assignments.
	n := s.deliverAssignments(primaryVS)
	if n == 0 {
		t.Fatal("no assignments delivered")
	}
	time.Sleep(200 * time.Millisecond)

	// Verify: engine has sender with stable ReplicaID.
	entry, _ := s.ms.blockRegistry.Lookup("pvc-data-1")
	expectedID := entry.Path + "/" + entry.Replicas[0].Server
	sender := s.bs.v2Orchestrator.Registry.Sender(expectedID)
	if sender == nil {
		t.Fatalf("sender not found: %s", expectedID)
	}

	// Verify: V1 HandleAssignment succeeded (vol exists in store).
	var volEpoch uint64
	if err := s.store.WithVolume(entry.Path, func(vol *blockvol.BlockVol) error {
		volEpoch = vol.Epoch()
		return nil
	}); err != nil {
		t.Fatalf("volume not accessible in store: %v", err)
	}
	if volEpoch == 0 {
		t.Fatal("vol epoch should be set after HandleAssignment")
	}

	// Verify: heartbeat reports the assigned volume.
	msgs := s.bs.CollectBlockVolumeHeartbeat()
	found := false
	for _, m := range msgs {
		if m.Path == entry.Path {
			found = true
			if m.Epoch != entry.Epoch {
				t.Fatalf("heartbeat epoch=%d, want %d", m.Epoch, entry.Epoch)
			}
		}
	}
	if !found {
		t.Fatalf("assigned volume %s not in heartbeat", entry.Path)
	}

	t.Logf("P10P4 create: master → queue → proto → VS → sender(%s) + vol(epoch=%d) + heartbeat", expectedID, volEpoch)
}

// --- 2. Real master failover → delivery → convergence ---

func TestP10P4_MasterFailover_Convergence(t *testing.T) {
	s := newP4Setup(t)
	ctx := context.Background()

	createResp, err := s.ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name:      "pvc-data-2",
		SizeBytes: 1 << 30,
	})
	if err != nil {
		t.Fatalf("CreateBlockVolume: %v", err)
	}
	primaryVS := createResp.VolumeServer
	replicaVS := createResp.ReplicaServer
	t.Logf("created: primary=%s replica=%s", primaryVS, replicaVS)

	// Deliver initial assignment to primary BS.
	n1 := s.deliverAssignments(primaryVS)
	if n1 == 0 {
		t.Fatal("no initial assignments delivered")
	}
	time.Sleep(200 * time.Millisecond)

	entry, _ := s.ms.blockRegistry.Lookup("pvc-data-2")

	// Expire lease, then failover.
	s.ms.blockRegistry.UpdateEntry("pvc-data-2", func(e *BlockVolumeEntry) {
		e.LastLeaseGrant = time.Now().Add(-1 * time.Minute)
	})
	s.ms.failoverBlockVolumes(primaryVS)

	entryAfter, _ := s.ms.blockRegistry.Lookup("pvc-data-2")
	if entryAfter.Epoch != 2 {
		t.Fatalf("epoch=%d, want 2", entryAfter.Epoch)
	}
	newPrimary := entryAfter.VolumeServer
	t.Logf("after failover: new primary=%s epoch=%d", newPrimary, entryAfter.Epoch)

	// Build a SEPARATE BlockService for the promoted replica VS.
	// This simulates the promoted VS having its own store and heartbeat.
	replicaStore := storage.NewBlockVolumeStore()
	// Find and register the replica's vol path.
	sanitized := strings.ReplaceAll(replicaVS, ":", "_")
	replicaVolPath := filepath.Join(s.dir, sanitized, "pvc-data-2.blk")
	if _, err := replicaStore.AddBlockVolume(replicaVolPath, ""); err != nil {
		t.Fatalf("register replica vol: %v", err)
	}
	promotedBS := &BlockService{
		blockStore:     replicaStore,
		blockDir:       s.dir,
		listenAddr:     "127.0.0.1:3261",
		localServerID:  newPrimary,
		v2Bridge:       v2bridge.NewControlBridge(),
		v2Orchestrator: engine.NewRecoveryOrchestrator(),
		replStates:     make(map[string]*volReplState),
	}
	promotedBS.v2Recovery = NewRecoveryManager(promotedBS)
	t.Cleanup(func() {
		promotedBS.v2Recovery.Shutdown()
		replicaStore.Close()
	})

	// Deliver failover assignment to the promoted BS through proto path.
	failoverPending := s.ms.blockAssignmentQueue.Peek(newPrimary)
	if len(failoverPending) == 0 {
		t.Fatal("no failover assignment in queue")
	}
	fa := failoverPending[0]
	if fa.Epoch != 2 {
		t.Fatalf("failover assignment epoch=%d, want 2", fa.Epoch)
	}

	protoAssignments := blockvol.AssignmentsToProto(failoverPending)
	goAssignments := blockvol.AssignmentsFromProto(protoAssignments)
	promotedBS.ProcessAssignments(goAssignments)
	time.Sleep(200 * time.Millisecond)

	// Verify: HandleAssignment succeeded on the promoted vol (epoch=2).
	var promotedEpoch uint64
	if err := replicaStore.WithVolume(fa.Path, func(vol *blockvol.BlockVol) error {
		promotedEpoch = vol.Epoch()
		return nil
	}); err != nil {
		t.Fatalf("promoted vol not accessible: %v", err)
	}
	if promotedEpoch != 2 {
		t.Fatalf("promoted vol epoch=%d, want 2", promotedEpoch)
	}

	// Verify: promoted VS's OWN heartbeat reports the vol at epoch 2.
	// This is the promoted VS's independent heartbeat, not the shared store.
	promotedMsgs := promotedBS.CollectBlockVolumeHeartbeat()
	foundPromoted := false
	for _, m := range promotedMsgs {
		if m.Path == fa.Path && m.Epoch == 2 {
			foundPromoted = true
		}
	}
	if !foundPromoted {
		t.Fatal("promoted VS heartbeat should show vol at epoch 2")
	}

	// Verify: promoted VS heartbeat does NOT contain the old primary's vol.
	_ = entry // reference to initial entry
	for _, m := range promotedMsgs {
		if m.Path == entry.Path && m.Path != fa.Path {
			t.Fatalf("promoted VS heartbeat should NOT contain old primary vol %s", entry.Path)
		}
	}

	t.Logf("P10P4 failover: create → failover(epoch=2) → separate promoted BS → vol(epoch=2) + own heartbeat")
}

// --- 3. Identity preservation through master-produced path ---

func TestP10P4_IdentityPreservation(t *testing.T) {
	s := newP4Setup(t)
	ctx := context.Background()

	_, err := s.ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name:      "pvc-data-3",
		SizeBytes: 1 << 30,
	})
	if err != nil {
		t.Fatalf("CreateBlockVolume: %v", err)
	}

	entry, _ := s.ms.blockRegistry.Lookup("pvc-data-3")

	// Verify master-produced assignment carries stable ServerID.
	pending := s.ms.blockAssignmentQueue.Peek(entry.VolumeServer)
	hasStableID := false
	for _, a := range pending {
		if a.ReplicaServerID != "" {
			hasStableID = true
		}
		for _, ra := range a.ReplicaAddrs {
			if ra.ServerID != "" {
				hasStableID = true
			}
		}
	}
	if !hasStableID {
		t.Fatal("master assignment missing stable ServerID")
	}

	// Deliver and verify engine uses stable ID, not address.
	s.deliverAssignments(entry.VolumeServer)
	time.Sleep(200 * time.Millisecond)

	for _, ri := range entry.Replicas {
		stableID := entry.Path + "/" + ri.Server
		addressID := entry.Path + "/" + ri.DataAddr
		if s.bs.v2Orchestrator.Registry.Sender(stableID) == nil {
			t.Fatalf("stable sender %s not found", stableID)
		}
		if s.bs.v2Orchestrator.Registry.Sender(addressID) != nil {
			t.Fatalf("address-derived sender %s should NOT exist", addressID)
		}
	}

	t.Log("P10P4 identity: master-produced stable ID preserved through full delivery")
}

// --- 4. Repeated delivery idempotence ---

func TestP10P4_RepeatedDelivery_Idempotence(t *testing.T) {
	s := newP4Setup(t)
	ctx := context.Background()

	_, err := s.ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name:      "pvc-data-4",
		SizeBytes: 1 << 30,
	})
	if err != nil {
		t.Fatalf("CreateBlockVolume: %v", err)
	}

	entry, _ := s.ms.blockRegistry.Lookup("pvc-data-4")
	primaryVS := entry.VolumeServer
	replicaID := entry.Path + "/" + entry.Replicas[0].Server

	// First delivery.
	s.deliverAssignments(primaryVS)
	time.Sleep(200 * time.Millisecond)

	eventsAfterFirst := len(s.bs.v2Orchestrator.Log.EventsFor(replicaID))
	if eventsAfterFirst == 0 {
		t.Fatal("first delivery must create engine events (guard against vacuous pass)")
	}
	t.Logf("first delivery: %d events", eventsAfterFirst)

	// Second delivery (Peek returns same assignments).
	s.deliverAssignments(primaryVS)
	time.Sleep(100 * time.Millisecond)

	eventsAfterSecond := len(s.bs.v2Orchestrator.Log.EventsFor(replicaID))
	if eventsAfterSecond != eventsAfterFirst {
		t.Fatalf("idempotence broken: events %d → %d", eventsAfterFirst, eventsAfterSecond)
	}

	t.Logf("P10P4 idempotent: repeated delivery → events stable at %d (guard: >0)", eventsAfterFirst)
}
