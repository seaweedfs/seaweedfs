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
// Phase 12 P2: Soak / Long-Run Stability Hardening
//
// Proves: repeated chosen-path cycles return to bounded truth
// without hidden state drift or unbounded runtime artifacts.
//
// NOT diagnosability, NOT performance-floor, NOT rollout readiness.
// ============================================================

const soakCycles = 5

type soakSetup struct {
	ms    *MasterServer
	bs    *BlockService
	store *storage.BlockVolumeStore
	dir   string
}

func newSoakSetup(t *testing.T) *soakSetup {
	t.Helper()
	dir := t.TempDir()
	store := storage.NewBlockVolumeStore()

	ms := &MasterServer{
		blockRegistry:        NewBlockVolumeRegistry(),
		blockAssignmentQueue: NewBlockAssignmentQueue(),
		blockFailover:        newBlockFailoverState(),
	}
	ms.blockRegistry.MarkBlockCapable("vs1:9333")
	ms.blockRegistry.MarkBlockCapable("vs2:9333")

	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, diskType string, durabilityMode string) (*blockAllocResult, error) {
		sanitized := strings.ReplaceAll(server, ":", "_")
		serverDir := filepath.Join(dir, sanitized)
		os.MkdirAll(serverDir, 0755)
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
		if _, err := store.AddBlockVolume(volPath, ""); err != nil {
			return nil, err
		}
		host := server
		if idx := strings.LastIndex(server, ":"); idx >= 0 {
			host = server[:idx]
		}
		return &blockAllocResult{
			Path:              volPath,
			IQN:               fmt.Sprintf("iqn.2024.test:%s", name),
			ISCSIAddr:         host + ":3260",
			ReplicaDataAddr:   server + ":14260",
			ReplicaCtrlAddr:   server + ":14261",
			RebuildListenAddr: server + ":15000",
		}, nil
	}
	ms.blockVSDelete = func(ctx context.Context, server string, name string) error { return nil }

	bs := &BlockService{
		blockStore:     store,
		blockDir:       filepath.Join(dir, "vs1_9333"),
		listenAddr:     "127.0.0.1:3260",
		localServerID:  "vs1:9333",
		advertisedIP:   "127.0.0.1",
		v2Bridge:       v2bridge.NewControlBridge(),
		v2Orchestrator: engine.NewRecoveryOrchestrator(),
		replStates:     make(map[string]*volReplState),
	}
	bs.v2Recovery = NewRecoveryManager(bs)

	t.Cleanup(func() {
		bs.v2Recovery.Shutdown()
		store.Close()
	})

	return &soakSetup{ms: ms, bs: bs, store: store, dir: dir}
}

func (s *soakSetup) deliver(server string) int {
	pending := s.ms.blockAssignmentQueue.Peek(server)
	if len(pending) == 0 {
		return 0
	}
	protoAssignments := blockvol.AssignmentsToProto(pending)
	goAssignments := blockvol.AssignmentsFromProto(protoAssignments)
	s.bs.ProcessAssignments(goAssignments)
	return len(goAssignments)
}

// --- Repeated create/failover/recover cycles with end-of-cycle truth checks ---

func TestP12P2_RepeatedCycles_NoDrift(t *testing.T) {
	s := newSoakSetup(t)
	ctx := context.Background()

	for cycle := 1; cycle <= soakCycles; cycle++ {
		volName := fmt.Sprintf("soak-vol-%d", cycle)

		// Step 1: Create.
		createResp, err := s.ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
			Name: volName, SizeBytes: 1 << 20,
		})
		if err != nil {
			t.Fatalf("cycle %d create: %v", cycle, err)
		}
		primaryVS := createResp.VolumeServer

		// Deliver initial assignment.
		s.bs.localServerID = primaryVS
		s.deliver(primaryVS)
		time.Sleep(100 * time.Millisecond)

		entry, ok := s.ms.blockRegistry.Lookup(volName)
		if !ok {
			t.Fatalf("cycle %d: volume not in registry", cycle)
		}

		// Step 2: Failover.
		s.ms.blockRegistry.UpdateEntry(volName, func(e *BlockVolumeEntry) {
			e.LastLeaseGrant = time.Now().Add(-1 * time.Minute)
		})
		s.ms.failoverBlockVolumes(primaryVS)

		entryAfter, _ := s.ms.blockRegistry.Lookup(volName)
		if entryAfter.Epoch <= entry.Epoch {
			t.Fatalf("cycle %d: epoch did not increase: %d <= %d", cycle, entryAfter.Epoch, entry.Epoch)
		}

		// Deliver failover assignment.
		newPrimary := entryAfter.VolumeServer
		s.bs.localServerID = newPrimary
		s.deliver(newPrimary)
		time.Sleep(100 * time.Millisecond)

		// Step 3: Reconnect.
		s.ms.recoverBlockVolumes(primaryVS)
		s.bs.localServerID = primaryVS
		s.deliver(primaryVS)
		s.bs.localServerID = newPrimary
		s.deliver(newPrimary)
		time.Sleep(100 * time.Millisecond)

		// === End-of-cycle truth checks ===

		// Registry: volume exists, epoch monotonic.
		finalEntry, ok := s.ms.blockRegistry.Lookup(volName)
		if !ok {
			t.Fatalf("cycle %d: volume missing from registry at end", cycle)
		}
		if finalEntry.Epoch < entryAfter.Epoch {
			t.Fatalf("cycle %d: registry epoch regressed: %d < %d", cycle, finalEntry.Epoch, entryAfter.Epoch)
		}

		// VS-visible: promoted vol epoch matches.
		var volEpoch uint64
		if err := s.store.WithVolume(entryAfter.Path, func(vol *blockvol.BlockVol) error {
			volEpoch = vol.Epoch()
			return nil
		}); err != nil {
			t.Fatalf("cycle %d: promoted vol access failed: %v", cycle, err)
		}
		if volEpoch < entryAfter.Epoch {
			t.Fatalf("cycle %d: vol epoch=%d < registry=%d", cycle, volEpoch, entryAfter.Epoch)
		}

		// Publication: lookup matches registry truth (not just non-empty).
		lookupResp, err := s.ms.LookupBlockVolume(ctx, &master_pb.LookupBlockVolumeRequest{Name: volName})
		if err != nil {
			t.Fatalf("cycle %d: lookup failed: %v", cycle, err)
		}
		if lookupResp.IscsiAddr != finalEntry.ISCSIAddr {
			t.Fatalf("cycle %d: lookup iSCSI=%q != registry=%q", cycle, lookupResp.IscsiAddr, finalEntry.ISCSIAddr)
		}
		if lookupResp.VolumeServer != finalEntry.VolumeServer {
			t.Fatalf("cycle %d: lookup VS=%q != registry=%q", cycle, lookupResp.VolumeServer, finalEntry.VolumeServer)
		}

		t.Logf("cycle %d: registry=%d vol=%d lookup=registry ✓",
			cycle, finalEntry.Epoch, volEpoch)
	}

	t.Logf("P12P2 repeated cycles: %d cycles, all end-of-cycle truth checks passed", soakCycles)
}

// --- Runtime state hygiene: no unbounded leftovers after cycles ---

func TestP12P2_RuntimeHygiene_NoLeftovers(t *testing.T) {
	s := newSoakSetup(t)
	ctx := context.Background()

	// Create and delete several volumes to exercise lifecycle.
	for i := 1; i <= soakCycles; i++ {
		name := fmt.Sprintf("hygiene-vol-%d", i)
		s.ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
			Name: name, SizeBytes: 1 << 20,
		})
		entry, _ := s.ms.blockRegistry.Lookup(name)
		s.bs.localServerID = entry.VolumeServer
		s.deliver(entry.VolumeServer)
		time.Sleep(50 * time.Millisecond)
	}

	// Delete all volumes.
	for i := 1; i <= soakCycles; i++ {
		name := fmt.Sprintf("hygiene-vol-%d", i)
		s.ms.DeleteBlockVolume(ctx, &master_pb.DeleteBlockVolumeRequest{Name: name})
	}

	time.Sleep(200 * time.Millisecond)

	// Check: no stale recovery tasks.
	activeTasks := s.bs.v2Recovery.ActiveTaskCount()
	if activeTasks > 0 {
		t.Fatalf("stale recovery tasks: %d (expected 0 after all volumes deleted)", activeTasks)
	}

	// Check: registry should have no entries for deleted volumes.
	for i := 1; i <= soakCycles; i++ {
		name := fmt.Sprintf("hygiene-vol-%d", i)
		if _, ok := s.ms.blockRegistry.Lookup(name); ok {
			t.Fatalf("stale registry entry: %s (should be deleted)", name)
		}
	}

	// Check: assignment queue should not have unbounded stale entries.
	for _, server := range []string{"vs1:9333", "vs2:9333"} {
		pending := s.ms.blockAssignmentQueue.Peek(server)
		// Some pending entries may exist (lease grants etc), but check for bounded size.
		if len(pending) > soakCycles*2 {
			t.Fatalf("unbounded stale assignments for %s: %d", server, len(pending))
		}
	}

	t.Logf("P12P2 hygiene: %d volumes created+deleted, 0 stale tasks, 0 stale registry, bounded queue", soakCycles)
}

// --- Steady-state repeated delivery: idempotence holds over many cycles ---

func TestP12P2_SteadyState_IdempotenceHolds(t *testing.T) {
	s := newSoakSetup(t)
	ctx := context.Background()

	s.ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name: "steady-vol-1", SizeBytes: 1 << 20,
	})

	entry, _ := s.ms.blockRegistry.Lookup("steady-vol-1")
	s.bs.localServerID = entry.VolumeServer
	s.deliver(entry.VolumeServer)
	time.Sleep(200 * time.Millisecond)

	replicaID := entry.Path + "/" + entry.Replicas[0].Server
	eventsAfterFirst := len(s.bs.v2Orchestrator.Log.EventsFor(replicaID))
	if eventsAfterFirst == 0 {
		t.Fatal("first delivery must create events")
	}

	// Deliver the same assignment many times.
	for i := 0; i < soakCycles*2; i++ {
		s.deliver(entry.VolumeServer)
		time.Sleep(20 * time.Millisecond)
	}

	eventsAfterSoak := len(s.bs.v2Orchestrator.Log.EventsFor(replicaID))
	if eventsAfterSoak != eventsAfterFirst {
		t.Fatalf("idempotence drift: events %d → %d after %d repeated deliveries",
			eventsAfterFirst, eventsAfterSoak, soakCycles*2)
	}

	// Verify: registry, vol, and lookup are still coherent.
	finalEntry, _ := s.ms.blockRegistry.Lookup("steady-vol-1")
	if finalEntry.Epoch != entry.Epoch {
		t.Fatalf("epoch drifted: %d → %d", entry.Epoch, finalEntry.Epoch)
	}

	lookupResp, _ := s.ms.LookupBlockVolume(ctx, &master_pb.LookupBlockVolumeRequest{Name: "steady-vol-1"})
	if lookupResp.IscsiAddr != finalEntry.ISCSIAddr {
		t.Fatalf("lookup iSCSI=%q != registry=%q after soak", lookupResp.IscsiAddr, finalEntry.ISCSIAddr)
	}
	if lookupResp.VolumeServer != finalEntry.VolumeServer {
		t.Fatalf("lookup VS=%q != registry=%q after soak", lookupResp.VolumeServer, finalEntry.VolumeServer)
	}

	t.Logf("P12P2 steady state: %d repeated deliveries, events stable at %d, lookup=registry ✓",
		soakCycles*2, eventsAfterFirst)
}
