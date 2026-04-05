package volumev2

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/sw-block/runtime/masterv2"
	"github.com/seaweedfs/seaweedfs/sw-block/runtime/protocolv2"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/iscsi"
)

func TestPOC_MasterV2VolumeV2_RF1HeartbeatAssignmentFlow(t *testing.T) {
	master := masterv2.New(masterv2.Config{})
	node, err := New(Config{NodeID: "node-a"})
	if err != nil {
		t.Fatalf("new volumev2 node: %v", err)
	}
	defer node.Close()
	session, err := NewInProcessSession(master)
	if err != nil {
		t.Fatalf("new session: %v", err)
	}
	orchestrator, err := NewOrchestrator(node, session)
	if err != nil {
		t.Fatalf("new orchestrator: %v", err)
	}

	path := filepath.Join(t.TempDir(), "rf1-poc.blk")
	if err := master.DeclarePrimary(masterv2.VolumeSpec{
		Name:          "vol-a",
		Path:          path,
		PrimaryNodeID: "node-a",
		CreateOptions: testCreateOptions(),
	}); err != nil {
		t.Fatalf("declare primary: %v", err)
	}

	assignments, err := session.Sync(mustHeartbeat(t, node))
	if err != nil {
		t.Fatalf("preview sync: %v", err)
	}
	if len(assignments) != 1 {
		t.Fatalf("preview assignments=%d, want 1", len(assignments))
	}
	if assignments[0].Epoch != 1 {
		t.Fatalf("epoch=%d, want 1", assignments[0].Epoch)
	}
	if err := orchestrator.SyncOnce(); err != nil {
		t.Fatalf("sync once 1: %v", err)
	}
	if err := orchestrator.SyncOnce(); err != nil {
		t.Fatalf("sync once 2: %v", err)
	}

	view, ok := master.Volume("vol-a")
	if !ok {
		t.Fatal("master view missing")
	}
	if view.ObservedRole != "primary" {
		t.Fatalf("observed role=%q", view.ObservedRole)
	}
	if !view.RoleApplied {
		t.Fatal("role_applied should be true")
	}
	if view.Mode != "allocated_only" {
		t.Fatalf("mode=%q", view.Mode)
	}

	payload := bytes.Repeat([]byte{0x4A}, 4096)
	if err := node.WriteLBA("vol-a", 0, payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := node.SyncCache("vol-a"); err != nil {
		t.Fatalf("sync cache: %v", err)
	}
	readBack, err := node.ReadLBA("vol-a", 0, uint32(len(payload)))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(readBack, payload) {
		t.Fatal("payload mismatch")
	}
}

func TestPOC_MasterV2VolumeV2_ReissuesAssignmentAfterDesiredPathChange(t *testing.T) {
	master := masterv2.New(masterv2.Config{})
	node, err := New(Config{NodeID: "node-a"})
	if err != nil {
		t.Fatalf("new volumev2 node: %v", err)
	}
	defer node.Close()

	tempDir := t.TempDir()
	path1 := filepath.Join(tempDir, "vol-a-1.blk")
	path2 := filepath.Join(tempDir, "vol-a-2.blk")

	if err := master.DeclarePrimary(masterv2.VolumeSpec{
		Name:          "vol-a",
		Path:          path1,
		PrimaryNodeID: "node-a",
		CreateOptions: testCreateOptions(),
	}); err != nil {
		t.Fatalf("declare primary path1: %v", err)
	}

	assignments, err := master.HandleHeartbeat(mustHeartbeat(t, node))
	if err != nil {
		t.Fatalf("handle heartbeat 0: %v", err)
	}
	if err := node.ApplyAssignments(assignments); err != nil {
		t.Fatalf("apply assignments path1: %v", err)
	}

	if _, err := master.HandleHeartbeat(mustHeartbeat(t, node)); err != nil {
		t.Fatalf("handle heartbeat after path1: %v", err)
	}

	if err := master.DeclarePrimary(masterv2.VolumeSpec{
		Name:          "vol-a",
		Path:          path2,
		PrimaryNodeID: "node-a",
		CreateOptions: testCreateOptions(),
	}); err != nil {
		t.Fatalf("declare primary path2: %v", err)
	}

	assignments, err = master.HandleHeartbeat(mustHeartbeat(t, node))
	if err != nil {
		t.Fatalf("handle heartbeat for path change: %v", err)
	}
	if len(assignments) != 1 {
		t.Fatalf("assignments=%d, want 1", len(assignments))
	}
	if assignments[0].Epoch != 2 {
		t.Fatalf("epoch=%d, want 2", assignments[0].Epoch)
	}
	if assignments[0].Path != path2 {
		t.Fatalf("path=%q, want %q", assignments[0].Path, path2)
	}

	if err := node.ApplyAssignments(assignments); err != nil {
		t.Fatalf("apply assignments path2: %v", err)
	}
	assignments, err = master.HandleHeartbeat(mustHeartbeat(t, node))
	if err != nil {
		t.Fatalf("handle heartbeat after path2: %v", err)
	}
	if len(assignments) != 0 {
		t.Fatalf("unexpected reissue count=%d", len(assignments))
	}

	view, ok := master.Volume("vol-a")
	if !ok {
		t.Fatal("master view missing")
	}
	if view.Path != path2 {
		t.Fatalf("view path=%q, want %q", view.Path, path2)
	}
	if view.ObservedEpoch != 2 {
		t.Fatalf("observed epoch=%d, want 2", view.ObservedEpoch)
	}
}

func TestPOC_MasterV2VolumeV2_OrchestratorSyncsSingleNodeMVP(t *testing.T) {
	master := masterv2.New(masterv2.Config{})
	node, err := New(Config{NodeID: "node-a"})
	if err != nil {
		t.Fatalf("new volumev2 node: %v", err)
	}
	defer node.Close()
	session, err := NewInProcessSession(master)
	if err != nil {
		t.Fatalf("new session: %v", err)
	}
	orchestrator, err := NewOrchestrator(node, session)
	if err != nil {
		t.Fatalf("new orchestrator: %v", err)
	}

	path := filepath.Join(t.TempDir(), "vol-orch.blk")
	if err := master.DeclarePrimary(masterv2.VolumeSpec{
		Name:          "vol-orch",
		Path:          path,
		PrimaryNodeID: "node-a",
		CreateOptions: testCreateOptions(),
	}); err != nil {
		t.Fatalf("declare primary: %v", err)
	}

	if err := orchestrator.SyncOnce(); err != nil {
		t.Fatalf("sync once 1: %v", err)
	}
	if err := orchestrator.SyncOnce(); err != nil {
		t.Fatalf("sync once 2: %v", err)
	}

	snap, err := node.Snapshot("vol-orch")
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if !snap.HasProjection || !snap.Projection.Readiness.RoleApplied {
		t.Fatal("expected applied projection after orchestrator sync")
	}
}

func TestKernelControlPlaneClosure_ConvergesAndReissuesOnDesiredChange(t *testing.T) {
	master := masterv2.New(masterv2.Config{})
	node, err := New(Config{NodeID: "node-a"})
	if err != nil {
		t.Fatalf("new volumev2 node: %v", err)
	}
	defer node.Close()
	session, err := NewInProcessSession(master)
	if err != nil {
		t.Fatalf("new session: %v", err)
	}
	orchestrator, err := NewOrchestrator(node, session)
	if err != nil {
		t.Fatalf("new orchestrator: %v", err)
	}

	tempDir := t.TempDir()
	path1 := filepath.Join(tempDir, "control-a.blk")
	path2 := filepath.Join(tempDir, "control-b.blk")
	spec := masterv2.VolumeSpec{
		Name:          "control-vol",
		Path:          path1,
		PrimaryNodeID: "node-a",
		CreateOptions: testCreateOptions(),
	}
	if err := master.DeclarePrimary(spec); err != nil {
		t.Fatalf("declare primary path1: %v", err)
	}

	assignments, err := session.Sync(mustHeartbeat(t, node))
	if err != nil {
		t.Fatalf("preview sync before convergence: %v", err)
	}
	if len(assignments) != 1 {
		t.Fatalf("preview assignments=%d, want 1", len(assignments))
	}
	if assignments[0].Path != path1 || assignments[0].Epoch != 1 {
		t.Fatalf("unexpected first assignment: %+v", assignments[0])
	}

	if err := orchestrator.SyncOnce(); err != nil {
		t.Fatalf("sync 1: %v", err)
	}
	if err := orchestrator.SyncOnce(); err != nil {
		t.Fatalf("sync 2: %v", err)
	}

	assignments, err = session.Sync(mustHeartbeat(t, node))
	if err != nil {
		t.Fatalf("post-convergence sync: %v", err)
	}
	if len(assignments) != 0 {
		t.Fatalf("unexpected post-convergence assignments=%d", len(assignments))
	}

	view, ok := master.Volume("control-vol")
	if !ok {
		t.Fatal("master view missing after convergence")
	}
	if view.DesiredEpoch != 1 || view.ObservedEpoch != 1 {
		t.Fatalf("epochs after convergence desired=%d observed=%d", view.DesiredEpoch, view.ObservedEpoch)
	}
	if view.ObservedRole != "primary" || !view.RoleApplied {
		t.Fatalf("view after convergence = %+v", view)
	}

	spec.Path = path2
	if err := master.DeclarePrimary(spec); err != nil {
		t.Fatalf("declare primary path2: %v", err)
	}
	assignments, err = session.Sync(mustHeartbeat(t, node))
	if err != nil {
		t.Fatalf("sync after desired change: %v", err)
	}
	if len(assignments) != 1 {
		t.Fatalf("assignments after desired change=%d, want 1", len(assignments))
	}
	if assignments[0].Path != path2 || assignments[0].Epoch != 2 {
		t.Fatalf("unexpected reissued assignment: %+v", assignments[0])
	}

	if err := orchestrator.SyncOnce(); err != nil {
		t.Fatalf("sync 3: %v", err)
	}
	if err := orchestrator.SyncOnce(); err != nil {
		t.Fatalf("sync 4: %v", err)
	}

	assignments, err = session.Sync(mustHeartbeat(t, node))
	if err != nil {
		t.Fatalf("final sync after re-convergence: %v", err)
	}
	if len(assignments) != 0 {
		t.Fatalf("unexpected assignments after re-convergence=%d", len(assignments))
	}

	view, ok = master.Volume("control-vol")
	if !ok {
		t.Fatal("master view missing after desired change")
	}
	if view.Path != path2 || view.DesiredEpoch != 2 || view.ObservedEpoch != 2 {
		t.Fatalf("view after desired change = %+v", view)
	}
	if view.ObservedRole != "primary" || !view.RoleApplied {
		t.Fatalf("final view not applied = %+v", view)
	}
}

func TestLoop1_PromotionQueryUsesFreshEvidenceSeparateFromHeartbeat(t *testing.T) {
	master := masterv2.New(masterv2.Config{})
	node, err := New(Config{NodeID: "node-a"})
	if err != nil {
		t.Fatalf("new volumev2 node: %v", err)
	}
	defer node.Close()
	session, err := NewInProcessSession(master)
	if err != nil {
		t.Fatalf("new session: %v", err)
	}
	orchestrator, err := NewOrchestrator(node, session)
	if err != nil {
		t.Fatalf("new orchestrator: %v", err)
	}

	path := filepath.Join(t.TempDir(), "query-vol.blk")
	if err := master.DeclarePrimary(masterv2.VolumeSpec{
		Name:          "query-vol",
		Path:          path,
		PrimaryNodeID: "node-a",
		CreateOptions: testCreateOptions(),
	}); err != nil {
		t.Fatalf("declare primary: %v", err)
	}
	if err := orchestrator.SyncOnce(); err != nil {
		t.Fatalf("sync 1: %v", err)
	}
	if err := orchestrator.SyncOnce(); err != nil {
		t.Fatalf("sync 2: %v", err)
	}

	hb := mustHeartbeat(t, node)
	if len(hb.Volumes) != 1 {
		t.Fatalf("heartbeat volumes=%d, want 1", len(hb.Volumes))
	}
	if hb.Volumes[0].Mode != "allocated_only" {
		t.Fatalf("heartbeat mode=%q", hb.Volumes[0].Mode)
	}

	before, err := node.QueryPromotionEvidence(masterv2.PromotionQueryRequest{
		VolumeName:    "query-vol",
		ExpectedEpoch: 1,
	})
	if err != nil {
		t.Fatalf("query before write: %v", err)
	}
	if !before.Eligible {
		t.Fatalf("before write should be eligible: %+v", before)
	}
	if hb.Volumes[0].CommittedLSN != before.CommittedLSN {
		t.Fatalf("heartbeat committed_lsn=%d, query committed_lsn=%d", hb.Volumes[0].CommittedLSN, before.CommittedLSN)
	}

	payload := bytes.Repeat([]byte{0x7D}, 4096)
	if err := node.WriteLBA("query-vol", 0, payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := node.SyncCache("query-vol"); err != nil {
		t.Fatalf("sync cache: %v", err)
	}

	after, err := node.QueryPromotionEvidence(masterv2.PromotionQueryRequest{
		VolumeName:    "query-vol",
		ExpectedEpoch: 1,
	})
	if err != nil {
		t.Fatalf("query after write: %v", err)
	}
	if after.CommittedLSN <= before.CommittedLSN {
		t.Fatalf("committed_lsn before=%d after=%d", before.CommittedLSN, after.CommittedLSN)
	}
	if after.WALHeadLSN <= before.WALHeadLSN {
		t.Fatalf("wal_head_lsn before=%d after=%d", before.WALHeadLSN, after.WALHeadLSN)
	}
	if after.CommittedLSN <= hb.Volumes[0].CommittedLSN {
		t.Fatalf("fresh query committed_lsn=%d should advance beyond heartbeat cache=%d", after.CommittedLSN, hb.Volumes[0].CommittedLSN)
	}

	if _, err := master.HandleHeartbeat(mustHeartbeat(t, node)); err != nil {
		t.Fatalf("refresh heartbeat after write: %v", err)
	}
	view, ok := master.Volume("query-vol")
	if !ok {
		t.Fatal("master view missing after refreshed heartbeat")
	}
	if view.CommittedLSN != after.CommittedLSN {
		t.Fatalf("master cached committed_lsn=%d, want %d", view.CommittedLSN, after.CommittedLSN)
	}
}

func TestLoop2_ReplicaSummaryPreservesBoundaryLayers(t *testing.T) {
	master := masterv2.New(masterv2.Config{})
	node, err := New(Config{NodeID: "node-a"})
	if err != nil {
		t.Fatalf("new volumev2 node: %v", err)
	}
	defer node.Close()
	session, err := NewInProcessSession(master)
	if err != nil {
		t.Fatalf("new session: %v", err)
	}
	orchestrator, err := NewOrchestrator(node, session)
	if err != nil {
		t.Fatalf("new orchestrator: %v", err)
	}

	path := filepath.Join(t.TempDir(), "summary-vol.blk")
	if err := master.DeclarePrimary(masterv2.VolumeSpec{
		Name:          "summary-vol",
		Path:          path,
		PrimaryNodeID: "node-a",
		CreateOptions: testCreateOptions(),
	}); err != nil {
		t.Fatalf("declare primary: %v", err)
	}
	if err := orchestrator.SyncOnce(); err != nil {
		t.Fatalf("sync 1: %v", err)
	}
	if err := orchestrator.SyncOnce(); err != nil {
		t.Fatalf("sync 2: %v", err)
	}

	payload := bytes.Repeat([]byte{0x22}, 4096)
	if err := node.WriteLBA("summary-vol", 0, payload); err != nil {
		t.Fatalf("write: %v", err)
	}

	beforeSync, err := node.QueryReplicaSummary(protocolv2.ReplicaSummaryRequest{
		VolumeName:    "summary-vol",
		ExpectedEpoch: 1,
	})
	if err != nil {
		t.Fatalf("summary before sync: %v", err)
	}
	if !beforeSync.Eligible {
		t.Fatalf("summary before sync should be eligible: %+v", beforeSync)
	}
	if beforeSync.CommittedLSN == 0 {
		t.Fatalf("committed_lsn=%d, want > 0", beforeSync.CommittedLSN)
	}
	if beforeSync.DurableLSN != 0 {
		t.Fatalf("durable_lsn=%d before sync, want 0", beforeSync.DurableLSN)
	}

	if err := node.SyncCache("summary-vol"); err != nil {
		t.Fatalf("sync cache: %v", err)
	}

	afterSync, err := node.QueryReplicaSummary(protocolv2.ReplicaSummaryRequest{
		VolumeName:    "summary-vol",
		ExpectedEpoch: 1,
	})
	if err != nil {
		t.Fatalf("summary after sync: %v", err)
	}
	if afterSync.CommittedLSN < beforeSync.CommittedLSN {
		t.Fatalf("committed_lsn before=%d after=%d", beforeSync.CommittedLSN, afterSync.CommittedLSN)
	}
	if afterSync.DurableLSN < afterSync.CommittedLSN {
		t.Fatalf("durable_lsn=%d committed_lsn=%d", afterSync.DurableLSN, afterSync.CommittedLSN)
	}
	if afterSync.CheckpointLSN > afterSync.DurableLSN {
		t.Fatalf("checkpoint_lsn=%d durable_lsn=%d", afterSync.CheckpointLSN, afterSync.DurableLSN)
	}
	if afterSync.RecoveryPhase != "idle" {
		t.Fatalf("recovery_phase=%q", afterSync.RecoveryPhase)
	}
}

func TestLoop2_ReplacementPrimaryReconstructsTakeoverTruth(t *testing.T) {
	master := masterv2.New(masterv2.Config{})
	nodeB, err := New(Config{NodeID: "node-b"})
	if err != nil {
		t.Fatalf("new node-b: %v", err)
	}
	defer nodeB.Close()
	nodeC, err := New(Config{NodeID: "node-c"})
	if err != nil {
		t.Fatalf("new node-c: %v", err)
	}
	defer nodeC.Close()

	tempDir := t.TempDir()
	assignments := []masterv2.Assignment{
		{
			Name:          "takeover-vol",
			Path:          filepath.Join(tempDir, "takeover-b.blk"),
			NodeID:        "node-b",
			Epoch:         3,
			LeaseTTL:      30 * time.Second,
			CreateOptions: testCreateOptions(),
			Role:          "primary",
		},
		{
			Name:          "takeover-vol",
			Path:          filepath.Join(tempDir, "takeover-c.blk"),
			NodeID:        "node-c",
			Epoch:         3,
			LeaseTTL:      30 * time.Second,
			CreateOptions: testCreateOptions(),
			Role:          "primary",
		},
	}
	if err := nodeB.ApplyAssignments(assignments); err != nil {
		t.Fatalf("apply node-b assignment: %v", err)
	}
	if err := nodeC.ApplyAssignments(assignments); err != nil {
		t.Fatalf("apply node-c assignment: %v", err)
	}

	payload := bytes.Repeat([]byte{0x6E}, 4096)
	if err := nodeB.WriteLBA("takeover-vol", 0, payload); err != nil {
		t.Fatalf("write node-b: %v", err)
	}
	if err := nodeB.SyncCache("takeover-vol"); err != nil {
		t.Fatalf("sync node-b: %v", err)
	}

	candidate, err := master.SelectPromotionCandidate([]masterv2.PromotionQueryResponse{
		mustPromotionEvidence(t, nodeB, "takeover-vol", 3),
		mustPromotionEvidence(t, nodeC, "takeover-vol", 3),
	})
	if err != nil {
		t.Fatalf("select promotion candidate: %v", err)
	}
	if candidate.NodeID != "node-b" {
		t.Fatalf("candidate node=%q, want node-b", candidate.NodeID)
	}

	truth, err := nodeB.ReconstructTakeoverTruth("takeover-vol", 3, []ReplicaSummarySource{nodeC})
	if err != nil {
		t.Fatalf("reconstruct takeover truth: %v", err)
	}
	if truth.PrimaryNodeID != "node-b" {
		t.Fatalf("primary_node=%q, want node-b", truth.PrimaryNodeID)
	}
	if truth.ReplicaCount != 2 {
		t.Fatalf("replica_count=%d, want 2", truth.ReplicaCount)
	}
	if truth.CommittedLSN == 0 {
		t.Fatalf("committed_lsn=%d, want > 0", truth.CommittedLSN)
	}
	if truth.DurableLSN < truth.CommittedLSN {
		t.Fatalf("durable_lsn=%d committed_lsn=%d", truth.DurableLSN, truth.CommittedLSN)
	}
	if truth.Degraded {
		t.Fatalf("unexpected degraded truth: %+v", truth)
	}
}

func TestLoop2_PrepareAndGatePrimaryTakeover_AllowsHealthyCandidate(t *testing.T) {
	nodeB, err := New(Config{NodeID: "node-b"})
	if err != nil {
		t.Fatalf("new node-b: %v", err)
	}
	defer nodeB.Close()
	nodeC, err := New(Config{NodeID: "node-c"})
	if err != nil {
		t.Fatalf("new node-c: %v", err)
	}
	defer nodeC.Close()

	tempDir := t.TempDir()
	assignB := masterv2.Assignment{
		Name:          "takeover-allow-vol",
		Path:          filepath.Join(tempDir, "takeover-allow-b.blk"),
		NodeID:        "node-b",
		Epoch:         5,
		LeaseTTL:      30 * time.Second,
		CreateOptions: testCreateOptions(),
		Role:          "primary",
	}
	assignC := masterv2.Assignment{
		Name:          "takeover-allow-vol",
		Path:          filepath.Join(tempDir, "takeover-allow-c.blk"),
		NodeID:        "node-c",
		Epoch:         5,
		LeaseTTL:      30 * time.Second,
		CreateOptions: testCreateOptions(),
		Role:          "primary",
	}
	if err := nodeC.ApplyAssignments([]masterv2.Assignment{assignC}); err != nil {
		t.Fatalf("apply peer assignment: %v", err)
	}

	payload := bytes.Repeat([]byte{0x2C}, 4096)
	if err := nodeB.ApplyAssignments([]masterv2.Assignment{assignB}); err != nil {
		t.Fatalf("seed candidate assignment: %v", err)
	}
	if err := nodeB.WriteLBA("takeover-allow-vol", 0, payload); err != nil {
		t.Fatalf("write node-b: %v", err)
	}
	if err := nodeB.SyncCache("takeover-allow-vol"); err != nil {
		t.Fatalf("sync node-b: %v", err)
	}

	truth, err := nodeB.PreparePrimaryTakeover(PrimaryTakeoverPlan{
		Assignment: assignB,
		Peers:      []ReplicaSummarySource{nodeC},
	})
	if err != nil {
		t.Fatalf("prepare primary takeover: %v", err)
	}
	if err := nodeB.GatePrimaryActivation(assignB.Name, truth); err != nil {
		t.Fatalf("gate primary activation: %v", err)
	}
	if truth.PrimaryNodeID != "node-b" {
		t.Fatalf("primary_node=%q, want node-b", truth.PrimaryNodeID)
	}
	if truth.ReplicaCount != 2 {
		t.Fatalf("replica_count=%d, want 2", truth.ReplicaCount)
	}
}

func TestLoop2_ApplyPrimaryTakeover_GatesDegradedTruth(t *testing.T) {
	nodeB, err := New(Config{NodeID: "node-b"})
	if err != nil {
		t.Fatalf("new node-b: %v", err)
	}
	defer nodeB.Close()

	tempDir := t.TempDir()
	assignB := masterv2.Assignment{
		Name:          "takeover-gated-vol",
		Path:          filepath.Join(tempDir, "takeover-gated-b.blk"),
		NodeID:        "node-b",
		Epoch:         6,
		LeaseTTL:      30 * time.Second,
		CreateOptions: testCreateOptions(),
		Role:          "primary",
	}

	truth, err := nodeB.PreparePrimaryTakeover(PrimaryTakeoverPlan{
		Assignment: assignB,
		Peers: []ReplicaSummarySource{
			staticReplicaSummarySource{resp: protocolv2.ReplicaSummaryResponse{
				VolumeName:        "takeover-gated-vol",
				NodeID:            "node-c",
				Epoch:             5,
				Role:              "replica",
				Mode:              "needs_rebuild",
				CommittedLSN:      3,
				DurableLSN:        2,
				CheckpointLSN:     1,
				RecoveryPhase:     "needs_rebuild",
				LastBarrierOK:     false,
				LastBarrierReason: "timeout",
				Eligible:          false,
				Reason:            "needs_rebuild",
			}},
		},
	})
	if err != nil {
		t.Fatalf("prepare primary takeover: %v", err)
	}
	if err := nodeB.GatePrimaryActivation(assignB.Name, truth); err == nil {
		t.Fatal("expected takeover gate error")
	}
}

func TestFailoverFlow_AuthorizesAndActivatesHealthyCandidate(t *testing.T) {
	master := masterv2.New(masterv2.Config{})
	nodeB, err := New(Config{NodeID: "node-b"})
	if err != nil {
		t.Fatalf("new node-b: %v", err)
	}
	defer nodeB.Close()
	nodeC, err := New(Config{NodeID: "node-c"})
	if err != nil {
		t.Fatalf("new node-c: %v", err)
	}
	defer nodeC.Close()

	tempDir := t.TempDir()
	pathB := filepath.Join(tempDir, "flow-b.blk")
	pathC := filepath.Join(tempDir, "flow-c.blk")
	if err := master.DeclarePrimary(masterv2.VolumeSpec{
		Name:          "flow-vol",
		Path:          pathB,
		PrimaryNodeID: "node-a",
		CreateOptions: testCreateOptions(),
	}); err != nil {
		t.Fatalf("declare primary: %v", err)
	}
	if err := nodeB.ApplyAssignments([]masterv2.Assignment{{
		Name:          "flow-vol",
		Path:          pathB,
		NodeID:        "node-b",
		Epoch:         2,
		LeaseTTL:      30 * time.Second,
		CreateOptions: testCreateOptions(),
		Role:          "primary",
	}}); err != nil {
		t.Fatalf("seed node-b: %v", err)
	}
	if err := nodeC.ApplyAssignments([]masterv2.Assignment{{
		Name:          "flow-vol",
		Path:          pathC,
		NodeID:        "node-c",
		Epoch:         2,
		LeaseTTL:      30 * time.Second,
		CreateOptions: testCreateOptions(),
		Role:          "primary",
	}}); err != nil {
		t.Fatalf("seed node-c: %v", err)
	}

	payload := bytes.Repeat([]byte{0x5A}, 4096)
	if err := nodeB.WriteLBA("flow-vol", 0, payload); err != nil {
		t.Fatalf("write node-b: %v", err)
	}
	if err := nodeB.SyncCache("flow-vol"); err != nil {
		t.Fatalf("sync node-b: %v", err)
	}

	result, err := ExecuteFailoverFlow(master, "flow-vol", 2, []FailoverTarget{
		mustInProcessFailoverTarget(t, nodeB),
		mustInProcessFailoverTarget(t, nodeC),
	})
	if err != nil {
		t.Fatalf("execute failover flow: %v", err)
	}
	if result.Candidate.NodeID != "node-b" {
		t.Fatalf("candidate node=%q, want node-b", result.Candidate.NodeID)
	}
	if result.Assignment.NodeID != "node-b" {
		t.Fatalf("assignment node=%q, want node-b", result.Assignment.NodeID)
	}
	if result.Assignment.Epoch != 2 {
		t.Fatalf("assignment epoch=%d, want 2", result.Assignment.Epoch)
	}
	if result.Truth.PrimaryNodeID != "node-b" {
		t.Fatalf("truth primary=%q, want node-b", result.Truth.PrimaryNodeID)
	}
	if result.Truth.Degraded {
		t.Fatalf("unexpected degraded truth: %+v", result.Truth)
	}

	view, ok := master.Volume("flow-vol")
	if !ok {
		t.Fatal("master view missing")
	}
	if view.PrimaryNodeID != "node-b" {
		t.Fatalf("view primary=%q, want node-b", view.PrimaryNodeID)
	}
	if view.DesiredEpoch != 2 {
		t.Fatalf("view desired epoch=%d, want 2", view.DesiredEpoch)
	}
}

func TestFailoverFlow_StopsAtActivationGate(t *testing.T) {
	master := masterv2.New(masterv2.Config{})
	nodeB, err := New(Config{NodeID: "node-b"})
	if err != nil {
		t.Fatalf("new node-b: %v", err)
	}
	defer nodeB.Close()

	tempDir := t.TempDir()
	pathB := filepath.Join(tempDir, "flow-gated-b.blk")
	if err := master.DeclarePrimary(masterv2.VolumeSpec{
		Name:          "flow-gated-vol",
		Path:          pathB,
		PrimaryNodeID: "node-a",
		CreateOptions: testCreateOptions(),
	}); err != nil {
		t.Fatalf("declare primary: %v", err)
	}
	if err := nodeB.ApplyAssignments([]masterv2.Assignment{{
		Name:          "flow-gated-vol",
		Path:          pathB,
		NodeID:        "node-b",
		Epoch:         2,
		LeaseTTL:      30 * time.Second,
		CreateOptions: testCreateOptions(),
		Role:          "primary",
	}}); err != nil {
		t.Fatalf("seed node-b: %v", err)
	}

	result, err := ExecuteFailoverFlow(master, "flow-gated-vol", 2, []FailoverTarget{
		mustInProcessFailoverTarget(t, nodeB),
		staticFailoverTarget(
			"node-c",
			masterv2.PromotionQueryResponse{
				VolumeName:   "flow-gated-vol",
				NodeID:       "node-c",
				Epoch:        1,
				CommittedLSN: 1,
				WALHeadLSN:   1,
				Eligible:     false,
				Reason:       "needs_rebuild",
			},
			protocolv2.ReplicaSummaryResponse{
				VolumeName:        "flow-gated-vol",
				NodeID:            "node-c",
				Epoch:             1,
				Role:              "replica",
				Mode:              "needs_rebuild",
				CommittedLSN:      3,
				DurableLSN:        2,
				CheckpointLSN:     1,
				RecoveryPhase:     "needs_rebuild",
				LastBarrierOK:     false,
				LastBarrierReason: "timeout",
				Eligible:          false,
				Reason:            "needs_rebuild",
			},
		),
	})
	if err == nil {
		t.Fatal("expected failover gate error")
	}
	if result.Assignment.NodeID != "node-b" {
		t.Fatalf("assignment node=%q, want node-b", result.Assignment.NodeID)
	}
	if !result.Truth.Degraded {
		t.Fatalf("expected degraded truth: %+v", result.Truth)
	}
	if !result.Truth.NeedsRebuild {
		t.Fatalf("expected needs_rebuild truth: %+v", result.Truth)
	}
}

func TestFailoverSession_StepwiseStagesExposeIntermediateState(t *testing.T) {
	master := masterv2.New(masterv2.Config{})
	nodeB, err := New(Config{NodeID: "node-b"})
	if err != nil {
		t.Fatalf("new node-b: %v", err)
	}
	defer nodeB.Close()
	nodeC, err := New(Config{NodeID: "node-c"})
	if err != nil {
		t.Fatalf("new node-c: %v", err)
	}
	defer nodeC.Close()

	tempDir := t.TempDir()
	pathB := filepath.Join(tempDir, "session-b.blk")
	pathC := filepath.Join(tempDir, "session-c.blk")
	if err := master.DeclarePrimary(masterv2.VolumeSpec{
		Name:          "session-vol",
		Path:          pathB,
		PrimaryNodeID: "node-a",
		CreateOptions: testCreateOptions(),
	}); err != nil {
		t.Fatalf("declare primary: %v", err)
	}
	if err := nodeB.ApplyAssignments([]masterv2.Assignment{{
		Name:          "session-vol",
		Path:          pathB,
		NodeID:        "node-b",
		Epoch:         2,
		LeaseTTL:      30 * time.Second,
		CreateOptions: testCreateOptions(),
		Role:          "primary",
	}}); err != nil {
		t.Fatalf("seed node-b: %v", err)
	}
	if err := nodeC.ApplyAssignments([]masterv2.Assignment{{
		Name:          "session-vol",
		Path:          pathC,
		NodeID:        "node-c",
		Epoch:         2,
		LeaseTTL:      30 * time.Second,
		CreateOptions: testCreateOptions(),
		Role:          "primary",
	}}); err != nil {
		t.Fatalf("seed node-c: %v", err)
	}
	if err := nodeB.WriteLBA("session-vol", 0, bytes.Repeat([]byte{0x41}, 4096)); err != nil {
		t.Fatalf("write node-b: %v", err)
	}
	if err := nodeB.SyncCache("session-vol"); err != nil {
		t.Fatalf("sync node-b: %v", err)
	}

	session, err := NewFailoverSession(master, "session-vol", 2, []FailoverTarget{
		mustInProcessFailoverTarget(t, nodeB),
		mustInProcessFailoverTarget(t, nodeC),
	})
	if err != nil {
		t.Fatalf("new failover session: %v", err)
	}
	if session.Stage() != FailoverStageNew {
		t.Fatalf("initial stage=%q, want %q", session.Stage(), FailoverStageNew)
	}

	responses, err := session.CollectPromotionEvidence()
	if err != nil {
		t.Fatalf("collect promotion evidence: %v", err)
	}
	if len(responses) != 2 {
		t.Fatalf("responses=%d, want 2", len(responses))
	}
	if session.Stage() != FailoverStageEvidenceCollected {
		t.Fatalf("stage after collect=%q, want %q", session.Stage(), FailoverStageEvidenceCollected)
	}
	if session.Snapshot().ResponseCount != 2 {
		t.Fatalf("snapshot response_count=%d, want 2", session.Snapshot().ResponseCount)
	}

	assign, err := session.Authorize()
	if err != nil {
		t.Fatalf("authorize: %v", err)
	}
	if assign.NodeID != "node-b" {
		t.Fatalf("assignment node=%q, want node-b", assign.NodeID)
	}
	if session.Stage() != FailoverStageAuthorized {
		t.Fatalf("stage after authorize=%q, want %q", session.Stage(), FailoverStageAuthorized)
	}

	truth, err := session.PrepareTakeover()
	if err != nil {
		t.Fatalf("prepare takeover: %v", err)
	}
	if truth.PrimaryNodeID != "node-b" {
		t.Fatalf("truth primary=%q, want node-b", truth.PrimaryNodeID)
	}
	if session.Stage() != FailoverStagePrepared {
		t.Fatalf("stage after prepare=%q, want %q", session.Stage(), FailoverStagePrepared)
	}

	if err := session.Activate(); err != nil {
		t.Fatalf("activate: %v", err)
	}
	if session.Stage() != FailoverStageActivated {
		t.Fatalf("stage after activate=%q, want %q", session.Stage(), FailoverStageActivated)
	}

	result := session.Result()
	if result.Candidate.NodeID != "node-b" {
		t.Fatalf("result candidate=%q, want node-b", result.Candidate.NodeID)
	}
	if result.Assignment.NodeID != "node-b" {
		t.Fatalf("result assignment=%q, want node-b", result.Assignment.NodeID)
	}
	if result.Truth.PrimaryNodeID != "node-b" {
		t.Fatalf("result truth primary=%q, want node-b", result.Truth.PrimaryNodeID)
	}
}

func TestFailoverSession_SnapshotCapturesFailureState(t *testing.T) {
	master := masterv2.New(masterv2.Config{})
	nodeB, err := New(Config{NodeID: "node-b"})
	if err != nil {
		t.Fatalf("new node-b: %v", err)
	}
	defer nodeB.Close()

	tempDir := t.TempDir()
	pathB := filepath.Join(tempDir, "snapshot-gated-b.blk")
	if err := master.DeclarePrimary(masterv2.VolumeSpec{
		Name:          "snapshot-gated-vol",
		Path:          pathB,
		PrimaryNodeID: "node-a",
		CreateOptions: testCreateOptions(),
	}); err != nil {
		t.Fatalf("declare primary: %v", err)
	}
	if err := nodeB.ApplyAssignments([]masterv2.Assignment{{
		Name:          "snapshot-gated-vol",
		Path:          pathB,
		NodeID:        "node-b",
		Epoch:         2,
		LeaseTTL:      30 * time.Second,
		CreateOptions: testCreateOptions(),
		Role:          "primary",
	}}); err != nil {
		t.Fatalf("seed node-b: %v", err)
	}

	session, err := NewFailoverSession(master, "snapshot-gated-vol", 2, []FailoverTarget{
		mustInProcessFailoverTarget(t, nodeB),
		staticFailoverTarget(
			"node-c",
			masterv2.PromotionQueryResponse{
				VolumeName:   "snapshot-gated-vol",
				NodeID:       "node-c",
				Epoch:        1,
				CommittedLSN: 1,
				WALHeadLSN:   1,
				Eligible:     false,
				Reason:       "needs_rebuild",
			},
			protocolv2.ReplicaSummaryResponse{
				VolumeName:        "snapshot-gated-vol",
				NodeID:            "node-c",
				Epoch:             1,
				Role:              "replica",
				Mode:              "needs_rebuild",
				CommittedLSN:      3,
				DurableLSN:        2,
				CheckpointLSN:     1,
				RecoveryPhase:     "needs_rebuild",
				LastBarrierOK:     false,
				LastBarrierReason: "timeout",
				Eligible:          false,
				Reason:            "needs_rebuild",
			},
		),
	})
	if err != nil {
		t.Fatalf("new failover session: %v", err)
	}

	if _, err := session.Run(); err == nil {
		t.Fatal("expected failover session error")
	}
	if session.Stage() != FailoverStageFailed {
		t.Fatalf("stage after failure=%q, want %q", session.Stage(), FailoverStageFailed)
	}
	if session.LastError() == nil {
		t.Fatal("expected last error")
	}
	snap := session.Snapshot()
	if snap.LastError == "" {
		t.Fatal("expected snapshot last error")
	}
	if snap.SelectedNodeID != "node-b" {
		t.Fatalf("snapshot selected node=%q, want node-b", snap.SelectedNodeID)
	}
	if !snap.Result.Truth.NeedsRebuild {
		t.Fatalf("expected needs_rebuild truth in snapshot: %+v", snap.Result.Truth)
	}
}

func TestInProcessFailoverDriver_ExecuteHealthyFailover(t *testing.T) {
	master := masterv2.New(masterv2.Config{})
	driver, err := NewInProcessFailoverDriver(master)
	if err != nil {
		t.Fatalf("new failover driver: %v", err)
	}
	nodeB, err := New(Config{NodeID: "node-b"})
	if err != nil {
		t.Fatalf("new node-b: %v", err)
	}
	defer nodeB.Close()
	nodeC, err := New(Config{NodeID: "node-c"})
	if err != nil {
		t.Fatalf("new node-c: %v", err)
	}
	defer nodeC.Close()
	if err := driver.RegisterTarget(mustInProcessFailoverTarget(t, nodeB)); err != nil {
		t.Fatalf("register node-b: %v", err)
	}
	if err := driver.RegisterTarget(mustInProcessFailoverTarget(t, nodeC)); err != nil {
		t.Fatalf("register node-c: %v", err)
	}

	tempDir := t.TempDir()
	pathB := filepath.Join(tempDir, "driver-b.blk")
	pathC := filepath.Join(tempDir, "driver-c.blk")
	if err := master.DeclarePrimary(masterv2.VolumeSpec{
		Name:          "driver-vol",
		Path:          pathB,
		PrimaryNodeID: "node-a",
		CreateOptions: testCreateOptions(),
	}); err != nil {
		t.Fatalf("declare primary: %v", err)
	}
	if err := nodeB.ApplyAssignments([]masterv2.Assignment{{
		Name:          "driver-vol",
		Path:          pathB,
		NodeID:        "node-b",
		Epoch:         2,
		LeaseTTL:      30 * time.Second,
		CreateOptions: testCreateOptions(),
		Role:          "primary",
	}}); err != nil {
		t.Fatalf("seed node-b: %v", err)
	}
	if err := nodeC.ApplyAssignments([]masterv2.Assignment{{
		Name:          "driver-vol",
		Path:          pathC,
		NodeID:        "node-c",
		Epoch:         2,
		LeaseTTL:      30 * time.Second,
		CreateOptions: testCreateOptions(),
		Role:          "primary",
	}}); err != nil {
		t.Fatalf("seed node-c: %v", err)
	}
	if err := nodeB.WriteLBA("driver-vol", 0, bytes.Repeat([]byte{0x33}, 4096)); err != nil {
		t.Fatalf("write node-b: %v", err)
	}
	if err := nodeB.SyncCache("driver-vol"); err != nil {
		t.Fatalf("sync node-b: %v", err)
	}

	result, err := driver.Execute("driver-vol", 2)
	if err != nil {
		t.Fatalf("driver execute: %v", err)
	}
	if result.Assignment.NodeID != "node-b" {
		t.Fatalf("assignment node=%q, want node-b", result.Assignment.NodeID)
	}
	if result.Truth.PrimaryNodeID != "node-b" {
		t.Fatalf("truth primary=%q, want node-b", result.Truth.PrimaryNodeID)
	}
}

func TestInProcessFailoverDriver_ExecuteStopsOnGate(t *testing.T) {
	master := masterv2.New(masterv2.Config{})
	driver, err := NewInProcessFailoverDriver(master)
	if err != nil {
		t.Fatalf("new failover driver: %v", err)
	}
	nodeB, err := New(Config{NodeID: "node-b"})
	if err != nil {
		t.Fatalf("new node-b: %v", err)
	}
	defer nodeB.Close()
	if err := driver.RegisterTarget(mustInProcessFailoverTarget(t, nodeB)); err != nil {
		t.Fatalf("register node-b: %v", err)
	}
	if err := driver.RegisterTarget(staticFailoverTarget(
		"node-c",
		masterv2.PromotionQueryResponse{
			VolumeName:   "driver-gated-vol",
			NodeID:       "node-c",
			Epoch:        1,
			CommittedLSN: 1,
			WALHeadLSN:   1,
			Eligible:     false,
			Reason:       "needs_rebuild",
		},
		protocolv2.ReplicaSummaryResponse{
			VolumeName:        "driver-gated-vol",
			NodeID:            "node-c",
			Epoch:             1,
			Role:              "replica",
			Mode:              "needs_rebuild",
			CommittedLSN:      3,
			DurableLSN:        2,
			CheckpointLSN:     1,
			RecoveryPhase:     "needs_rebuild",
			LastBarrierOK:     false,
			LastBarrierReason: "timeout",
			Eligible:          false,
			Reason:            "needs_rebuild",
		},
	)); err != nil {
		t.Fatalf("register node-c: %v", err)
	}

	tempDir := t.TempDir()
	pathB := filepath.Join(tempDir, "driver-gated-b.blk")
	if err := master.DeclarePrimary(masterv2.VolumeSpec{
		Name:          "driver-gated-vol",
		Path:          pathB,
		PrimaryNodeID: "node-a",
		CreateOptions: testCreateOptions(),
	}); err != nil {
		t.Fatalf("declare primary: %v", err)
	}
	if err := nodeB.ApplyAssignments([]masterv2.Assignment{{
		Name:          "driver-gated-vol",
		Path:          pathB,
		NodeID:        "node-b",
		Epoch:         2,
		LeaseTTL:      30 * time.Second,
		CreateOptions: testCreateOptions(),
		Role:          "primary",
	}}); err != nil {
		t.Fatalf("seed node-b: %v", err)
	}

	_, err = driver.Execute("driver-gated-vol", 2)
	if err == nil {
		t.Fatal("expected gated driver execute error")
	}
}

func TestInProcessRuntimeManager_ExecuteHealthyFailoverAndPersistSnapshot(t *testing.T) {
	master := masterv2.New(masterv2.Config{})
	manager, err := NewInProcessRuntimeManager(master)
	if err != nil {
		t.Fatalf("new runtime manager: %v", err)
	}
	nodeB, err := New(Config{NodeID: "node-b"})
	if err != nil {
		t.Fatalf("new node-b: %v", err)
	}
	defer nodeB.Close()
	nodeC, err := New(Config{NodeID: "node-c"})
	if err != nil {
		t.Fatalf("new node-c: %v", err)
	}
	defer nodeC.Close()
	if err := manager.RegisterNode(nodeB); err != nil {
		t.Fatalf("register node-b: %v", err)
	}
	if err := manager.RegisterNode(nodeC); err != nil {
		t.Fatalf("register node-c: %v", err)
	}

	tempDir := t.TempDir()
	pathB := filepath.Join(tempDir, "manager-b.blk")
	pathC := filepath.Join(tempDir, "manager-c.blk")
	if err := master.DeclarePrimary(masterv2.VolumeSpec{
		Name:          "manager-vol",
		Path:          pathB,
		PrimaryNodeID: "node-a",
		CreateOptions: testCreateOptions(),
	}); err != nil {
		t.Fatalf("declare primary: %v", err)
	}
	if err := nodeB.ApplyAssignments([]masterv2.Assignment{{
		Name:          "manager-vol",
		Path:          pathB,
		NodeID:        "node-b",
		Epoch:         2,
		LeaseTTL:      30 * time.Second,
		CreateOptions: testCreateOptions(),
		Role:          "primary",
	}}); err != nil {
		t.Fatalf("seed node-b: %v", err)
	}
	if err := nodeC.ApplyAssignments([]masterv2.Assignment{{
		Name:          "manager-vol",
		Path:          pathC,
		NodeID:        "node-c",
		Epoch:         2,
		LeaseTTL:      30 * time.Second,
		CreateOptions: testCreateOptions(),
		Role:          "primary",
	}}); err != nil {
		t.Fatalf("seed node-c: %v", err)
	}
	if err := nodeB.WriteLBA("manager-vol", 0, bytes.Repeat([]byte{0x52}, 4096)); err != nil {
		t.Fatalf("write node-b: %v", err)
	}
	if err := nodeB.SyncCache("manager-vol"); err != nil {
		t.Fatalf("sync node-b: %v", err)
	}

	result, err := manager.ExecuteFailover("manager-vol", 2)
	if err != nil {
		t.Fatalf("manager execute failover: %v", err)
	}
	if result.Assignment.NodeID != "node-b" {
		t.Fatalf("assignment node=%q, want node-b", result.Assignment.NodeID)
	}

	lastSnap, ok := manager.LastFailoverSnapshot()
	if !ok {
		t.Fatal("expected last failover snapshot")
	}
	if lastSnap.Stage != FailoverStageActivated {
		t.Fatalf("snapshot stage=%q, want %q", lastSnap.Stage, FailoverStageActivated)
	}
	if lastSnap.SelectedNodeID != "node-b" {
		t.Fatalf("snapshot selected node=%q, want node-b", lastSnap.SelectedNodeID)
	}

	perVolSnap, ok := manager.FailoverSnapshot("manager-vol")
	if !ok {
		t.Fatal("expected per-volume failover snapshot")
	}
	if perVolSnap.Result.Truth.PrimaryNodeID != "node-b" {
		t.Fatalf("per-volume truth primary=%q, want node-b", perVolSnap.Result.Truth.PrimaryNodeID)
	}

	lastResult, ok := manager.LastFailoverResult()
	if !ok {
		t.Fatal("expected last failover result")
	}
	if lastResult.Assignment.NodeID != "node-b" {
		t.Fatalf("last result assignment=%q, want node-b", lastResult.Assignment.NodeID)
	}
}

func TestInProcessRuntimeManager_ExecuteStopsOnGateAndPersistsFailureSnapshot(t *testing.T) {
	master := masterv2.New(masterv2.Config{})
	manager, err := NewInProcessRuntimeManager(master)
	if err != nil {
		t.Fatalf("new runtime manager: %v", err)
	}
	nodeB, err := New(Config{NodeID: "node-b"})
	if err != nil {
		t.Fatalf("new node-b: %v", err)
	}
	defer nodeB.Close()
	if err := manager.RegisterNode(nodeB); err != nil {
		t.Fatalf("register node-b: %v", err)
	}
	if err := manager.RegisterTarget(staticFailoverTarget(
		"node-c",
		masterv2.PromotionQueryResponse{
			VolumeName:   "manager-gated-vol",
			NodeID:       "node-c",
			Epoch:        1,
			CommittedLSN: 1,
			WALHeadLSN:   1,
			Eligible:     false,
			Reason:       "needs_rebuild",
		},
		protocolv2.ReplicaSummaryResponse{
			VolumeName:        "manager-gated-vol",
			NodeID:            "node-c",
			Epoch:             1,
			Role:              "replica",
			Mode:              "needs_rebuild",
			CommittedLSN:      3,
			DurableLSN:        2,
			CheckpointLSN:     1,
			RecoveryPhase:     "needs_rebuild",
			LastBarrierOK:     false,
			LastBarrierReason: "timeout",
			Eligible:          false,
			Reason:            "needs_rebuild",
		},
	)); err != nil {
		t.Fatalf("register node-c: %v", err)
	}

	tempDir := t.TempDir()
	pathB := filepath.Join(tempDir, "manager-gated-b.blk")
	if err := master.DeclarePrimary(masterv2.VolumeSpec{
		Name:          "manager-gated-vol",
		Path:          pathB,
		PrimaryNodeID: "node-a",
		CreateOptions: testCreateOptions(),
	}); err != nil {
		t.Fatalf("declare primary: %v", err)
	}
	if err := nodeB.ApplyAssignments([]masterv2.Assignment{{
		Name:          "manager-gated-vol",
		Path:          pathB,
		NodeID:        "node-b",
		Epoch:         2,
		LeaseTTL:      30 * time.Second,
		CreateOptions: testCreateOptions(),
		Role:          "primary",
	}}); err != nil {
		t.Fatalf("seed node-b: %v", err)
	}

	_, err = manager.ExecuteFailover("manager-gated-vol", 2)
	if err == nil {
		t.Fatal("expected manager failover gate error")
	}

	lastSnap, ok := manager.LastFailoverSnapshot()
	if !ok {
		t.Fatal("expected last failover snapshot")
	}
	if lastSnap.Stage != FailoverStageFailed {
		t.Fatalf("snapshot stage=%q, want %q", lastSnap.Stage, FailoverStageFailed)
	}
	if lastSnap.LastError == "" {
		t.Fatal("expected snapshot last error")
	}
	if !lastSnap.Result.Truth.NeedsRebuild {
		t.Fatalf("expected needs_rebuild truth in snapshot: %+v", lastSnap.Result.Truth)
	}
}

func TestTransportEvidenceAdapter_HealthyFailoverFlow(t *testing.T) {
	master := masterv2.New(masterv2.Config{})
	transport := NewInMemoryFailoverEvidenceTransport()
	nodeB, err := New(Config{NodeID: "node-b"})
	if err != nil {
		t.Fatalf("new node-b: %v", err)
	}
	defer nodeB.Close()
	nodeC, err := New(Config{NodeID: "node-c"})
	if err != nil {
		t.Fatalf("new node-c: %v", err)
	}
	defer nodeC.Close()
	if err := transport.RegisterHandler("node-b", nodeB); err != nil {
		t.Fatalf("register node-b handler: %v", err)
	}
	if err := transport.RegisterHandler("node-c", nodeC); err != nil {
		t.Fatalf("register node-c handler: %v", err)
	}

	tempDir := t.TempDir()
	pathB := filepath.Join(tempDir, "transport-b.blk")
	pathC := filepath.Join(tempDir, "transport-c.blk")
	if err := master.DeclarePrimary(masterv2.VolumeSpec{
		Name:          "transport-vol",
		Path:          pathB,
		PrimaryNodeID: "node-a",
		CreateOptions: testCreateOptions(),
	}); err != nil {
		t.Fatalf("declare primary: %v", err)
	}
	if err := nodeB.ApplyAssignments([]masterv2.Assignment{{
		Name:          "transport-vol",
		Path:          pathB,
		NodeID:        "node-b",
		Epoch:         2,
		LeaseTTL:      30 * time.Second,
		CreateOptions: testCreateOptions(),
		Role:          "primary",
	}}); err != nil {
		t.Fatalf("seed node-b: %v", err)
	}
	if err := nodeC.ApplyAssignments([]masterv2.Assignment{{
		Name:          "transport-vol",
		Path:          pathC,
		NodeID:        "node-c",
		Epoch:         2,
		LeaseTTL:      30 * time.Second,
		CreateOptions: testCreateOptions(),
		Role:          "primary",
	}}); err != nil {
		t.Fatalf("seed node-c: %v", err)
	}
	if err := nodeB.WriteLBA("transport-vol", 0, bytes.Repeat([]byte{0x61}, 4096)); err != nil {
		t.Fatalf("write node-b: %v", err)
	}
	if err := nodeB.SyncCache("transport-vol"); err != nil {
		t.Fatalf("sync node-b: %v", err)
	}

	result, err := ExecuteFailoverFlow(master, "transport-vol", 2, []FailoverTarget{
		mustHybridFailoverTarget(t, nodeB, transport),
		mustHybridFailoverTarget(t, nodeC, transport),
	})
	if err != nil {
		t.Fatalf("transport-backed failover flow: %v", err)
	}
	if result.Assignment.NodeID != "node-b" {
		t.Fatalf("assignment node=%q, want node-b", result.Assignment.NodeID)
	}
	if result.Truth.PrimaryNodeID != "node-b" {
		t.Fatalf("truth primary=%q, want node-b", result.Truth.PrimaryNodeID)
	}
}

func TestTransportEvidenceAdapter_GatedFailoverFlow(t *testing.T) {
	master := masterv2.New(masterv2.Config{})
	transport := NewInMemoryFailoverEvidenceTransport()
	nodeB, err := New(Config{NodeID: "node-b"})
	if err != nil {
		t.Fatalf("new node-b: %v", err)
	}
	defer nodeB.Close()
	if err := transport.RegisterHandler("node-b", nodeB); err != nil {
		t.Fatalf("register node-b handler: %v", err)
	}

	tempDir := t.TempDir()
	pathB := filepath.Join(tempDir, "transport-gated-b.blk")
	if err := master.DeclarePrimary(masterv2.VolumeSpec{
		Name:          "transport-gated-vol",
		Path:          pathB,
		PrimaryNodeID: "node-a",
		CreateOptions: testCreateOptions(),
	}); err != nil {
		t.Fatalf("declare primary: %v", err)
	}
	if err := nodeB.ApplyAssignments([]masterv2.Assignment{{
		Name:          "transport-gated-vol",
		Path:          pathB,
		NodeID:        "node-b",
		Epoch:         2,
		LeaseTTL:      30 * time.Second,
		CreateOptions: testCreateOptions(),
		Role:          "primary",
	}}); err != nil {
		t.Fatalf("seed node-b: %v", err)
	}

	_, err = ExecuteFailoverFlow(master, "transport-gated-vol", 2, []FailoverTarget{
		mustHybridFailoverTarget(t, nodeB, transport),
		staticFailoverTarget(
			"node-c",
			masterv2.PromotionQueryResponse{
				VolumeName:   "transport-gated-vol",
				NodeID:       "node-c",
				Epoch:        1,
				CommittedLSN: 1,
				WALHeadLSN:   1,
				Eligible:     false,
				Reason:       "needs_rebuild",
			},
			protocolv2.ReplicaSummaryResponse{
				VolumeName:        "transport-gated-vol",
				NodeID:            "node-c",
				Epoch:             1,
				Role:              "replica",
				Mode:              "needs_rebuild",
				CommittedLSN:      3,
				DurableLSN:        2,
				CheckpointLSN:     1,
				RecoveryPhase:     "needs_rebuild",
				LastBarrierOK:     false,
				LastBarrierReason: "timeout",
				Eligible:          false,
				Reason:            "needs_rebuild",
			},
		),
	})
	if err == nil {
		t.Fatal("expected gated transport-backed failover error")
	}
}

func TestHTTPTransportEvidenceAdapter_HealthyFailoverFlow(t *testing.T) {
	master := masterv2.New(masterv2.Config{})
	transport := NewHTTPFailoverEvidenceTransport()
	defer transport.Close()
	nodeB, err := New(Config{NodeID: "node-b"})
	if err != nil {
		t.Fatalf("new node-b: %v", err)
	}
	defer nodeB.Close()
	nodeC, err := New(Config{NodeID: "node-c"})
	if err != nil {
		t.Fatalf("new node-c: %v", err)
	}
	defer nodeC.Close()
	if err := transport.RegisterHandler("node-b", nodeB); err != nil {
		t.Fatalf("register node-b handler: %v", err)
	}
	if err := transport.RegisterHandler("node-c", nodeC); err != nil {
		t.Fatalf("register node-c handler: %v", err)
	}

	tempDir := t.TempDir()
	pathB := filepath.Join(tempDir, "http-transport-b.blk")
	pathC := filepath.Join(tempDir, "http-transport-c.blk")
	if err := master.DeclarePrimary(masterv2.VolumeSpec{
		Name:          "http-transport-vol",
		Path:          pathB,
		PrimaryNodeID: "node-a",
		CreateOptions: testCreateOptions(),
	}); err != nil {
		t.Fatalf("declare primary: %v", err)
	}
	if err := nodeB.ApplyAssignments([]masterv2.Assignment{{
		Name:          "http-transport-vol",
		Path:          pathB,
		NodeID:        "node-b",
		Epoch:         2,
		LeaseTTL:      30 * time.Second,
		CreateOptions: testCreateOptions(),
		Role:          "primary",
	}}); err != nil {
		t.Fatalf("seed node-b: %v", err)
	}
	if err := nodeC.ApplyAssignments([]masterv2.Assignment{{
		Name:          "http-transport-vol",
		Path:          pathC,
		NodeID:        "node-c",
		Epoch:         2,
		LeaseTTL:      30 * time.Second,
		CreateOptions: testCreateOptions(),
		Role:          "primary",
	}}); err != nil {
		t.Fatalf("seed node-c: %v", err)
	}
	if err := nodeB.WriteLBA("http-transport-vol", 0, bytes.Repeat([]byte{0x71}, 4096)); err != nil {
		t.Fatalf("write node-b: %v", err)
	}
	if err := nodeB.SyncCache("http-transport-vol"); err != nil {
		t.Fatalf("sync node-b: %v", err)
	}

	result, err := ExecuteFailoverFlow(master, "http-transport-vol", 2, []FailoverTarget{
		mustHybridFailoverTarget(t, nodeB, transport),
		mustHybridFailoverTarget(t, nodeC, transport),
	})
	if err != nil {
		t.Fatalf("http transport failover flow: %v", err)
	}
	if result.Assignment.NodeID != "node-b" {
		t.Fatalf("assignment node=%q, want node-b", result.Assignment.NodeID)
	}
	if result.Truth.PrimaryNodeID != "node-b" {
		t.Fatalf("truth primary=%q, want node-b", result.Truth.PrimaryNodeID)
	}
}

func TestHTTPTransportEvidenceAdapter_GatedFailoverFlow(t *testing.T) {
	master := masterv2.New(masterv2.Config{})
	transport := NewHTTPFailoverEvidenceTransport()
	defer transport.Close()
	nodeB, err := New(Config{NodeID: "node-b"})
	if err != nil {
		t.Fatalf("new node-b: %v", err)
	}
	defer nodeB.Close()
	if err := transport.RegisterHandler("node-b", nodeB); err != nil {
		t.Fatalf("register node-b handler: %v", err)
	}

	tempDir := t.TempDir()
	pathB := filepath.Join(tempDir, "http-transport-gated-b.blk")
	if err := master.DeclarePrimary(masterv2.VolumeSpec{
		Name:          "http-transport-gated-vol",
		Path:          pathB,
		PrimaryNodeID: "node-a",
		CreateOptions: testCreateOptions(),
	}); err != nil {
		t.Fatalf("declare primary: %v", err)
	}
	if err := nodeB.ApplyAssignments([]masterv2.Assignment{{
		Name:          "http-transport-gated-vol",
		Path:          pathB,
		NodeID:        "node-b",
		Epoch:         2,
		LeaseTTL:      30 * time.Second,
		CreateOptions: testCreateOptions(),
		Role:          "primary",
	}}); err != nil {
		t.Fatalf("seed node-b: %v", err)
	}

	_, err = ExecuteFailoverFlow(master, "http-transport-gated-vol", 2, []FailoverTarget{
		mustHybridFailoverTarget(t, nodeB, transport),
		staticFailoverTarget(
			"node-c",
			masterv2.PromotionQueryResponse{
				VolumeName:   "http-transport-gated-vol",
				NodeID:       "node-c",
				Epoch:        1,
				CommittedLSN: 1,
				WALHeadLSN:   1,
				Eligible:     false,
				Reason:       "needs_rebuild",
			},
			protocolv2.ReplicaSummaryResponse{
				VolumeName:        "http-transport-gated-vol",
				NodeID:            "node-c",
				Epoch:             1,
				Role:              "replica",
				Mode:              "needs_rebuild",
				CommittedLSN:      3,
				DurableLSN:        2,
				CheckpointLSN:     1,
				RecoveryPhase:     "needs_rebuild",
				LastBarrierOK:     false,
				LastBarrierReason: "timeout",
				Eligible:          false,
				Reason:            "needs_rebuild",
			},
		),
	})
	if err == nil {
		t.Fatal("expected gated http transport failover error")
	}
}

func TestInProcessRuntimeManager_Loop2Service_RefreshesRF2Surface(t *testing.T) {
	master := masterv2.New(masterv2.Config{})
	manager, err := NewInProcessRuntimeManagerWithEvidenceTransport(master, NewHTTPFailoverEvidenceTransport())
	if err != nil {
		t.Fatalf("new runtime manager: %v", err)
	}
	defer manager.Close()

	nodeB, err := New(Config{NodeID: "node-b"})
	if err != nil {
		t.Fatalf("new node-b: %v", err)
	}
	defer nodeB.Close()
	nodeC, err := New(Config{NodeID: "node-c"})
	if err != nil {
		t.Fatalf("new node-c: %v", err)
	}
	defer nodeC.Close()
	if err := manager.RegisterNode(nodeB); err != nil {
		t.Fatalf("register node-b: %v", err)
	}
	if err := manager.RegisterNode(nodeC); err != nil {
		t.Fatalf("register node-c: %v", err)
	}

	tempDir := t.TempDir()
	pathB := filepath.Join(tempDir, "loop2-service-b.blk")
	pathC := filepath.Join(tempDir, "loop2-service-c.blk")
	if err := master.DeclarePrimary(masterv2.VolumeSpec{
		Name:          "loop2-service-vol",
		Path:          pathB,
		PrimaryNodeID: "node-a",
		CreateOptions: testCreateOptions(),
	}); err != nil {
		t.Fatalf("declare primary: %v", err)
	}
	if err := nodeB.ApplyAssignments([]masterv2.Assignment{{
		Name:          "loop2-service-vol",
		Path:          pathB,
		NodeID:        "node-b",
		Epoch:         2,
		LeaseTTL:      30 * time.Second,
		CreateOptions: testCreateOptions(),
		Role:          "primary",
	}}); err != nil {
		t.Fatalf("seed node-b: %v", err)
	}
	if err := nodeC.ApplyAssignments([]masterv2.Assignment{{
		Name:          "loop2-service-vol",
		Path:          pathC,
		NodeID:        "node-c",
		Epoch:         2,
		LeaseTTL:      30 * time.Second,
		CreateOptions: testCreateOptions(),
		Role:          "primary",
	}}); err != nil {
		t.Fatalf("seed node-c: %v", err)
	}

	handle, err := manager.StartLoop2Service(Loop2ServiceConfig{
		VolumeName:    "loop2-service-vol",
		PrimaryNodeID: "node-b",
		ExpectedEpoch: 2,
		NodeIDs:       []string{"node-b", "node-c"},
		Interval:      10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("start loop2 service: %v", err)
	}
	defer func() {
		handle.Stop()
		handle.Wait()
	}()

	waitForCondition(t, time.Second, func() bool {
		surface, ok := manager.RF2VolumeSurface("loop2-service-vol")
		return ok && surface.HasLoop2 && surface.Mode == RF2SurfaceModeHealthy
	})
}

func TestInProcessRuntimeManager_AutoFailoverService_TriggersOnPrimaryLoss(t *testing.T) {
	master := masterv2.New(masterv2.Config{})
	manager, err := NewInProcessRuntimeManagerWithEvidenceTransport(master, NewHTTPFailoverEvidenceTransport())
	if err != nil {
		t.Fatalf("new runtime manager: %v", err)
	}
	defer manager.Close()

	nodeB, err := New(Config{NodeID: "node-b"})
	if err != nil {
		t.Fatalf("new node-b: %v", err)
	}
	defer nodeB.Close()
	nodeC, err := New(Config{NodeID: "node-c"})
	if err != nil {
		t.Fatalf("new node-c: %v", err)
	}
	defer nodeC.Close()
	if err := manager.RegisterNode(nodeB); err != nil {
		t.Fatalf("register node-b: %v", err)
	}
	if err := manager.RegisterNode(nodeC); err != nil {
		t.Fatalf("register node-c: %v", err)
	}

	tempDir := t.TempDir()
	pathB := filepath.Join(tempDir, "auto-failover-b.blk")
	pathC := filepath.Join(tempDir, "auto-failover-c.blk")
	if err := master.DeclarePrimary(masterv2.VolumeSpec{
		Name:          "auto-failover-vol",
		Path:          pathB,
		PrimaryNodeID: "node-a",
		CreateOptions: testCreateOptions(),
	}); err != nil {
		t.Fatalf("declare primary: %v", err)
	}
	if err := nodeB.ApplyAssignments([]masterv2.Assignment{{
		Name:          "auto-failover-vol",
		Path:          pathB,
		NodeID:        "node-b",
		Epoch:         2,
		LeaseTTL:      30 * time.Second,
		CreateOptions: testCreateOptions(),
		Role:          "primary",
	}}); err != nil {
		t.Fatalf("seed node-b: %v", err)
	}
	if err := nodeC.ApplyAssignments([]masterv2.Assignment{{
		Name:          "auto-failover-vol",
		Path:          pathC,
		NodeID:        "node-c",
		Epoch:         2,
		LeaseTTL:      30 * time.Second,
		CreateOptions: testCreateOptions(),
		Role:          "primary",
	}}); err != nil {
		t.Fatalf("seed node-c: %v", err)
	}

	handle, err := manager.StartAutoFailoverService(AutoFailoverConfig{
		VolumeName:    "auto-failover-vol",
		PrimaryNodeID: "node-b",
		ExpectedEpoch: 2,
		NodeIDs:       []string{"node-b", "node-c"},
		Interval:      10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("start auto failover service: %v", err)
	}
	defer func() {
		handle.Stop()
		handle.Wait()
	}()

	manager.DisconnectEvidenceNode("node-b")
	waitForCondition(t, time.Second, func() bool {
		result, ok := manager.FailoverResult("auto-failover-vol")
		return ok && result.Assignment.NodeID == "node-c"
	})
	if handle.PrimaryNodeID() != "node-c" {
		t.Fatalf("tracked primary=%q, want node-c", handle.PrimaryNodeID())
	}
}

func TestInProcessRuntimeManager_AutoFailoverService_DoesNotTriggerOnCatchingUpReplica(t *testing.T) {
	master := masterv2.New(masterv2.Config{})
	manager, err := NewInProcessRuntimeManagerWithEvidenceTransport(master, NewHTTPFailoverEvidenceTransport())
	if err != nil {
		t.Fatalf("new runtime manager: %v", err)
	}
	defer manager.Close()

	nodeB, err := New(Config{NodeID: "node-b"})
	if err != nil {
		t.Fatalf("new node-b: %v", err)
	}
	defer nodeB.Close()
	if err := manager.RegisterNode(nodeB); err != nil {
		t.Fatalf("register node-b: %v", err)
	}

	tempDir := t.TempDir()
	pathB := filepath.Join(tempDir, "auto-suppress-b.blk")
	if err := master.DeclarePrimary(masterv2.VolumeSpec{
		Name:          "auto-suppress-vol",
		Path:          pathB,
		PrimaryNodeID: "node-a",
		CreateOptions: testCreateOptions(),
	}); err != nil {
		t.Fatalf("declare primary: %v", err)
	}
	if err := nodeB.ApplyAssignments([]masterv2.Assignment{{
		Name:          "auto-suppress-vol",
		Path:          pathB,
		NodeID:        "node-b",
		Epoch:         2,
		LeaseTTL:      30 * time.Second,
		CreateOptions: testCreateOptions(),
		Role:          "primary",
	}}); err != nil {
		t.Fatalf("seed node-b: %v", err)
	}
	if err := manager.RegisterTarget(staticFailoverTarget(
		"node-c",
		masterv2.PromotionQueryResponse{
			VolumeName:    "auto-suppress-vol",
			NodeID:        "node-c",
			Epoch:         2,
			CommittedLSN:  1,
			WALHeadLSN:    1,
			ReceiverReady: true,
			Eligible:      true,
		},
		protocolv2.ReplicaSummaryResponse{
			VolumeName:    "auto-suppress-vol",
			NodeID:        "node-c",
			Epoch:         2,
			Role:          "replica",
			CommittedLSN:  1,
			DurableLSN:    1,
			CheckpointLSN: 1,
			TargetLSN:     2,
			AchievedLSN:   1,
			RecoveryPhase: "catching_up",
			Eligible:      true,
			LastBarrierOK: true,
		},
	)); err != nil {
		t.Fatalf("register node-c target: %v", err)
	}

	handle, err := manager.StartAutoFailoverService(AutoFailoverConfig{
		VolumeName:    "auto-suppress-vol",
		PrimaryNodeID: "node-b",
		ExpectedEpoch: 2,
		NodeIDs:       []string{"node-b", "node-c"},
		Interval:      10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("start auto failover service: %v", err)
	}
	defer func() {
		handle.Stop()
		handle.Wait()
	}()

	time.Sleep(120 * time.Millisecond)
	if _, ok := manager.FailoverResult("auto-suppress-vol"); ok {
		t.Fatal("unexpected auto failover result on catching_up replica")
	}
	surface, ok := manager.RF2VolumeSurface("auto-suppress-vol")
	if !ok {
		t.Fatal("expected rf2 volume surface")
	}
	if surface.Mode != RF2SurfaceModeCatchingUp {
		t.Fatalf("surface mode=%q, want %q", surface.Mode, RF2SurfaceModeCatchingUp)
	}
}

func TestInProcessRuntimeManager_ExportVolumeISCSI_BindsFrontendToRuntimeNode(t *testing.T) {
	master := masterv2.New(masterv2.Config{})
	manager, err := NewInProcessRuntimeManager(master)
	if err != nil {
		t.Fatalf("new runtime manager: %v", err)
	}
	defer manager.Close()

	nodeB, err := New(Config{NodeID: "node-b"})
	if err != nil {
		t.Fatalf("new node-b: %v", err)
	}
	defer nodeB.Close()
	if err := manager.RegisterNode(nodeB); err != nil {
		t.Fatalf("register node-b: %v", err)
	}

	pathB := filepath.Join(t.TempDir(), "frontend-runtime-b.blk")
	if err := master.DeclarePrimary(masterv2.VolumeSpec{
		Name:          "frontend-runtime-vol",
		Path:          pathB,
		PrimaryNodeID: "node-a",
		CreateOptions: testCreateOptions(),
	}); err != nil {
		t.Fatalf("declare primary: %v", err)
	}
	if err := nodeB.ApplyAssignments([]masterv2.Assignment{{
		Name:          "frontend-runtime-vol",
		Path:          pathB,
		NodeID:        "node-b",
		Epoch:         2,
		LeaseTTL:      30 * time.Second,
		CreateOptions: testCreateOptions(),
		Role:          "primary",
	}}); err != nil {
		t.Fatalf("seed node-b: %v", err)
	}

	export, err := manager.ExportVolumeISCSI("frontend-runtime-vol", "node-b", "127.0.0.1:0", "iqn.2026-04.com.seaweedfs:test.frontend-runtime-vol")
	if err != nil {
		t.Fatalf("export volume iscsi: %v", err)
	}
	snapshot, ok := manager.ISCSIExport("frontend-runtime-vol")
	if !ok {
		t.Fatal("expected managed iscsi export snapshot")
	}
	if snapshot.NodeID != "node-b" {
		t.Fatalf("snapshot node=%q, want node-b", snapshot.NodeID)
	}

	conn := mustLoginISCSI(t, export.Address, export.IQN)
	defer conn.Close()

	writeData := bytes.Repeat([]byte{0x58}, 4096)
	var writeCDB [16]byte
	writeCDB[0] = iscsi.ScsiWrite10
	binary.BigEndian.PutUint32(writeCDB[2:6], 0)
	binary.BigEndian.PutUint16(writeCDB[7:9], 1)
	resp := sendSCSICmd(t, conn, writeCDB, 2, false, true, writeData, uint32(len(writeData)))
	if resp.SCSIStatus() != iscsi.SCSIStatusGood {
		t.Fatalf("iscsi write failed: status=%d", resp.SCSIStatus())
	}

	var syncCDB [16]byte
	syncCDB[0] = iscsi.ScsiSyncCache10
	resp = sendSCSICmd(t, conn, syncCDB, 3, false, false, nil, 0)
	if resp.SCSIStatus() != iscsi.SCSIStatusGood {
		t.Fatalf("iscsi sync cache failed: status=%d", resp.SCSIStatus())
	}

	readBack, err := nodeB.ReadLBA("frontend-runtime-vol", 0, uint32(len(writeData)))
	if err != nil {
		t.Fatalf("backend read: %v", err)
	}
	if !bytes.Equal(readBack, writeData) {
		t.Fatal("backend readback mismatch")
	}
}

func TestInProcessRuntimeManager_RepairReplicaFromPrimary_ReturnsLoop2ToHealthy(t *testing.T) {
	master := masterv2.New(masterv2.Config{})
	manager, err := NewInProcessRuntimeManagerWithEvidenceTransport(master, NewHTTPFailoverEvidenceTransport())
	if err != nil {
		t.Fatalf("new runtime manager: %v", err)
	}
	defer manager.Close()

	nodeB, err := New(Config{NodeID: "node-b"})
	if err != nil {
		t.Fatalf("new node-b: %v", err)
	}
	defer nodeB.Close()
	nodeC, err := New(Config{NodeID: "node-c"})
	if err != nil {
		t.Fatalf("new node-c: %v", err)
	}
	defer nodeC.Close()
	if err := manager.RegisterNode(nodeB); err != nil {
		t.Fatalf("register node-b: %v", err)
	}
	if err := manager.RegisterNode(nodeC); err != nil {
		t.Fatalf("register node-c: %v", err)
	}

	tempDir := t.TempDir()
	pathB := filepath.Join(tempDir, "repair-b.blk")
	pathC := filepath.Join(tempDir, "repair-c.blk")
	if err := master.DeclarePrimary(masterv2.VolumeSpec{
		Name:          "repair-vol",
		Path:          pathB,
		PrimaryNodeID: "node-a",
		CreateOptions: testCreateOptions(),
	}); err != nil {
		t.Fatalf("declare primary: %v", err)
	}
	assignments := []masterv2.Assignment{
		{
			Name:          "repair-vol",
			Path:          pathB,
			NodeID:        "node-b",
			Epoch:         2,
			LeaseTTL:      30 * time.Second,
			CreateOptions: testCreateOptions(),
			Role:          "primary",
		},
		{
			Name:          "repair-vol",
			Path:          pathC,
			NodeID:        "node-c",
			Epoch:         2,
			LeaseTTL:      30 * time.Second,
			CreateOptions: testCreateOptions(),
			Role:          "primary",
		},
	}
	if err := nodeB.ApplyAssignments(assignments[:1]); err != nil {
		t.Fatalf("seed node-b: %v", err)
	}
	if err := nodeC.ApplyAssignments(assignments[1:]); err != nil {
		t.Fatalf("seed node-c: %v", err)
	}

	payload := bytes.Repeat([]byte{0x62}, 4096)
	if err := nodeB.WriteLBA("repair-vol", 0, payload); err != nil {
		t.Fatalf("write node-b: %v", err)
	}
	if err := nodeB.SyncCache("repair-vol"); err != nil {
		t.Fatalf("sync node-b: %v", err)
	}

	before, err := manager.ObserveLoop2("repair-vol", "node-b", 2, "node-b", "node-c")
	if err != nil {
		t.Fatalf("observe before repair: %v", err)
	}
	if before.Mode != Loop2RuntimeModeCatchingUp {
		t.Fatalf("before repair mode=%q, want %q", before.Mode, Loop2RuntimeModeCatchingUp)
	}

	result, err := manager.RepairReplicaFromPrimary("repair-vol", "node-b", "node-c", 2, uint32(len(payload)))
	if err != nil {
		t.Fatalf("repair replica from primary: %v", err)
	}
	if result.Loop2Before.Mode != Loop2RuntimeModeCatchingUp {
		t.Fatalf("repair before mode=%q, want %q", result.Loop2Before.Mode, Loop2RuntimeModeCatchingUp)
	}
	if result.Loop2After.Mode != Loop2RuntimeModeKeepUp {
		t.Fatalf("repair after mode=%q, want %q", result.Loop2After.Mode, Loop2RuntimeModeKeepUp)
	}
	if !result.DataMatch {
		t.Fatalf("repair data match=false: %+v", result)
	}
}

func TestInProcessRuntimeManager_EndToEndRF2Handoff_ContinuesIOOnNewPrimary(t *testing.T) {
	master := masterv2.New(masterv2.Config{})
	manager, err := NewInProcessRuntimeManagerWithEvidenceTransport(master, NewHTTPFailoverEvidenceTransport())
	if err != nil {
		t.Fatalf("new runtime manager: %v", err)
	}
	defer manager.Close()

	nodeB, err := New(Config{NodeID: "node-b"})
	if err != nil {
		t.Fatalf("new node-b: %v", err)
	}
	defer nodeB.Close()
	nodeC, err := New(Config{NodeID: "node-c"})
	if err != nil {
		t.Fatalf("new node-c: %v", err)
	}
	defer nodeC.Close()
	if err := manager.RegisterNode(nodeB); err != nil {
		t.Fatalf("register node-b: %v", err)
	}
	if err := manager.RegisterNode(nodeC); err != nil {
		t.Fatalf("register node-c: %v", err)
	}

	tempDir := t.TempDir()
	pathB := filepath.Join(tempDir, "e2e-b.blk")
	pathC := filepath.Join(tempDir, "e2e-c.blk")
	if err := master.DeclarePrimary(masterv2.VolumeSpec{
		Name:          "e2e-vol",
		Path:          pathB,
		PrimaryNodeID: "node-a",
		CreateOptions: testCreateOptions(),
	}); err != nil {
		t.Fatalf("declare primary: %v", err)
	}
	if err := nodeB.ApplyAssignments([]masterv2.Assignment{{
		Name:          "e2e-vol",
		Path:          pathB,
		NodeID:        "node-b",
		Epoch:         2,
		LeaseTTL:      30 * time.Second,
		CreateOptions: testCreateOptions(),
		Role:          "primary",
	}}); err != nil {
		t.Fatalf("seed node-b: %v", err)
	}
	if err := nodeC.ApplyAssignments([]masterv2.Assignment{{
		Name:          "e2e-vol",
		Path:          pathC,
		NodeID:        "node-c",
		Epoch:         2,
		LeaseTTL:      30 * time.Second,
		CreateOptions: testCreateOptions(),
		Role:          "primary",
	}}); err != nil {
		t.Fatalf("seed node-c: %v", err)
	}

	export, err := manager.ExportVolumeISCSI("e2e-vol", "node-b", "127.0.0.1:0", "iqn.2026-04.com.seaweedfs:test.e2e-vol.primary")
	if err != nil {
		t.Fatalf("export primary iscsi: %v", err)
	}
	conn := mustLoginISCSI(t, export.Address, export.IQN)
	defer conn.Close()

	firstPayload := bytes.Repeat([]byte{0x63}, 4096)
	var writeCDB [16]byte
	writeCDB[0] = iscsi.ScsiWrite10
	binary.BigEndian.PutUint32(writeCDB[2:6], 0)
	binary.BigEndian.PutUint16(writeCDB[7:9], 1)
	resp := sendSCSICmd(t, conn, writeCDB, 2, false, true, firstPayload, uint32(len(firstPayload)))
	if resp.SCSIStatus() != iscsi.SCSIStatusGood {
		t.Fatalf("initial iscsi write failed: status=%d", resp.SCSIStatus())
	}
	var syncCDB [16]byte
	syncCDB[0] = iscsi.ScsiSyncCache10
	resp = sendSCSICmd(t, conn, syncCDB, 3, false, false, nil, 0)
	if resp.SCSIStatus() != iscsi.SCSIStatusGood {
		t.Fatalf("initial iscsi sync failed: status=%d", resp.SCSIStatus())
	}

	if _, err := manager.RepairReplicaFromPrimary("e2e-vol", "node-b", "node-c", 2, uint32(len(firstPayload))); err != nil {
		t.Fatalf("repair before failover: %v", err)
	}

	handle, err := manager.StartAutoFailoverService(AutoFailoverConfig{
		VolumeName:    "e2e-vol",
		PrimaryNodeID: "node-b",
		ExpectedEpoch: 2,
		NodeIDs:       []string{"node-b", "node-c"},
		Interval:      10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("start auto failover service: %v", err)
	}
	defer func() {
		handle.Stop()
		handle.Wait()
	}()

	manager.DisconnectEvidenceNode("node-b")
	waitForCondition(t, time.Second, func() bool {
		result, ok := manager.FailoverResult("e2e-vol")
		return ok && result.Assignment.NodeID == "node-c"
	})

	exportAfter, err := manager.ExportCurrentPrimaryISCSI("e2e-vol", "127.0.0.1:0", "iqn.2026-04.com.seaweedfs:test.e2e-vol.failover")
	if err != nil {
		t.Fatalf("export new primary iscsi: %v", err)
	}
	connAfter := mustLoginISCSI(t, exportAfter.Address, exportAfter.IQN)
	defer connAfter.Close()

	var readCDB [16]byte
	readCDB[0] = iscsi.ScsiRead10
	binary.BigEndian.PutUint32(readCDB[2:6], 0)
	binary.BigEndian.PutUint16(readCDB[7:9], 1)
	resp = sendSCSICmd(t, connAfter, readCDB, 4, true, false, nil, uint32(len(firstPayload)))
	if resp.Opcode() != iscsi.OpSCSIDataIn {
		t.Fatalf("expected Data-In after failover, got %s", iscsi.OpcodeName(resp.Opcode()))
	}
	if !bytes.Equal(resp.DataSegment, firstPayload) {
		t.Fatal("post-failover readback mismatch")
	}

	secondPayload := bytes.Repeat([]byte{0x64}, 4096)
	resp = sendSCSICmd(t, connAfter, writeCDB, 5, false, true, secondPayload, uint32(len(secondPayload)))
	if resp.SCSIStatus() != iscsi.SCSIStatusGood {
		t.Fatalf("post-failover write failed: status=%d", resp.SCSIStatus())
	}
	resp = sendSCSICmd(t, connAfter, syncCDB, 6, false, false, nil, 0)
	if resp.SCSIStatus() != iscsi.SCSIStatusGood {
		t.Fatalf("post-failover sync failed: status=%d", resp.SCSIStatus())
	}
	finalRead, err := nodeC.ReadLBA("e2e-vol", 0, uint32(len(secondPayload)))
	if err != nil {
		t.Fatalf("node-c backend read: %v", err)
	}
	if !bytes.Equal(finalRead, secondPayload) {
		t.Fatal("post-failover backend readback mismatch")
	}
}

func TestInProcessRuntimeManager_EndToEndRF2Handoff_GatedReplicaStopsFailClosed(t *testing.T) {
	master := masterv2.New(masterv2.Config{})
	manager, err := NewInProcessRuntimeManagerWithEvidenceTransport(master, NewHTTPFailoverEvidenceTransport())
	if err != nil {
		t.Fatalf("new runtime manager: %v", err)
	}
	defer manager.Close()

	nodeB, err := New(Config{NodeID: "node-b"})
	if err != nil {
		t.Fatalf("new node-b: %v", err)
	}
	defer nodeB.Close()
	if err := manager.RegisterNode(nodeB); err != nil {
		t.Fatalf("register node-b: %v", err)
	}

	tempDir := t.TempDir()
	pathB := filepath.Join(tempDir, "e2e-gated-b.blk")
	if err := master.DeclarePrimary(masterv2.VolumeSpec{
		Name:          "e2e-gated-vol",
		Path:          pathB,
		PrimaryNodeID: "node-a",
		CreateOptions: testCreateOptions(),
	}); err != nil {
		t.Fatalf("declare primary: %v", err)
	}
	if err := nodeB.ApplyAssignments([]masterv2.Assignment{{
		Name:          "e2e-gated-vol",
		Path:          pathB,
		NodeID:        "node-b",
		Epoch:         2,
		LeaseTTL:      30 * time.Second,
		CreateOptions: testCreateOptions(),
		Role:          "primary",
	}}); err != nil {
		t.Fatalf("seed node-b: %v", err)
	}
	if err := manager.RegisterTarget(staticFailoverTarget(
		"node-c",
		masterv2.PromotionQueryResponse{
			VolumeName:   "e2e-gated-vol",
			NodeID:       "node-c",
			Epoch:        2,
			CommittedLSN: 1,
			WALHeadLSN:   1,
			Eligible:     false,
			Reason:       "needs_rebuild",
		},
		protocolv2.ReplicaSummaryResponse{
			VolumeName:        "e2e-gated-vol",
			NodeID:            "node-c",
			Epoch:             2,
			Role:              "replica",
			Mode:              "needs_rebuild",
			CommittedLSN:      1,
			DurableLSN:        1,
			CheckpointLSN:     1,
			RecoveryPhase:     "needs_rebuild",
			LastBarrierOK:     false,
			LastBarrierReason: "timeout",
			Eligible:          false,
			Reason:            "needs_rebuild",
		},
	)); err != nil {
		t.Fatalf("register node-c target: %v", err)
	}

	handle, err := manager.StartAutoFailoverService(AutoFailoverConfig{
		VolumeName:    "e2e-gated-vol",
		PrimaryNodeID: "node-b",
		ExpectedEpoch: 2,
		NodeIDs:       []string{"node-b", "node-c"},
		Interval:      10 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("start auto failover service: %v", err)
	}
	defer func() {
		handle.Stop()
		handle.Wait()
	}()

	manager.DisconnectEvidenceNode("node-b")
	waitForCondition(t, time.Second, func() bool {
		snap, ok := manager.FailoverSnapshot("e2e-gated-vol")
		return ok && snap.Stage == FailoverStageFailed
	})
	result, ok := manager.FailoverResult("e2e-gated-vol")
	if !ok {
		t.Fatal("expected recorded gated failover result slot")
	}
	if result.Assignment.NodeID != "" {
		t.Fatalf("unexpected successful assignment on gated handoff: %+v", result)
	}
}

func TestInProcessRuntimeManager_OperatorSurface_ExposesRuntimeOwnedViews(t *testing.T) {
	master := masterv2.New(masterv2.Config{})
	manager, err := NewInProcessRuntimeManager(master)
	if err != nil {
		t.Fatalf("new runtime manager: %v", err)
	}
	defer manager.Close()

	nodeB, err := New(Config{NodeID: "node-b"})
	if err != nil {
		t.Fatalf("new node-b: %v", err)
	}
	defer nodeB.Close()
	nodeC, err := New(Config{NodeID: "node-c"})
	if err != nil {
		t.Fatalf("new node-c: %v", err)
	}
	defer nodeC.Close()
	if err := manager.RegisterNode(nodeB); err != nil {
		t.Fatalf("register node-b: %v", err)
	}
	if err := manager.RegisterNode(nodeC); err != nil {
		t.Fatalf("register node-c: %v", err)
	}

	tempDir := t.TempDir()
	pathB := filepath.Join(tempDir, "operator-b.blk")
	pathC := filepath.Join(tempDir, "operator-c.blk")
	if err := master.DeclarePrimary(masterv2.VolumeSpec{
		Name:          "operator-vol",
		Path:          pathB,
		PrimaryNodeID: "node-a",
		CreateOptions: testCreateOptions(),
	}); err != nil {
		t.Fatalf("declare primary: %v", err)
	}
	if err := nodeB.ApplyAssignments([]masterv2.Assignment{{
		Name:          "operator-vol",
		Path:          pathB,
		NodeID:        "node-b",
		Epoch:         2,
		LeaseTTL:      30 * time.Second,
		CreateOptions: testCreateOptions(),
		Role:          "primary",
	}}); err != nil {
		t.Fatalf("seed node-b: %v", err)
	}
	if err := nodeC.ApplyAssignments([]masterv2.Assignment{{
		Name:          "operator-vol",
		Path:          pathC,
		NodeID:        "node-c",
		Epoch:         2,
		LeaseTTL:      30 * time.Second,
		CreateOptions: testCreateOptions(),
		Role:          "primary",
	}}); err != nil {
		t.Fatalf("seed node-c: %v", err)
	}
	if _, err := manager.ObserveLoop2("operator-vol", "node-b", 2, "node-b", "node-c"); err != nil {
		t.Fatalf("observe loop2: %v", err)
	}
	if _, err := manager.ExportVolumeISCSI("operator-vol", "node-b", "127.0.0.1:0", "iqn.2026-04.com.seaweedfs:test.operator-vol"); err != nil {
		t.Fatalf("export iscsi: %v", err)
	}

	handle, err := manager.StartOperatorSurface("127.0.0.1:0")
	if err != nil {
		t.Fatalf("start operator surface: %v", err)
	}
	defer handle.Close()

	resp, err := http.Get("http://" + handle.Address() + "/v1/volumes/operator-vol/rf2")
	if err != nil {
		t.Fatalf("get rf2 surface: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("rf2 status=%d, want 200", resp.StatusCode)
	}
	var rf2Surface RF2VolumeSurface
	if err := json.NewDecoder(resp.Body).Decode(&rf2Surface); err != nil {
		t.Fatalf("decode rf2 surface: %v", err)
	}
	if rf2Surface.Mode != RF2SurfaceModeHealthy {
		t.Fatalf("rf2 surface mode=%q, want %q", rf2Surface.Mode, RF2SurfaceModeHealthy)
	}

	resp, err = http.Get("http://" + handle.Address() + "/v1/volumes/operator-vol/frontend")
	if err != nil {
		t.Fatalf("get frontend surface: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("frontend status=%d, want 200", resp.StatusCode)
	}
	var frontend ManagedISCSIExportSnapshot
	if err := json.NewDecoder(resp.Body).Decode(&frontend); err != nil {
		t.Fatalf("decode frontend surface: %v", err)
	}
	if frontend.NodeID != "node-b" {
		t.Fatalf("frontend node=%q, want node-b", frontend.NodeID)
	}
}

func TestLoop2RuntimeSession_KeepUpOnHealthyReplicaSet(t *testing.T) {
	nodeB, err := New(Config{NodeID: "node-b"})
	if err != nil {
		t.Fatalf("new node-b: %v", err)
	}
	defer nodeB.Close()

	tempDir := t.TempDir()
	pathB := filepath.Join(tempDir, "loop2-keepup-b.blk")
	assignB := masterv2.Assignment{
		Name:          "loop2-keepup-vol",
		Path:          pathB,
		NodeID:        "node-b",
		Epoch:         3,
		LeaseTTL:      30 * time.Second,
		CreateOptions: testCreateOptions(),
		Role:          "primary",
	}
	if err := nodeB.ApplyAssignments([]masterv2.Assignment{assignB}); err != nil {
		t.Fatalf("seed node-b: %v", err)
	}
	payload := bytes.Repeat([]byte{0x71}, 4096)
	if err := nodeB.WriteLBA("loop2-keepup-vol", 0, payload); err != nil {
		t.Fatalf("write node-b: %v", err)
	}
	if err := nodeB.SyncCache("loop2-keepup-vol"); err != nil {
		t.Fatalf("sync node-b: %v", err)
	}
	primary := mustReplicaSummary(t, nodeB, "loop2-keepup-vol", 3)

	session, err := NewLoop2RuntimeSession("loop2-keepup-vol", "node-b", 3, []FailoverTarget{
		mustInProcessFailoverTarget(t, nodeB),
		staticFailoverTarget(
			"node-c",
			masterv2.PromotionQueryResponse{VolumeName: "loop2-keepup-vol", NodeID: "node-c"},
			protocolv2.ReplicaSummaryResponse{
				VolumeName:    "loop2-keepup-vol",
				NodeID:        "node-c",
				Epoch:         3,
				Role:          "replica",
				Mode:          "replica_ready",
				RoleApplied:   true,
				ReceiverReady: true,
				CommittedLSN:  primary.CommittedLSN,
				DurableLSN:    primary.DurableLSN,
				CheckpointLSN: primary.CheckpointLSN,
				LastBarrierOK: true,
				Eligible:      true,
			},
		),
	})
	if err != nil {
		t.Fatalf("new loop2 runtime session: %v", err)
	}
	snap, err := session.ObserveOnce()
	if err != nil {
		t.Fatalf("observe loop2 keepup: %v", err)
	}
	if snap.Mode != Loop2RuntimeModeKeepUp {
		t.Fatalf("loop2 mode=%q, want %q", snap.Mode, Loop2RuntimeModeKeepUp)
	}
	if snap.HealthyReplicaCount != 2 {
		t.Fatalf("healthy replicas=%d, want 2", snap.HealthyReplicaCount)
	}
}

func TestInProcessRuntimeManager_ObserveLoop2_CatchingUp(t *testing.T) {
	master := masterv2.New(masterv2.Config{})
	manager, err := NewInProcessRuntimeManager(master)
	if err != nil {
		t.Fatalf("new runtime manager: %v", err)
	}
	nodeB, err := New(Config{NodeID: "node-b"})
	if err != nil {
		t.Fatalf("new node-b: %v", err)
	}
	defer nodeB.Close()
	if err := manager.RegisterNode(nodeB); err != nil {
		t.Fatalf("register node-b: %v", err)
	}

	tempDir := t.TempDir()
	pathB := filepath.Join(tempDir, "loop2-catchup-b.blk")
	assignB := masterv2.Assignment{
		Name:          "loop2-catchup-vol",
		Path:          pathB,
		NodeID:        "node-b",
		Epoch:         4,
		LeaseTTL:      30 * time.Second,
		CreateOptions: testCreateOptions(),
		Role:          "primary",
	}
	if err := nodeB.ApplyAssignments([]masterv2.Assignment{assignB}); err != nil {
		t.Fatalf("seed node-b: %v", err)
	}
	payload := bytes.Repeat([]byte{0x72}, 4096)
	if err := nodeB.WriteLBA("loop2-catchup-vol", 0, payload); err != nil {
		t.Fatalf("write node-b: %v", err)
	}
	if err := nodeB.SyncCache("loop2-catchup-vol"); err != nil {
		t.Fatalf("sync node-b: %v", err)
	}
	primary := mustReplicaSummary(t, nodeB, "loop2-catchup-vol", 4)

	if err := manager.RegisterTarget(staticFailoverTarget(
		"node-c",
		masterv2.PromotionQueryResponse{VolumeName: "loop2-catchup-vol", NodeID: "node-c"},
		protocolv2.ReplicaSummaryResponse{
			VolumeName:    "loop2-catchup-vol",
			NodeID:        "node-c",
			Epoch:         4,
			Role:          "replica",
			Mode:          "replica_ready",
			RoleApplied:   true,
			ReceiverReady: true,
			CommittedLSN:  primary.CommittedLSN - 1,
			DurableLSN:    primary.CommittedLSN - 1,
			CheckpointLSN: primary.CheckpointLSN,
			TargetLSN:     primary.CommittedLSN,
			AchievedLSN:   primary.CommittedLSN - 1,
			RecoveryPhase: "catching_up",
			LastBarrierOK: true,
			Eligible:      true,
		},
	)); err != nil {
		t.Fatalf("register node-c target: %v", err)
	}

	snap, err := manager.ObserveLoop2("loop2-catchup-vol", "node-b", 4)
	if err != nil {
		t.Fatalf("observe loop2 catchup: %v", err)
	}
	if snap.Mode != Loop2RuntimeModeCatchingUp {
		t.Fatalf("loop2 mode=%q, want %q", snap.Mode, Loop2RuntimeModeCatchingUp)
	}
	if snap.Reason == "" {
		t.Fatal("expected loop2 catching_up reason")
	}
	lastSnap, ok := manager.LastLoop2Snapshot()
	if !ok {
		t.Fatal("expected last loop2 snapshot")
	}
	if lastSnap.Mode != Loop2RuntimeModeCatchingUp {
		t.Fatalf("last loop2 mode=%q, want %q", lastSnap.Mode, Loop2RuntimeModeCatchingUp)
	}
}

func TestInProcessRuntimeManager_ObserveLoop2_NeedsRebuild(t *testing.T) {
	master := masterv2.New(masterv2.Config{})
	manager, err := NewInProcessRuntimeManager(master)
	if err != nil {
		t.Fatalf("new runtime manager: %v", err)
	}
	nodeB, err := New(Config{NodeID: "node-b"})
	if err != nil {
		t.Fatalf("new node-b: %v", err)
	}
	defer nodeB.Close()
	if err := manager.RegisterNode(nodeB); err != nil {
		t.Fatalf("register node-b: %v", err)
	}

	tempDir := t.TempDir()
	pathB := filepath.Join(tempDir, "loop2-rebuild-b.blk")
	assignB := masterv2.Assignment{
		Name:          "loop2-rebuild-vol",
		Path:          pathB,
		NodeID:        "node-b",
		Epoch:         5,
		LeaseTTL:      30 * time.Second,
		CreateOptions: testCreateOptions(),
		Role:          "primary",
	}
	if err := nodeB.ApplyAssignments([]masterv2.Assignment{assignB}); err != nil {
		t.Fatalf("seed node-b: %v", err)
	}

	if err := manager.RegisterTarget(staticFailoverTarget(
		"node-c",
		masterv2.PromotionQueryResponse{VolumeName: "loop2-rebuild-vol", NodeID: "node-c"},
		protocolv2.ReplicaSummaryResponse{
			VolumeName:        "loop2-rebuild-vol",
			NodeID:            "node-c",
			Epoch:             5,
			Role:              "replica",
			Mode:              "needs_rebuild",
			RoleApplied:       true,
			ReceiverReady:     false,
			CommittedLSN:      1,
			DurableLSN:        1,
			CheckpointLSN:     1,
			RecoveryPhase:     "needs_rebuild",
			LastBarrierOK:     false,
			LastBarrierReason: "timeout",
			Eligible:          false,
			Reason:            "needs_rebuild",
		},
	)); err != nil {
		t.Fatalf("register node-c target: %v", err)
	}

	snap, err := manager.ObserveLoop2("loop2-rebuild-vol", "node-b", 5)
	if err != nil {
		t.Fatalf("observe loop2 needs_rebuild: %v", err)
	}
	if snap.Mode != Loop2RuntimeModeNeedsRebuild {
		t.Fatalf("loop2 mode=%q, want %q", snap.Mode, Loop2RuntimeModeNeedsRebuild)
	}
	if snap.Reason != "needs_rebuild" {
		t.Fatalf("loop2 reason=%q, want needs_rebuild", snap.Reason)
	}
	perVolSnap, ok := manager.Loop2Snapshot("loop2-rebuild-vol")
	if !ok {
		t.Fatal("expected per-volume loop2 snapshot")
	}
	if perVolSnap.Mode != Loop2RuntimeModeNeedsRebuild {
		t.Fatalf("per-volume loop2 mode=%q, want %q", perVolSnap.Mode, Loop2RuntimeModeNeedsRebuild)
	}
}

func TestInProcessRuntimeManager_ExecuteReplicatedContinuity_HappyPath(t *testing.T) {
	master := masterv2.New(masterv2.Config{})
	manager, err := NewInProcessRuntimeManager(master)
	if err != nil {
		t.Fatalf("new runtime manager: %v", err)
	}
	nodeB, err := New(Config{NodeID: "node-b"})
	if err != nil {
		t.Fatalf("new node-b: %v", err)
	}
	defer nodeB.Close()
	nodeC, err := New(Config{NodeID: "node-c"})
	if err != nil {
		t.Fatalf("new node-c: %v", err)
	}
	defer nodeC.Close()
	if err := manager.RegisterNode(nodeB); err != nil {
		t.Fatalf("register node-b: %v", err)
	}
	if err := manager.RegisterNode(nodeC); err != nil {
		t.Fatalf("register node-c: %v", err)
	}

	tempDir := t.TempDir()
	pathB := filepath.Join(tempDir, "continuity-b.blk")
	pathC := filepath.Join(tempDir, "continuity-c.blk")
	if err := master.DeclarePrimary(masterv2.VolumeSpec{
		Name:          "continuity-vol",
		Path:          pathB,
		PrimaryNodeID: "node-a",
		CreateOptions: testCreateOptions(),
	}); err != nil {
		t.Fatalf("declare primary: %v", err)
	}
	if err := nodeB.ApplyAssignments([]masterv2.Assignment{{
		Name:          "continuity-vol",
		Path:          pathB,
		NodeID:        "node-b",
		Epoch:         2,
		LeaseTTL:      30 * time.Second,
		CreateOptions: testCreateOptions(),
		Role:          "primary",
	}}); err != nil {
		t.Fatalf("seed node-b: %v", err)
	}
	if err := nodeC.ApplyAssignments([]masterv2.Assignment{{
		Name:          "continuity-vol",
		Path:          pathC,
		NodeID:        "node-c",
		Epoch:         2,
		LeaseTTL:      30 * time.Second,
		CreateOptions: testCreateOptions(),
		Role:          "primary",
	}}); err != nil {
		t.Fatalf("seed node-c: %v", err)
	}

	payload := bytes.Repeat([]byte{0x7A}, 4096)
	result, err := manager.ExecuteReplicatedContinuity("continuity-vol", "node-b", 2, []string{"node-c"}, 0, payload)
	if err != nil {
		t.Fatalf("execute replicated continuity: %v", err)
	}
	if result.Loop2BeforeFailover.Mode != Loop2RuntimeModeKeepUp {
		t.Fatalf("loop2 before failover=%q, want %q", result.Loop2BeforeFailover.Mode, Loop2RuntimeModeKeepUp)
	}
	if result.SelectedPrimaryNodeID != "node-c" {
		t.Fatalf("selected primary=%q, want node-c", result.SelectedPrimaryNodeID)
	}
	if !result.DataMatch {
		t.Fatalf("expected continuity data match: %+v", result)
	}
	if result.ReadBackLength != uint32(len(payload)) {
		t.Fatalf("readback length=%d, want %d", result.ReadBackLength, len(payload))
	}
}

func TestInProcessRuntimeManager_ExecuteReplicatedContinuity_GatedPath(t *testing.T) {
	master := masterv2.New(masterv2.Config{})
	manager, err := NewInProcessRuntimeManager(master)
	if err != nil {
		t.Fatalf("new runtime manager: %v", err)
	}
	nodeB, err := New(Config{NodeID: "node-b"})
	if err != nil {
		t.Fatalf("new node-b: %v", err)
	}
	defer nodeB.Close()
	nodeC, err := New(Config{NodeID: "node-c"})
	if err != nil {
		t.Fatalf("new node-c: %v", err)
	}
	defer nodeC.Close()
	if err := manager.RegisterNode(nodeB); err != nil {
		t.Fatalf("register node-b: %v", err)
	}
	if err := manager.RegisterNode(nodeC); err != nil {
		t.Fatalf("register node-c: %v", err)
	}

	tempDir := t.TempDir()
	pathB := filepath.Join(tempDir, "continuity-gated-b.blk")
	pathC := filepath.Join(tempDir, "continuity-gated-c.blk")
	if err := master.DeclarePrimary(masterv2.VolumeSpec{
		Name:          "continuity-gated-vol",
		Path:          pathB,
		PrimaryNodeID: "node-a",
		CreateOptions: testCreateOptions(),
	}); err != nil {
		t.Fatalf("declare primary: %v", err)
	}
	if err := nodeB.ApplyAssignments([]masterv2.Assignment{{
		Name:          "continuity-gated-vol",
		Path:          pathB,
		NodeID:        "node-b",
		Epoch:         2,
		LeaseTTL:      30 * time.Second,
		CreateOptions: testCreateOptions(),
		Role:          "primary",
	}}); err != nil {
		t.Fatalf("seed node-b: %v", err)
	}
	if err := nodeC.ApplyAssignments([]masterv2.Assignment{{
		Name:          "continuity-gated-vol",
		Path:          pathC,
		NodeID:        "node-c",
		Epoch:         2,
		LeaseTTL:      30 * time.Second,
		CreateOptions: testCreateOptions(),
		Role:          "primary",
	}}); err != nil {
		t.Fatalf("seed node-c: %v", err)
	}
	if err := manager.RegisterTarget(staticFailoverTarget(
		"node-c",
		masterv2.PromotionQueryResponse{
			VolumeName:   "continuity-gated-vol",
			NodeID:       "node-c",
			Epoch:        2,
			CommittedLSN: 1,
			WALHeadLSN:   1,
			Eligible:     false,
			Reason:       "needs_rebuild",
		},
		protocolv2.ReplicaSummaryResponse{
			VolumeName:        "continuity-gated-vol",
			NodeID:            "node-c",
			Epoch:             2,
			Role:              "replica",
			Mode:              "needs_rebuild",
			CommittedLSN:      1,
			DurableLSN:        1,
			CheckpointLSN:     1,
			RecoveryPhase:     "needs_rebuild",
			LastBarrierOK:     false,
			LastBarrierReason: "timeout",
			Eligible:          false,
			Reason:            "needs_rebuild",
		},
	)); err != nil {
		t.Fatalf("override node-c target: %v", err)
	}

	payload := bytes.Repeat([]byte{0x7B}, 4096)
	result, err := manager.ExecuteReplicatedContinuity("continuity-gated-vol", "node-b", 2, []string{"node-c"}, 0, payload)
	if err == nil {
		t.Fatal("expected gated replicated continuity error")
	}
	if result.Loop2BeforeFailover.Mode != Loop2RuntimeModeNeedsRebuild {
		t.Fatalf("loop2 before failover=%q, want %q", result.Loop2BeforeFailover.Mode, Loop2RuntimeModeNeedsRebuild)
	}
	if result.SelectedPrimaryNodeID != "" {
		t.Fatalf("selected primary=%q, want empty on gated continuity", result.SelectedPrimaryNodeID)
	}
	if result.DataMatch {
		t.Fatalf("unexpected continuity data match on gated path: %+v", result)
	}
}

func TestInProcessRuntimeManager_RF2VolumeSurface_HealthyPackage(t *testing.T) {
	master := masterv2.New(masterv2.Config{})
	manager, err := NewInProcessRuntimeManager(master)
	if err != nil {
		t.Fatalf("new runtime manager: %v", err)
	}
	nodeB, err := New(Config{NodeID: "node-b"})
	if err != nil {
		t.Fatalf("new node-b: %v", err)
	}
	defer nodeB.Close()
	nodeC, err := New(Config{NodeID: "node-c"})
	if err != nil {
		t.Fatalf("new node-c: %v", err)
	}
	defer nodeC.Close()
	if err := manager.RegisterNode(nodeB); err != nil {
		t.Fatalf("register node-b: %v", err)
	}
	if err := manager.RegisterNode(nodeC); err != nil {
		t.Fatalf("register node-c: %v", err)
	}

	tempDir := t.TempDir()
	pathB := filepath.Join(tempDir, "rf2-surface-b.blk")
	pathC := filepath.Join(tempDir, "rf2-surface-c.blk")
	if err := master.DeclarePrimary(masterv2.VolumeSpec{
		Name:          "rf2-surface-vol",
		Path:          pathB,
		PrimaryNodeID: "node-a",
		CreateOptions: testCreateOptions(),
	}); err != nil {
		t.Fatalf("declare primary: %v", err)
	}
	if err := nodeB.ApplyAssignments([]masterv2.Assignment{{
		Name:          "rf2-surface-vol",
		Path:          pathB,
		NodeID:        "node-b",
		Epoch:         2,
		LeaseTTL:      30 * time.Second,
		CreateOptions: testCreateOptions(),
		Role:          "primary",
	}}); err != nil {
		t.Fatalf("seed node-b: %v", err)
	}
	if err := nodeC.ApplyAssignments([]masterv2.Assignment{{
		Name:          "rf2-surface-vol",
		Path:          pathC,
		NodeID:        "node-c",
		Epoch:         2,
		LeaseTTL:      30 * time.Second,
		CreateOptions: testCreateOptions(),
		Role:          "primary",
	}}); err != nil {
		t.Fatalf("seed node-c: %v", err)
	}

	payload := bytes.Repeat([]byte{0x55}, 4096)
	if _, err := manager.ExecuteReplicatedContinuity("rf2-surface-vol", "node-b", 2, []string{"node-c"}, 0, payload); err != nil {
		t.Fatalf("execute replicated continuity: %v", err)
	}

	continuitySnap, ok := manager.ReplicatedContinuitySnapshot("rf2-surface-vol")
	if !ok {
		t.Fatal("expected continuity snapshot")
	}
	if continuitySnap.LastError != "" {
		t.Fatalf("continuity last_error=%q, want empty", continuitySnap.LastError)
	}

	surface, ok := manager.RF2VolumeSurface("rf2-surface-vol")
	if !ok {
		t.Fatal("expected rf2 volume surface")
	}
	if !surface.HasLoop2 || !surface.HasFailover || !surface.HasContinuity {
		t.Fatalf("surface source flags=%+v, want all true", surface)
	}
	if surface.Mode != RF2SurfaceModeHealthy {
		t.Fatalf("surface mode=%q, want %q", surface.Mode, RF2SurfaceModeHealthy)
	}
	if surface.ReplicationMode != Loop2RuntimeModeKeepUp {
		t.Fatalf("replication mode=%q, want %q", surface.ReplicationMode, Loop2RuntimeModeKeepUp)
	}
	if surface.FailoverStage != FailoverStageActivated {
		t.Fatalf("failover stage=%q, want %q", surface.FailoverStage, FailoverStageActivated)
	}
	if surface.FailoverNodeID != "node-c" {
		t.Fatalf("failover node=%q, want node-c", surface.FailoverNodeID)
	}
	if surface.ContinuityStatus != RF2ContinuityStatusProven {
		t.Fatalf("continuity status=%q, want %q", surface.ContinuityStatus, RF2ContinuityStatusProven)
	}
	if surface.ContinuityNodeID != "node-c" {
		t.Fatalf("continuity node=%q, want node-c", surface.ContinuityNodeID)
	}
}

func TestInProcessRuntimeManager_RF2VolumeSurface_GatedPackage(t *testing.T) {
	master := masterv2.New(masterv2.Config{})
	manager, err := NewInProcessRuntimeManager(master)
	if err != nil {
		t.Fatalf("new runtime manager: %v", err)
	}
	nodeB, err := New(Config{NodeID: "node-b"})
	if err != nil {
		t.Fatalf("new node-b: %v", err)
	}
	defer nodeB.Close()
	nodeC, err := New(Config{NodeID: "node-c"})
	if err != nil {
		t.Fatalf("new node-c: %v", err)
	}
	defer nodeC.Close()
	if err := manager.RegisterNode(nodeB); err != nil {
		t.Fatalf("register node-b: %v", err)
	}
	if err := manager.RegisterNode(nodeC); err != nil {
		t.Fatalf("register node-c: %v", err)
	}

	tempDir := t.TempDir()
	pathB := filepath.Join(tempDir, "rf2-surface-gated-b.blk")
	pathC := filepath.Join(tempDir, "rf2-surface-gated-c.blk")
	if err := master.DeclarePrimary(masterv2.VolumeSpec{
		Name:          "rf2-surface-gated-vol",
		Path:          pathB,
		PrimaryNodeID: "node-a",
		CreateOptions: testCreateOptions(),
	}); err != nil {
		t.Fatalf("declare primary: %v", err)
	}
	if err := nodeB.ApplyAssignments([]masterv2.Assignment{{
		Name:          "rf2-surface-gated-vol",
		Path:          pathB,
		NodeID:        "node-b",
		Epoch:         2,
		LeaseTTL:      30 * time.Second,
		CreateOptions: testCreateOptions(),
		Role:          "primary",
	}}); err != nil {
		t.Fatalf("seed node-b: %v", err)
	}
	if err := nodeC.ApplyAssignments([]masterv2.Assignment{{
		Name:          "rf2-surface-gated-vol",
		Path:          pathC,
		NodeID:        "node-c",
		Epoch:         2,
		LeaseTTL:      30 * time.Second,
		CreateOptions: testCreateOptions(),
		Role:          "primary",
	}}); err != nil {
		t.Fatalf("seed node-c: %v", err)
	}
	if err := manager.RegisterTarget(staticFailoverTarget(
		"node-c",
		masterv2.PromotionQueryResponse{
			VolumeName:   "rf2-surface-gated-vol",
			NodeID:       "node-c",
			Epoch:        2,
			CommittedLSN: 1,
			WALHeadLSN:   1,
			Eligible:     false,
			Reason:       "needs_rebuild",
		},
		protocolv2.ReplicaSummaryResponse{
			VolumeName:        "rf2-surface-gated-vol",
			NodeID:            "node-c",
			Epoch:             2,
			Role:              "replica",
			Mode:              "needs_rebuild",
			CommittedLSN:      1,
			DurableLSN:        1,
			CheckpointLSN:     1,
			RecoveryPhase:     "needs_rebuild",
			LastBarrierOK:     false,
			LastBarrierReason: "timeout",
			Eligible:          false,
			Reason:            "needs_rebuild",
		},
	)); err != nil {
		t.Fatalf("override node-c target: %v", err)
	}

	payload := bytes.Repeat([]byte{0x56}, 4096)
	if _, err := manager.ExecuteReplicatedContinuity("rf2-surface-gated-vol", "node-b", 2, []string{"node-c"}, 0, payload); err == nil {
		t.Fatal("expected gated replicated continuity error")
	}

	continuitySnap, ok := manager.ReplicatedContinuitySnapshot("rf2-surface-gated-vol")
	if !ok {
		t.Fatal("expected continuity snapshot")
	}
	if continuitySnap.LastError == "" {
		t.Fatal("expected continuity last_error")
	}

	surface, ok := manager.RF2VolumeSurface("rf2-surface-gated-vol")
	if !ok {
		t.Fatal("expected rf2 volume surface")
	}
	if surface.Mode != RF2SurfaceModeBlocked {
		t.Fatalf("surface mode=%q, want %q", surface.Mode, RF2SurfaceModeBlocked)
	}
	if surface.ReplicationMode != Loop2RuntimeModeNeedsRebuild {
		t.Fatalf("replication mode=%q, want %q", surface.ReplicationMode, Loop2RuntimeModeNeedsRebuild)
	}
	if surface.FailoverStage != FailoverStageFailed {
		t.Fatalf("failover stage=%q, want %q", surface.FailoverStage, FailoverStageFailed)
	}
	if surface.FailoverError == "" {
		t.Fatal("expected failover error")
	}
	if surface.ContinuityStatus != RF2ContinuityStatusFailed {
		t.Fatalf("continuity status=%q, want %q", surface.ContinuityStatus, RF2ContinuityStatusFailed)
	}
	if surface.ContinuityError == "" {
		t.Fatal("expected continuity error")
	}
}

func mustHeartbeat(t *testing.T, node *Node) masterv2.NodeHeartbeat {
	t.Helper()
	hb, err := node.Heartbeat()
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	return hb
}

func mustPromotionEvidence(t *testing.T, node *Node, volumeName string, epoch uint64) masterv2.PromotionQueryResponse {
	t.Helper()
	resp, err := node.QueryPromotionEvidence(masterv2.PromotionQueryRequest{
		VolumeName:    volumeName,
		ExpectedEpoch: epoch,
	})
	if err != nil {
		t.Fatalf("promotion evidence: %v", err)
	}
	return resp
}

func mustReplicaSummary(t *testing.T, node *Node, volumeName string, epoch uint64) protocolv2.ReplicaSummaryResponse {
	t.Helper()
	resp, err := node.QueryReplicaSummary(protocolv2.ReplicaSummaryRequest{
		VolumeName:    volumeName,
		ExpectedEpoch: epoch,
	})
	if err != nil {
		t.Fatalf("replica summary: %v", err)
	}
	return resp
}

func waitForCondition(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not reached before timeout")
}

func mustInProcessFailoverTarget(t *testing.T, node *Node) FailoverTarget {
	t.Helper()
	target, err := NewInProcessFailoverTarget(node)
	if err != nil {
		t.Fatalf("new in-process failover target: %v", err)
	}
	return target
}

func mustHybridFailoverTarget(t *testing.T, node *Node, transport FailoverEvidenceTransport) FailoverTarget {
	t.Helper()
	target, err := NewHybridInProcessFailoverTarget(node, transport)
	if err != nil {
		t.Fatalf("new hybrid failover target: %v", err)
	}
	return target
}

type staticReplicaSummarySource struct {
	resp protocolv2.ReplicaSummaryResponse
	err  error
}

func (s staticReplicaSummarySource) QueryReplicaSummary(protocolv2.ReplicaSummaryRequest) (protocolv2.ReplicaSummaryResponse, error) {
	return s.resp, s.err
}

type staticFailoverAdapter struct {
	promotion masterv2.PromotionQueryResponse
	summary   protocolv2.ReplicaSummaryResponse
	err       error
}

func staticFailoverTarget(nodeID string, promotion masterv2.PromotionQueryResponse, summary protocolv2.ReplicaSummaryResponse) FailoverTarget {
	adapter := staticFailoverAdapter{
		promotion: promotion,
		summary:   summary,
	}
	return FailoverTarget{
		NodeID:   nodeID,
		Evidence: adapter,
		Takeover: adapter,
	}
}

func (s staticFailoverAdapter) QueryPromotionEvidence(masterv2.PromotionQueryRequest) (masterv2.PromotionQueryResponse, error) {
	return s.promotion, s.err
}

func (s staticFailoverAdapter) QueryReplicaSummary(protocolv2.ReplicaSummaryRequest) (protocolv2.ReplicaSummaryResponse, error) {
	return s.summary, s.err
}

func (s staticFailoverAdapter) PreparePrimaryTakeover(PrimaryTakeoverPlan) (ReconstructedPrimaryTruth, error) {
	return ReconstructedPrimaryTruth{}, fmt.Errorf("static failover participant cannot prepare takeover")
}

func (s staticFailoverAdapter) GatePrimaryActivation(string, ReconstructedPrimaryTruth) error {
	return fmt.Errorf("static failover participant cannot gate activation")
}

func testCreateOptions() blockvol.CreateOptions {
	return blockvol.CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    256 * 1024,
	}
}
