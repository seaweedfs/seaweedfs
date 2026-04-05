package volumev2

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/sw-block/runtime/masterv2"
	"github.com/seaweedfs/seaweedfs/sw-block/runtime/protocolv2"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
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

	result, err := ExecuteFailoverFlow(master, "flow-vol", 2, []FailoverParticipant{nodeB, nodeC})
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

	result, err := ExecuteFailoverFlow(master, "flow-gated-vol", 2, []FailoverParticipant{
		nodeB,
		staticFailoverParticipant{
			promotion: masterv2.PromotionQueryResponse{
				VolumeName:   "flow-gated-vol",
				NodeID:       "node-c",
				Epoch:        1,
				CommittedLSN: 1,
				WALHeadLSN:   1,
				Eligible:     false,
				Reason:       "needs_rebuild",
			},
			summary: protocolv2.ReplicaSummaryResponse{
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
		},
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

	session, err := NewFailoverSession(master, "session-vol", 2, []FailoverParticipant{nodeB, nodeC})
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

	session, err := NewFailoverSession(master, "snapshot-gated-vol", 2, []FailoverParticipant{
		nodeB,
		staticFailoverParticipant{
			promotion: masterv2.PromotionQueryResponse{
				VolumeName:   "snapshot-gated-vol",
				NodeID:       "node-c",
				Epoch:        1,
				CommittedLSN: 1,
				WALHeadLSN:   1,
				Eligible:     false,
				Reason:       "needs_rebuild",
			},
			summary: protocolv2.ReplicaSummaryResponse{
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
		},
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
	if err := driver.RegisterParticipant("node-b", nodeB); err != nil {
		t.Fatalf("register node-b: %v", err)
	}
	if err := driver.RegisterParticipant("node-c", nodeC); err != nil {
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
	if err := driver.RegisterParticipant("node-b", nodeB); err != nil {
		t.Fatalf("register node-b: %v", err)
	}
	if err := driver.RegisterParticipant("node-c", staticFailoverParticipant{
		promotion: masterv2.PromotionQueryResponse{
			VolumeName:   "driver-gated-vol",
			NodeID:       "node-c",
			Epoch:        1,
			CommittedLSN: 1,
			WALHeadLSN:   1,
			Eligible:     false,
			Reason:       "needs_rebuild",
		},
		summary: protocolv2.ReplicaSummaryResponse{
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
	}); err != nil {
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

type staticReplicaSummarySource struct {
	resp protocolv2.ReplicaSummaryResponse
	err  error
}

func (s staticReplicaSummarySource) QueryReplicaSummary(protocolv2.ReplicaSummaryRequest) (protocolv2.ReplicaSummaryResponse, error) {
	return s.resp, s.err
}

type staticFailoverParticipant struct {
	promotion masterv2.PromotionQueryResponse
	summary   protocolv2.ReplicaSummaryResponse
	err       error
}

func (s staticFailoverParticipant) QueryPromotionEvidence(masterv2.PromotionQueryRequest) (masterv2.PromotionQueryResponse, error) {
	return s.promotion, s.err
}

func (s staticFailoverParticipant) QueryReplicaSummary(protocolv2.ReplicaSummaryRequest) (protocolv2.ReplicaSummaryResponse, error) {
	return s.summary, s.err
}

func (s staticFailoverParticipant) PreparePrimaryTakeover(PrimaryTakeoverPlan) (ReconstructedPrimaryTruth, error) {
	return ReconstructedPrimaryTruth{}, fmt.Errorf("static failover participant cannot prepare takeover")
}

func (s staticFailoverParticipant) GatePrimaryActivation(string, ReconstructedPrimaryTruth) error {
	return fmt.Errorf("static failover participant cannot gate activation")
}

func testCreateOptions() blockvol.CreateOptions {
	return blockvol.CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    256 * 1024,
	}
}
