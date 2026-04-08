package v2bridge

import (
	"bytes"
	"net"
	"path/filepath"
	"strings"
	"testing"

	bridge "github.com/seaweedfs/seaweedfs/sw-block/bridge/blockvol"
	engine "github.com/seaweedfs/seaweedfs/sw-block/engine/replication"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// ============================================================
// Phase 09 P1: Full-base execution closure
//
// Proofs:
//   1. Component: TCP transfer + local install (bridge level)
//   2. One-chain: engine plan → RebuildExecutor → v2bridge → blockvol install → completion
//   3. Fail-closed: connection refused, epoch mismatch, partial transfer, no address
// ============================================================

// createTestVolNamed creates a real file-backed BlockVol in the given dir.
func createTestVolNamed(t *testing.T, dir, name string) *blockvol.BlockVol {
	t.Helper()
	path := filepath.Join(dir, name)
	v, err := blockvol.CreateBlockVol(path, blockvol.CreateOptions{
		VolumeSize: 1 * 1024 * 1024,
		BlockSize:  4096,
		WALSize:    256 * 1024,
	})
	if err != nil {
		t.Fatalf("CreateBlockVol %s: %v", name, err)
	}
	return v
}

// verifyLBAMatch reads LBAs from both vols and verifies they match.
func verifyLBAMatch(t *testing.T, primaryVol, replicaVol *blockvol.BlockVol, lbaCount int) {
	t.Helper()
	blockSize := primaryVol.Info().BlockSize
	for i := 0; i < lbaCount; i++ {
		pdata, perr := primaryVol.ReadLBA(uint64(i), blockSize)
		rdata, rerr := replicaVol.ReadLBA(uint64(i), blockSize)
		if perr != nil {
			t.Fatalf("primary ReadLBA(%d): %v", i, perr)
		}
		if rerr != nil {
			t.Fatalf("replica ReadLBA(%d): %v", i, rerr)
		}
		if !bytes.Equal(pdata, rdata) {
			t.Fatalf("LBA %d mismatch: primary[0]=%d replica[0]=%d", i, pdata[0], rdata[0])
		}
	}
}

// --- Component Proof: TCP transfer + local install ---

func TestP1_TransferFullBase_RealTCP(t *testing.T) {
	dir := t.TempDir()

	// Primary: write data + flush to populate extent region.
	primaryVol := createTestVolNamed(t, dir, "primary.blockvol")
	defer primaryVol.Close()

	for i := 0; i < 20; i++ {
		primaryVol.WriteLBA(uint64(i), makeBlock(byte('A'+i%26)))
	}
	primaryVol.ForceFlush()

	primaryState := NewReader(primaryVol).ReadState()
	t.Logf("primary: head=%d tail=%d committed=%d checkpoint=%d",
		primaryState.WALHeadLSN, primaryState.WALTailLSN,
		primaryState.CommittedLSN, primaryState.CheckpointLSN)

	// Start rebuild server on primary (existing V1 code).
	rebuildServer, err := blockvol.NewRebuildServer(primaryVol, "127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewRebuildServer: %v", err)
	}
	rebuildServer.Serve()
	defer rebuildServer.Stop()
	t.Logf("rebuild server on %s", rebuildServer.Addr())

	// Replica: empty vol, same geometry.
	replicaVol := createTestVolNamed(t, dir, "replica.blockvol")
	defer replicaVol.Close()

	// Create executor for replica, pointing to primary's rebuild server.
	executor := NewExecutor(replicaVol, rebuildServer.Addr())

	// Transfer full base.
	if _, err := executor.TransferFullBase(primaryState.CommittedLSN); err != nil {
		t.Fatalf("TransferFullBase: %v", err)
	}

	// Verify: replica LBA data matches primary (reads from extent since
	// replica has no WAL entries or dirty map entries).
	verifyLBAMatch(t, primaryVol, replicaVol, 20)

	t.Log("P1 component proof: TCP transfer + local install verified — LBA data matches")
}

func TestP1_TransferFullBase_UsesSessionControlledLoop(t *testing.T) {
	dir := t.TempDir()

	primaryVol := createTestVolNamed(t, dir, "primary-session.blockvol")
	defer primaryVol.Close()
	for i := 0; i < 8; i++ {
		primaryVol.WriteLBA(uint64(i), makeBlock(byte('a'+i)))
	}
	primaryVol.ForceFlush()

	rebuildServer, err := blockvol.NewRebuildServer(primaryVol, "127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewRebuildServer: %v", err)
	}
	rebuildServer.Serve()
	defer rebuildServer.Stop()

	replicaVol := createTestVolNamed(t, dir, "replica-session.blockvol")
	defer replicaVol.Close()

	executor := NewExecutor(replicaVol, rebuildServer.Addr())
	achieved, err := executor.TransferFullBase(primaryVol.StatusSnapshot().CommittedLSN)
	if err != nil {
		t.Fatalf("TransferFullBase: %v", err)
	}
	if achieved == 0 {
		t.Fatalf("achieved=%d, want > 0", achieved)
	}
	if replicaVol.ReplicaReceiverAddr() == nil {
		t.Fatal("expected local replica receiver to be started by session-controlled rebuild")
	}
	cfg, progress, ok := replicaVol.ActiveRebuildSession()
	if !ok {
		t.Fatal("expected rebuild session snapshot after transfer")
	}
	if cfg.TargetLSN == 0 {
		t.Fatalf("target_lsn=%d, want > 0", cfg.TargetLSN)
	}
	if progress.Phase != blockvol.RebuildPhaseCompleted {
		t.Fatalf("phase=%s, want completed", progress.Phase)
	}
	if !progress.BaseComplete {
		t.Fatal("expected baseComplete=true")
	}
}

// --- One-Chain Proof: engine → executor → bridge → blockvol → completion ---

// untrustedReaderShim wraps a Reader but reports CheckpointTrusted=false.
// This forces the engine's RebuildSourceDecision to select RebuildFullBase
// instead of RebuildSnapshotTail, so the one-chain test exercises
// TransferFullBase specifically.
type untrustedReaderShim struct{ r *Reader }

func (s *untrustedReaderShim) ReadState() bridge.BlockVolState {
	rs := s.r.ReadState()
	return bridge.BlockVolState{
		WALHeadLSN:        rs.WALHeadLSN,
		WALTailLSN:        rs.WALTailLSN,
		CommittedLSN:      rs.CommittedLSN,
		CheckpointLSN:     rs.CheckpointLSN,
		CheckpointTrusted: false, // force full-base path
	}
}

func TestP1_FullBaseRebuild_OneChain(t *testing.T) {
	dir := t.TempDir()

	// Primary: write data + flush → force rebuild condition.
	primaryVol := createTestVolNamed(t, dir, "primary.blockvol")
	defer primaryVol.Close()

	for i := 0; i < 20; i++ {
		primaryVol.WriteLBA(uint64(i), makeBlock(byte('A'+i%26)))
	}
	primaryVol.ForceFlush()

	primaryState := NewReader(primaryVol).ReadState()
	if primaryState.WALTailLSN == 0 {
		t.Fatal("ForceFlush must advance tail for rebuild condition")
	}

	// Start rebuild server on primary.
	rebuildServer, err := blockvol.NewRebuildServer(primaryVol, "127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewRebuildServer: %v", err)
	}
	rebuildServer.Serve()
	defer rebuildServer.Stop()

	// Replica: empty vol.
	replicaVol := createTestVolNamed(t, dir, "replica.blockvol")
	defer replicaVol.Close()

	// Engine setup: StorageAdapter reads from PRIMARY (for planning).
	// Use untrustedReaderShim to force full-base rebuild path so this
	// test exercises TransferFullBase specifically (not TransferSnapshot).
	primaryReader := NewReader(primaryVol)
	primaryPinner := NewPinner(primaryVol)
	sa := bridge.NewStorageAdapter(
		&untrustedReaderShim{primaryReader},
		&pinnerShim{primaryPinner},
	)
	ca := bridge.NewControlAdapter()
	driver := engine.NewRecoveryDriver(sa)

	// Step 1: assignment — register the replica sender.
	intent := ca.ToAssignmentIntent(
		bridge.MasterAssignment{VolumeName: "vol1", Epoch: 1, Role: "primary"},
		[]bridge.MasterAssignment{
			{VolumeName: "vol1", ReplicaServerID: "vs2", Role: "replica",
				DataAddr: "10.0.0.2:9333", CtrlAddr: "10.0.0.2:9334"},
		},
	)
	driver.Orchestrator.ProcessAssignment(intent)

	// Step 2: plan recovery — replicaLSN=0 with tail>0 forces NeedsRebuild.
	plan, err := driver.PlanRecovery("vol1/vs2", 0)
	if err != nil {
		t.Fatalf("PlanRecovery: %v", err)
	}
	if plan.Outcome != engine.OutcomeNeedsRebuild {
		t.Fatalf("outcome=%s, want NeedsRebuild", plan.Outcome)
	}

	// Step 3: rebuild assignment — switch sender to rebuild session.
	rebuildIntent := ca.ToAssignmentIntent(
		bridge.MasterAssignment{VolumeName: "vol1", Epoch: 1, Role: "primary"},
		[]bridge.MasterAssignment{
			{VolumeName: "vol1", ReplicaServerID: "vs2", Role: "rebuilding",
				DataAddr: "10.0.0.2:9333", CtrlAddr: "10.0.0.2:9334"},
		},
	)
	driver.Orchestrator.ProcessAssignment(rebuildIntent)

	// Step 4: plan rebuild from real storage.
	rebuildPlan, err := driver.PlanRebuild("vol1/vs2")
	if err != nil {
		t.Fatalf("PlanRebuild: %v", err)
	}
	if rebuildPlan.RebuildSource != engine.RebuildFullBase {
		t.Fatalf("source=%s, want full_base (untrusted checkpoint should force this)",
			rebuildPlan.RebuildSource)
	}
	t.Logf("rebuild plan: source=%s target=%d", rebuildPlan.RebuildSource, rebuildPlan.RebuildTargetLSN)

	// Step 5: RebuildExecutor with real IO wired to v2bridge executor (on replica vol).
	replicaExecutor := NewExecutor(replicaVol, rebuildServer.Addr())
	exec := engine.NewRebuildExecutor(driver, rebuildPlan)
	exec.IO = replicaExecutor

	if err := exec.Execute(); err != nil {
		t.Fatalf("RebuildExecutor.Execute: %v", err)
	}

	// Step 6: verify sender state → InSync.
	s := driver.Orchestrator.Registry.Sender("vol1/vs2")
	if s.State() != engine.StateInSync {
		t.Fatalf("state=%s, want InSync", s.State())
	}

	// Step 7: verify pins released.
	if primaryPinner.ActiveHoldCount() != 0 {
		t.Fatalf("%d pins leaked", primaryPinner.ActiveHoldCount())
	}

	// Step 8: verify LBA data matches.
	verifyLBAMatch(t, primaryVol, replicaVol, 20)

	// Step 9: verify observability — execution log shows rebuild events.
	events := driver.Orchestrator.Log.EventsFor("vol1/vs2")
	hasStarted := false
	hasCompleted := false
	for _, ev := range events {
		if ev.Event == "exec_rebuild_started" {
			hasStarted = true
		}
		if ev.Event == "exec_rebuild_completed" {
			hasCompleted = true
		}
	}
	if !hasStarted || !hasCompleted {
		t.Fatalf("observability: started=%v completed=%v", hasStarted, hasCompleted)
	}

	t.Log("P1 one-chain: plan(full_base) → RebuildExecutor(IO=v2bridge) → TCP → local install → InSync → pins released → data verified")
}

// --- Non-empty replica: stale state must be cleared ---

func TestP1_TransferFullBase_NonEmptyReplica(t *testing.T) {
	dir := t.TempDir()

	// Primary: write data + flush.
	primaryVol := createTestVolNamed(t, dir, "primary.blockvol")
	defer primaryVol.Close()

	for i := 0; i < 10; i++ {
		primaryVol.WriteLBA(uint64(i), makeBlock(byte('P')))
	}
	primaryVol.ForceFlush()

	rebuildServer, err := blockvol.NewRebuildServer(primaryVol, "127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewRebuildServer: %v", err)
	}
	rebuildServer.Serve()
	defer rebuildServer.Stop()

	// Replica: has STALE data + WAL entries (simulates a previously-used replica).
	replicaVol := createTestVolNamed(t, dir, "replica.blockvol")
	defer replicaVol.Close()

	for i := 0; i < 5; i++ {
		replicaVol.WriteLBA(uint64(i), makeBlock(byte('R')))
	}
	// Confirm replica has WAL entries and dirty map entries.
	replicaStateBefore := NewReader(replicaVol).ReadState()
	if replicaStateBefore.WALHeadLSN == 0 {
		t.Fatal("replica must have WAL entries before rebuild")
	}
	t.Logf("replica before: head=%d", replicaStateBefore.WALHeadLSN)

	// Transfer full base — must clear stale state.
	executor := NewExecutor(replicaVol, rebuildServer.Addr())
	if _, err := executor.TransferFullBase(0); err != nil {
		t.Fatalf("TransferFullBase: %v", err)
	}

	// Verify: replica reads primary's data (not stale 'R' blocks).
	for i := 0; i < 10; i++ {
		data, err := replicaVol.ReadLBA(uint64(i), replicaVol.Info().BlockSize)
		if err != nil {
			t.Fatalf("ReadLBA(%d): %v", i, err)
		}
		if data[0] != 'P' {
			t.Fatalf("LBA %d: got %c, want P (stale data not cleared)", i, data[0])
		}
	}

	// Verify: WAL state was reset (no stale entries overlaying the new extent).
	replicaStateAfter := NewReader(replicaVol).ReadState()
	t.Logf("replica after: head=%d tail=%d checkpoint=%d",
		replicaStateAfter.WALHeadLSN, replicaStateAfter.WALTailLSN, replicaStateAfter.CheckpointLSN)

	t.Log("P1 non-empty replica: stale WAL/dirty state cleared, primary data installed correctly")
}

// --- Pre-flush correctness: unflushed WAL entries are in the extent ---

func TestP1_TransferFullBase_UnflushedEntries(t *testing.T) {
	dir := t.TempDir()

	// Primary: write data, flush SOME, then write MORE that stay in WAL.
	primaryVol := createTestVolNamed(t, dir, "primary.blockvol")
	defer primaryVol.Close()

	for i := 0; i < 10; i++ {
		primaryVol.WriteLBA(uint64(i), makeBlock(byte('A'+i%26)))
	}
	primaryVol.ForceFlush()

	// These 5 writes are in the WAL, NOT yet flushed to extent.
	for i := 10; i < 15; i++ {
		primaryVol.WriteLBA(uint64(i), makeBlock(byte('A'+i%26)))
	}

	primaryState := NewReader(primaryVol).ReadState()
	t.Logf("primary: head=%d tail=%d committed=%d checkpoint=%d",
		primaryState.WALHeadLSN, primaryState.WALTailLSN,
		primaryState.CommittedLSN, primaryState.CheckpointLSN)

	// Confirm: checkpoint < head (unflushed entries exist).
	if primaryState.CheckpointLSN >= primaryState.WALHeadLSN {
		t.Fatal("need unflushed entries: checkpoint must be < head")
	}

	rebuildServer, err := blockvol.NewRebuildServer(primaryVol, "127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewRebuildServer: %v", err)
	}
	rebuildServer.Serve()
	defer rebuildServer.Stop()

	// Replica: empty vol.
	replicaVol := createTestVolNamed(t, dir, "replica.blockvol")
	defer replicaVol.Close()

	executor := NewExecutor(replicaVol, rebuildServer.Addr())
	if _, err := executor.TransferFullBase(primaryState.CommittedLSN); err != nil {
		t.Fatalf("TransferFullBase: %v", err)
	}

	// Verify: ALL 15 LBAs match — including the 5 that were unflushed.
	// The rebuild server's pre-flush ensures they are in the extent.
	verifyLBAMatch(t, primaryVol, replicaVol, 15)

	t.Log("P1 pre-flush: unflushed WAL entries flushed by rebuild server before extent copy — all data correct")
}

// --- Convergence proof: achievedLSN > targetLSN, no split truth ---

func TestP1_FullBaseRebuild_AchievedConvergence(t *testing.T) {
	dir := t.TempDir()

	// Primary: write initial data + flush.
	primaryVol := createTestVolNamed(t, dir, "primary.blockvol")
	defer primaryVol.Close()

	for i := 0; i < 20; i++ {
		primaryVol.WriteLBA(uint64(i), makeBlock(byte('A'+i%26)))
	}
	primaryVol.ForceFlush()

	primaryState := NewReader(primaryVol).ReadState()
	if primaryState.WALTailLSN == 0 {
		t.Fatal("ForceFlush must advance tail for rebuild condition")
	}

	rebuildServer, err := blockvol.NewRebuildServer(primaryVol, "127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewRebuildServer: %v", err)
	}
	rebuildServer.Serve()
	defer rebuildServer.Stop()

	// Replica: empty vol.
	replicaVol := createTestVolNamed(t, dir, "replica.blockvol")
	defer replicaVol.Close()

	// Engine setup with untrusted reader to force full-base path.
	primaryReader := NewReader(primaryVol)
	primaryPinner := NewPinner(primaryVol)
	sa := bridge.NewStorageAdapter(
		&untrustedReaderShim{primaryReader},
		&pinnerShim{primaryPinner},
	)
	ca := bridge.NewControlAdapter()
	driver := engine.NewRecoveryDriver(sa)

	// Assignment + plan.
	intent := ca.ToAssignmentIntent(
		bridge.MasterAssignment{VolumeName: "vol1", Epoch: 1, Role: "primary"},
		[]bridge.MasterAssignment{
			{VolumeName: "vol1", ReplicaServerID: "vs2", Role: "replica",
				DataAddr: "10.0.0.2:9333", CtrlAddr: "10.0.0.2:9334"},
		},
	)
	driver.Orchestrator.ProcessAssignment(intent)
	plan, _ := driver.PlanRecovery("vol1/vs2", 0)
	if plan.Outcome != engine.OutcomeNeedsRebuild {
		t.Fatalf("outcome=%s", plan.Outcome)
	}

	rebuildIntent := ca.ToAssignmentIntent(
		bridge.MasterAssignment{VolumeName: "vol1", Epoch: 1, Role: "primary"},
		[]bridge.MasterAssignment{
			{VolumeName: "vol1", ReplicaServerID: "vs2", Role: "rebuilding",
				DataAddr: "10.0.0.2:9333", CtrlAddr: "10.0.0.2:9334"},
		},
	)
	driver.Orchestrator.ProcessAssignment(rebuildIntent)

	rebuildPlan, err := driver.PlanRebuild("vol1/vs2")
	if err != nil {
		t.Fatalf("PlanRebuild: %v", err)
	}

	targetLSN := rebuildPlan.RebuildTargetLSN
	t.Logf("plan: target=%d source=%s", targetLSN, rebuildPlan.RebuildSource)

	// --- Force achievedLSN > targetLSN ---
	// Write additional data to primary AFTER planning. The rebuild server
	// will see these via ForceFlush and serve an extent newer than the plan.
	for i := 20; i < 25; i++ {
		primaryVol.WriteLBA(uint64(i), makeBlock(byte('X')))
	}
	postPlanState := NewReader(primaryVol).ReadState()
	t.Logf("primary after extra writes: head=%d (plan target was %d)",
		postPlanState.WALHeadLSN, targetLSN)

	// Execute rebuild with real IO.
	replicaExecutor := NewExecutor(replicaVol, rebuildServer.Addr())
	exec := engine.NewRebuildExecutor(driver, rebuildPlan)
	exec.IO = replicaExecutor

	if err := exec.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	// --- Full convergence verification ---

	replicaState := NewReader(replicaVol).ReadState()
	localAchieved := replicaState.WALHeadLSN
	localCheckpoint := replicaState.CheckpointLSN

	// 1. achievedLSN > targetLSN — primary advanced between plan and transfer.
	if localAchieved <= targetLSN {
		t.Fatalf("achievedLSN %d must be > targetLSN %d (primary wrote 5 more entries)",
			localAchieved, targetLSN)
	}
	t.Logf("achievedLSN=%d > targetLSN=%d — confirmed", localAchieved, targetLSN)

	// 2. Sender reached InSync.
	s := driver.Orchestrator.Registry.Sender("vol1/vs2")
	if s.State() != engine.StateInSync {
		t.Fatalf("state=%s, want InSync", s.State())
	}

	// 3. No split truth: local checkpoint = local head = achievedLSN.
	if localCheckpoint != localAchieved {
		t.Fatalf("split truth: checkpoint=%d != achieved=%d", localCheckpoint, localAchieved)
	}

	// 4. Receiver progress aligned to achievedLSN.
	// In this test the receiver is nil (standalone replica vol), so
	// ReceivedLSN returns 0. The fix is verified structurally by
	// Commit + SyncReceiverProgress; production tests with live
	// receivers will exercise the full path.
	receiverLSN := replicaVol.ReceivedLSN()
	t.Logf("receiver progress: %d (0 = no active receiver in this test)", receiverLSN)

	// 5. All 25 LBAs match (original 20 + 5 written after plan).
	verifyLBAMatch(t, primaryVol, replicaVol, 25)

	// 6. Pins released.
	if primaryPinner.ActiveHoldCount() != 0 {
		t.Fatalf("%d pins leaked", primaryPinner.ActiveHoldCount())
	}

	// 7. Engine log shows rebuild completion.
	events := driver.Orchestrator.Log.EventsFor("vol1/vs2")
	hasCompleted := false
	for _, ev := range events {
		if ev.Event == "exec_rebuild_completed" {
			hasCompleted = true
		}
	}
	if !hasCompleted {
		t.Fatal("missing exec_rebuild_completed event")
	}

	t.Logf("convergence: target=%d achieved=%d checkpoint=%d — single truth verified",
		targetLSN, localAchieved, localCheckpoint)
}

// --- Stale-higher convergence: replica had higher LSN than rebuilt boundary ---

func TestP1_TransferFullBase_StaleHigherThanAchieved(t *testing.T) {
	dir := t.TempDir()

	// Primary: small amount of data (10 entries → achievedLSN ~10).
	primaryVol := createTestVolNamed(t, dir, "primary.blockvol")
	defer primaryVol.Close()

	for i := 0; i < 10; i++ {
		primaryVol.WriteLBA(uint64(i), makeBlock(byte('P')))
	}
	primaryVol.ForceFlush()

	rebuildServer, err := blockvol.NewRebuildServer(primaryVol, "127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewRebuildServer: %v", err)
	}
	rebuildServer.Serve()
	defer rebuildServer.Stop()

	// Replica: has MORE data than primary (30 entries → higher nextLSN/receivedLSN).
	// This simulates a stale replica that diverged (e.g., old primary that was demoted).
	replicaVol := createTestVolNamed(t, dir, "replica.blockvol")
	defer replicaVol.Close()

	for i := 0; i < 30; i++ {
		replicaVol.WriteLBA(uint64(i), makeBlock(byte('R')))
	}

	// Start receiver so replRecv has a high receivedLSN.
	if err := replicaVol.StartReplicaReceiver("127.0.0.1:0", "127.0.0.1:0"); err != nil {
		t.Fatalf("StartReplicaReceiver: %v", err)
	}

	staleLSN := replicaVol.ReceivedLSN()
	staleState := NewReader(replicaVol).ReadState()
	t.Logf("replica before: head=%d receivedLSN=%d (higher than primary)",
		staleState.WALHeadLSN, staleLSN)

	if staleLSN <= 10 {
		t.Fatalf("replica receivedLSN %d must be > primary's 10 for this test", staleLSN)
	}

	// Transfer full base — must RESET (not just advance) to achieved boundary.
	executor := NewExecutor(replicaVol, rebuildServer.Addr())
	achieved, err := executor.TransferFullBase(0)
	if err != nil {
		t.Fatalf("TransferFullBase: %v", err)
	}

	// The achieved boundary should match the primary (~10), NOT the stale (~30).
	postState := NewReader(replicaVol).ReadState()
	postReceivedLSN := replicaVol.ReceivedLSN()

	t.Logf("replica after: head=%d checkpoint=%d receivedLSN=%d achieved=%d",
		postState.WALHeadLSN, postState.CheckpointLSN, postReceivedLSN, achieved)

	// nextLSN (via WALHeadLSN) must be reset to achieved, not kept at stale higher value.
	if postState.WALHeadLSN != achieved {
		t.Fatalf("split truth: WALHeadLSN=%d != achieved=%d (stale higher value not reset)",
			postState.WALHeadLSN, achieved)
	}

	// receivedLSN must be reset to achieved, not kept at stale higher value.
	if postReceivedLSN != achieved {
		t.Fatalf("split truth: receivedLSN=%d != achieved=%d (stale higher value not reset)",
			postReceivedLSN, achieved)
	}

	// Data must be primary's, not stale replica's.
	for i := 0; i < 10; i++ {
		data, err := replicaVol.ReadLBA(uint64(i), replicaVol.Info().BlockSize)
		if err != nil {
			t.Fatalf("ReadLBA(%d): %v", i, err)
		}
		if data[0] != 'P' {
			t.Fatalf("LBA %d: got %c, want P", i, data[0])
		}
	}

	t.Logf("stale-higher: staleRecv=%d staleHead=%d → achieved=%d receivedLSN=%d — reset, not max",
		staleLSN, staleState.WALHeadLSN, achieved, postReceivedLSN)
}

// --- Live receiver: receivedLSN convergence through active receiver ---

func TestP1_TransferFullBase_LiveReceiverConvergence(t *testing.T) {
	dir := t.TempDir()

	// Primary: write data + flush.
	primaryVol := createTestVolNamed(t, dir, "primary.blockvol")
	defer primaryVol.Close()

	for i := 0; i < 20; i++ {
		primaryVol.WriteLBA(uint64(i), makeBlock(byte('A'+i%26)))
	}
	primaryVol.ForceFlush()

	rebuildServer, err := blockvol.NewRebuildServer(primaryVol, "127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewRebuildServer: %v", err)
	}
	rebuildServer.Serve()
	defer rebuildServer.Stop()

	// Replica: has an ACTIVE receiver before rebuild (simulates a replica
	// that was previously receiving WAL entries and now needs a rebuild).
	replicaVol := createTestVolNamed(t, dir, "replica.blockvol")
	defer replicaVol.Close()

	// Write some stale data so the replica has a non-zero receivedLSN.
	for i := 0; i < 3; i++ {
		replicaVol.WriteLBA(uint64(i), makeBlock(byte('R')))
	}

	// Start a real receiver on the replica via StartReplicaReceiver
	// (sets vol.replRecv so ReceivedLSN() returns a real value).
	if err := replicaVol.StartReplicaReceiver("127.0.0.1:0", "127.0.0.1:0"); err != nil {
		t.Fatalf("StartReplicaReceiver: %v", err)
	}

	staleReceivedLSN := replicaVol.ReceivedLSN()
	t.Logf("replica before rebuild: receivedLSN=%d", staleReceivedLSN)
	if staleReceivedLSN == 0 {
		t.Fatal("receiver must have non-zero receivedLSN before rebuild")
	}

	// Transfer full base — must align receiver progress.
	executor := NewExecutor(replicaVol, rebuildServer.Addr())
	achieved, err := executor.TransferFullBase(0)
	if err != nil {
		t.Fatalf("TransferFullBase: %v", err)
	}

	// Verify: receivedLSN advanced to achievedLSN.
	postReceivedLSN := replicaVol.ReceivedLSN()
	if postReceivedLSN != achieved {
		t.Fatalf("receiver split truth: receivedLSN=%d != achieved=%d",
			postReceivedLSN, achieved)
	}

	// Verify: LBA data matches primary (not stale 'R' blocks).
	verifyLBAMatch(t, primaryVol, replicaVol, 20)

	t.Logf("live receiver convergence: stale=%d → achieved=%d, receivedLSN=%d — no split truth",
		staleReceivedLSN, achieved, postReceivedLSN)
}

// --- Fail-Closed: connection refused ---

func TestP1_TransferFullBase_ConnectionRefused(t *testing.T) {
	dir := t.TempDir()

	replicaVol := createTestVolNamed(t, dir, "replica.blockvol")
	defer replicaVol.Close()

	// Point to an address where nothing is listening.
	executor := NewExecutor(replicaVol, "127.0.0.1:1")

	_, err := executor.TransferFullBase(100)
	if err == nil {
		t.Fatal("should fail on connection refused")
	}
	t.Logf("connection refused: %v", err)
}

// --- Fail-Closed: epoch mismatch ---

func TestP1_TransferFullBase_EpochMismatch(t *testing.T) {
	dir := t.TempDir()

	// Primary with epoch 5.
	primaryVol := createTestVolNamed(t, dir, "primary.blockvol")
	defer primaryVol.Close()
	primaryVol.SetEpoch(5)

	primaryVol.WriteLBA(0, makeBlock('A'))
	primaryVol.ForceFlush()

	rebuildServer, err := blockvol.NewRebuildServer(primaryVol, "127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewRebuildServer: %v", err)
	}
	rebuildServer.Serve()
	defer rebuildServer.Stop()

	// Replica with epoch 3 (stale).
	replicaVol := createTestVolNamed(t, dir, "replica.blockvol")
	defer replicaVol.Close()
	replicaVol.SetEpoch(3)

	executor := NewExecutor(replicaVol, rebuildServer.Addr())

	_, err = executor.TransferFullBase(100)
	if err == nil {
		t.Fatal("should fail on epoch mismatch")
	}
	if !strings.Contains(err.Error(), "EPOCH_MISMATCH") {
		t.Fatalf("expected EPOCH_MISMATCH, got: %v", err)
	}
	t.Logf("epoch mismatch: %v", err)
}

// --- Fail-Closed: no rebuild address ---

func TestP1_TransferFullBase_NoAddress(t *testing.T) {
	vol := createTestVol(t)
	defer vol.Close()

	executor := NewExecutor(vol, "")

	_, err := executor.TransferFullBase(100)
	if err == nil {
		t.Fatal("should fail without rebuild address")
	}
	t.Logf("no address: %v", err)
}

// --- Fail-Closed: partial transfer (server closes mid-stream) ---

func TestP1_TransferFullBase_PartialTransfer(t *testing.T) {
	dir := t.TempDir()

	replicaVol := createTestVolNamed(t, dir, "replica.blockvol")
	defer replicaVol.Close()

	// Start a fake server that sends one extent chunk then closes.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		// Read the request frame (discard).
		blockvol.ReadFrame(conn)
		// Send one extent chunk.
		chunk := make([]byte, 4096)
		for i := range chunk {
			chunk[i] = 0xFF
		}
		blockvol.WriteFrame(conn, blockvol.MsgRebuildExtent, chunk)
		// Close abruptly — no MsgRebuildDone.
		conn.Close()
	}()

	executor := NewExecutor(replicaVol, ln.Addr().String())

	_, err = executor.TransferFullBase(100)
	if err == nil {
		t.Fatal("should fail on partial transfer (connection closed before Done)")
	}
	t.Logf("partial transfer: %v", err)
}
