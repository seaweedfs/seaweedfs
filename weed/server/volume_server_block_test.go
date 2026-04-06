package weed_server

import (
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	engine "github.com/seaweedfs/seaweedfs/sw-block/engine/replication"
	rt "github.com/seaweedfs/seaweedfs/sw-block/engine/replication/runtime"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/v2bridge"
)

func createTestBlockVolFile(t *testing.T, dir, name string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	vol, err := blockvol.CreateBlockVol(path, blockvol.CreateOptions{
		VolumeSize: 1024 * 4096,
		BlockSize:  4096,
		ExtentSize: 65536,
		WALSize:    1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	vol.Close()
	return path
}

func TestBlockServiceDisabledByDefault(t *testing.T) {
	// Empty blockDir means feature is disabled.
	bs := StartBlockService("0.0.0.0:3260", "", "", "", NVMeConfig{})
	if bs != nil {
		bs.Shutdown()
		t.Fatal("expected nil BlockService when blockDir is empty")
	}

	// Shutdown on nil should be safe (no panic).
	var nilBS *BlockService
	nilBS.Shutdown()
}

func TestBlockServiceStartAndShutdown(t *testing.T) {
	dir := t.TempDir()
	createTestBlockVolFile(t, dir, "testvol.blk")

	bs := StartBlockService("127.0.0.1:0", dir, "iqn.2024-01.com.test:vol.", "127.0.0.1:3260,1", NVMeConfig{})
	if bs == nil {
		t.Fatal("expected non-nil BlockService")
	}
	defer bs.Shutdown()

	// Verify the volume was registered.
	paths := bs.blockStore.ListBlockVolumes()
	if len(paths) != 1 {
		t.Fatalf("expected 1 volume, got %d", len(paths))
	}

	expected := filepath.Join(dir, "testvol.blk")
	if paths[0] != expected {
		t.Fatalf("expected path %s, got %s", expected, paths[0])
	}
}

// newTestBlockServiceDirect creates a BlockService without iSCSI target for unit testing.
func newTestBlockServiceDirect(t *testing.T) *BlockService {
	t.Helper()
	dir := t.TempDir()
	store := storage.NewBlockVolumeStore()
	t.Cleanup(func() { store.Close() })
	bs := &BlockService{
		blockStore:      store,
		blockDir:        dir,
		listenAddr:      "0.0.0.0:3260",
		iqnPrefix:       "iqn.2024-01.com.seaweedfs:vol.",
		replStates:      make(map[string]*volReplState),
		v2Bridge:        v2bridge.NewControlBridge(),
		v2Orchestrator:  engine.NewRecoveryOrchestrator(),
		v2Core:          engine.NewCoreEngine(),
		coreProj:        make(map[string]engine.PublicationProjection),
		protocolExec:    make(map[string]volumeProtocolExecutionState),
		activationGated: make(map[string]string),
		localServerID:   "vs-test",
	}
	bs.v2Recovery = NewRecoveryManager(bs)
	return bs
}

func createTestVolDirect(t *testing.T, bs *BlockService, name string) string {
	t.Helper()
	path := filepath.Join(bs.blockDir, name+".blk")
	vol, err := blockvol.CreateBlockVol(path, blockvol.CreateOptions{VolumeSize: 4 * 1024 * 1024})
	if err != nil {
		t.Fatalf("create %s: %v", name, err)
	}
	vol.Close()
	if _, err := bs.blockStore.AddBlockVolume(path, "ssd"); err != nil {
		t.Fatalf("register %s: %v", name, err)
	}
	return path
}

type fakeCatchUpIO struct {
	transferredTo uint64
}

func (f fakeCatchUpIO) StreamWALEntries(startExclusive, endInclusive uint64) (uint64, error) {
	if f.transferredTo > 0 {
		return f.transferredTo, nil
	}
	return endInclusive, nil
}

func (f fakeCatchUpIO) TruncateWAL(truncateLSN uint64) error {
	return nil
}

func TestBlockService_ProcessAssignment_Primary(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "vol1")

	bs.ProcessAssignments([]blockvol.BlockVolumeAssignment{
		{Path: path, Epoch: 1, Role: blockvol.RoleToWire(blockvol.RolePrimary), LeaseTtlMs: 30000},
	})

	vol, ok := bs.blockStore.GetBlockVolume(path)
	if !ok {
		t.Fatal("volume not found")
	}
	s := vol.Status()
	if s.Role != blockvol.RolePrimary {
		t.Fatalf("expected Primary, got %v", s.Role)
	}
	if s.Epoch != 1 {
		t.Fatalf("expected epoch 1, got %d", s.Epoch)
	}
}

func TestBlockService_ProcessAssignment_Replica(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "vol1")

	bs.ProcessAssignments([]blockvol.BlockVolumeAssignment{
		{Path: path, Epoch: 1, Role: blockvol.RoleToWire(blockvol.RoleReplica), LeaseTtlMs: 30000},
	})

	vol, ok := bs.blockStore.GetBlockVolume(path)
	if !ok {
		t.Fatal("volume not found")
	}
	s := vol.Status()
	if s.Role != blockvol.RoleReplica {
		t.Fatalf("expected Replica, got %v", s.Role)
	}
}

func TestBlockService_ProcessAssignment_UnknownVolume(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	// Should log warning but not panic.
	bs.ProcessAssignments([]blockvol.BlockVolumeAssignment{
		{Path: "/nonexistent.blk", Epoch: 1, Role: blockvol.RoleToWire(blockvol.RolePrimary)},
	})
}

func TestBlockService_ProcessAssignment_LeaseRefresh(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "vol1")

	bs.ProcessAssignments([]blockvol.BlockVolumeAssignment{
		{Path: path, Epoch: 1, Role: blockvol.RoleToWire(blockvol.RolePrimary), LeaseTtlMs: 30000},
	})
	bs.ProcessAssignments([]blockvol.BlockVolumeAssignment{
		{Path: path, Epoch: 1, Role: blockvol.RoleToWire(blockvol.RolePrimary), LeaseTtlMs: 60000},
	})

	vol, _ := bs.blockStore.GetBlockVolume(path)
	s := vol.Status()
	if s.Role != blockvol.RolePrimary || s.Epoch != 1 {
		t.Fatalf("unexpected: role=%v epoch=%d", s.Role, s.Epoch)
	}
}

func TestBlockService_ProcessAssignment_WithReplicaAddrs(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "vol1")

	bs.ProcessAssignments([]blockvol.BlockVolumeAssignment{
		{
			Path: path, Epoch: 1, Role: blockvol.RoleToWire(blockvol.RolePrimary),
			LeaseTtlMs: 30000, ReplicaDataAddr: "10.0.0.2:4260", ReplicaCtrlAddr: "10.0.0.2:4261",
		},
	})

	vol, _ := bs.blockStore.GetBlockVolume(path)
	if vol.Status().Role != blockvol.RolePrimary {
		t.Fatalf("expected Primary")
	}
}

func TestBlockService_ApplyAssignments_UpdatesCoreProjectionReplicaPath(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "vol-core-replica")

	errs := bs.ApplyAssignments([]blockvol.BlockVolumeAssignment{
		{
			Path:            path,
			Epoch:           1,
			Role:            blockvol.RoleToWire(blockvol.RoleReplica),
			LeaseTtlMs:      30000,
			ReplicaDataAddr: "127.0.0.1:0",
			ReplicaCtrlAddr: "127.0.0.1:0",
		},
	})
	if len(errs) != 1 {
		t.Fatalf("errs len=%d", len(errs))
	}
	if errs[0] != nil {
		t.Fatalf("apply assignment: %v", errs[0])
	}

	proj, ok := bs.CoreProjection(path)
	if !ok {
		t.Fatal("expected core projection to be cached on narrow live path")
	}
	if proj.Role != engine.RoleReplica {
		t.Fatalf("role=%s", proj.Role)
	}
	if proj.Mode.Name != engine.ModeReplicaReady {
		t.Fatalf("mode=%s", proj.Mode.Name)
	}
	if !proj.Readiness.RoleApplied || !proj.Readiness.ReceiverReady {
		t.Fatalf("readiness=%+v", proj.Readiness)
	}
	if proj.Publication.Healthy {
		t.Fatal("replica-ready narrow path must not overclaim publish healthy")
	}
	if proj.Publication.Reason != "replica_not_primary" {
		t.Fatalf("publication_reason=%q", proj.Publication.Reason)
	}

	coreProj, ok := bs.V2Core().Projection(path)
	if !ok {
		t.Fatal("expected core projection from explicit V2 core")
	}
	if !reflect.DeepEqual(proj, coreProj) {
		t.Fatalf("adapter cache diverged from core projection:\ncache=%+v\ncore=%+v", proj, coreProj)
	}
	if mismatches := bs.CoreProjectionMismatches(path); len(mismatches) != 0 {
		t.Fatalf("readiness/core mismatches=%v", mismatches)
	}
}

func TestBlockService_ApplyAssignments_UpdatesCoreProjectionPrimaryPath(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "vol-core-primary")

	errs := bs.ApplyAssignments([]blockvol.BlockVolumeAssignment{
		{
			Path:            path,
			Epoch:           1,
			Role:            blockvol.RoleToWire(blockvol.RolePrimary),
			LeaseTtlMs:      30000,
			ReplicaServerID: "vs-2",
			ReplicaDataAddr: "10.0.0.2:4260",
			ReplicaCtrlAddr: "10.0.0.2:4261",
		},
	})
	if len(errs) != 1 {
		t.Fatalf("errs len=%d", len(errs))
	}
	if errs[0] != nil {
		t.Fatalf("apply assignment: %v", errs[0])
	}

	proj, ok := bs.CoreProjection(path)
	if !ok {
		t.Fatal("expected core projection to be cached on narrow live path")
	}
	if proj.Role != engine.RolePrimary {
		t.Fatalf("role=%s", proj.Role)
	}
	if proj.Readiness.RoleApplied != true {
		t.Fatalf("readiness=%+v", proj.Readiness)
	}
	if !proj.Readiness.ShipperConfigured {
		t.Fatalf("readiness=%+v", proj.Readiness)
	}
	if proj.Publication.Healthy {
		t.Fatal("primary assignment without durable boundary must not overclaim publish healthy")
	}

	coreProj, ok := bs.V2Core().Projection(path)
	if !ok {
		t.Fatal("expected core projection from explicit V2 core")
	}
	if !reflect.DeepEqual(proj, coreProj) {
		t.Fatalf("adapter cache diverged from core projection:\ncache=%+v\ncore=%+v", proj, coreProj)
	}
	if mismatches := bs.CoreProjectionMismatches(path); len(mismatches) != 0 {
		t.Fatalf("readiness/core mismatches=%v", mismatches)
	}
}

func TestBlockService_ApplyAssignments_PrimaryScalarReplicaAddrWithoutServerID(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "vol-core-primary-scalar")

	errs := bs.ApplyAssignments([]blockvol.BlockVolumeAssignment{
		{
			Path:            path,
			Epoch:           1,
			Role:            blockvol.RoleToWire(blockvol.RolePrimary),
			LeaseTtlMs:      30000,
			ReplicaDataAddr: "10.0.0.2:4260",
			ReplicaCtrlAddr: "10.0.0.2:4261",
		},
	})
	if len(errs) != 1 {
		t.Fatalf("errs len=%d", len(errs))
	}
	if errs[0] != nil {
		t.Fatalf("apply assignment: %v", errs[0])
	}

	proj, ok := bs.CoreProjection(path)
	if !ok {
		t.Fatal("expected core projection to be cached on narrow live path")
	}
	if !proj.Readiness.RoleApplied {
		t.Fatalf("role_applied should still be observed on fail-closed scalar path, projection=%+v", proj)
	}
	if proj.Readiness.ShipperConfigured {
		t.Fatalf("shipper_configured should stay false without explicit ReplicaServerID, projection=%+v", proj)
	}
	if len(proj.ReplicaIDs) != 0 {
		t.Fatalf("replica_ids=%v, want empty fail-closed set", proj.ReplicaIDs)
	}
	if proj.Mode.Name != engine.ModeAllocatedOnly {
		t.Fatalf("mode=%s", proj.Mode.Name)
	}
}

func TestBlockService_ApplyAssignments_PrimaryMultiReplicaMissingServerID_SkipsOnlyInvalidReplica(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "vol-core-primary-multi-missing-id")

	errs := bs.ApplyAssignments([]blockvol.BlockVolumeAssignment{
		{
			Path:       path,
			Epoch:      1,
			Role:       blockvol.RoleToWire(blockvol.RolePrimary),
			LeaseTtlMs: 30000,
			ReplicaAddrs: []blockvol.ReplicaAddr{
				{ServerID: "vs-2", DataAddr: "10.0.0.2:4260", CtrlAddr: "10.0.0.2:4261"},
				{ServerID: "", DataAddr: "10.0.0.3:4260", CtrlAddr: "10.0.0.3:4261"},
			},
		},
	})
	if len(errs) != 1 {
		t.Fatalf("errs len=%d", len(errs))
	}
	if errs[0] != nil {
		t.Fatalf("apply assignment: %v", errs[0])
	}

	proj, ok := bs.CoreProjection(path)
	if !ok {
		t.Fatal("expected core projection to be cached on narrow live path")
	}
	if !proj.Readiness.RoleApplied {
		t.Fatalf("role_applied should remain true, projection=%+v", proj)
	}
	if !proj.Readiness.ShipperConfigured {
		t.Fatalf("shipper_configured should stay true when at least one replica has valid identity, projection=%+v", proj)
	}
	if len(proj.ReplicaIDs) != 1 {
		t.Fatalf("replica_ids=%v, want exactly one valid replica", proj.ReplicaIDs)
	}
	if !strings.HasSuffix(proj.ReplicaIDs[0], "/vs-2") {
		t.Fatalf("replica_id=%q, want suffix %q", proj.ReplicaIDs[0], "/vs-2")
	}
	if proj.Mode.Name != engine.ModeBootstrapPending {
		t.Fatalf("mode=%s", proj.Mode.Name)
	}
}

func TestBlockService_ApplyAssignments_PrimaryRefreshSameEpochPreservesRoleApplied(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "vol-core-primary-refresh")

	initial := blockvol.BlockVolumeAssignment{
		Path:       path,
		Epoch:      1,
		Role:       blockvol.RoleToWire(blockvol.RolePrimary),
		LeaseTtlMs: 30000,
	}
	errs := bs.ApplyAssignments([]blockvol.BlockVolumeAssignment{initial})
	if len(errs) != 1 || errs[0] != nil {
		t.Fatalf("initial apply errs=%v", errs)
	}
	first, ok := bs.CoreProjection(path)
	if !ok {
		t.Fatal("expected core projection after initial apply")
	}
	if !first.Readiness.RoleApplied {
		t.Fatalf("initial projection should report role applied, got %+v", first.Readiness)
	}
	if len(first.ReplicaIDs) != 0 {
		t.Fatalf("initial replica_ids=%v, want empty", first.ReplicaIDs)
	}

	refresh := blockvol.BlockVolumeAssignment{
		Path:            path,
		Epoch:           1,
		Role:            blockvol.RoleToWire(blockvol.RolePrimary),
		LeaseTtlMs:      30000,
		ReplicaServerID: "vs-2",
		ReplicaDataAddr: "10.0.0.2:4260",
		ReplicaCtrlAddr: "10.0.0.2:4261",
	}
	errs = bs.ApplyAssignments([]blockvol.BlockVolumeAssignment{refresh})
	if len(errs) != 1 || errs[0] != nil {
		t.Fatalf("refresh apply errs=%v", errs)
	}

	proj, ok := bs.CoreProjection(path)
	if !ok {
		t.Fatal("expected core projection after refresh apply")
	}
	if !proj.Readiness.RoleApplied {
		t.Fatalf("same-epoch refresh should preserve role_applied, got %+v", proj.Readiness)
	}
	if !proj.Readiness.ShipperConfigured {
		t.Fatalf("same-epoch refresh should configure shipper, got %+v", proj.Readiness)
	}
	if len(proj.ReplicaIDs) != 1 {
		t.Fatalf("replica_ids=%v", proj.ReplicaIDs)
	}
	if !strings.HasSuffix(proj.ReplicaIDs[0], "/vs-2") {
		t.Fatalf("replica_id=%q, want suffix %q", proj.ReplicaIDs[0], "/vs-2")
	}
	if proj.Mode.Name != engine.ModeBootstrapPending {
		t.Fatalf("mode=%s", proj.Mode.Name)
	}
}

func TestBlockService_ApplyAssignments_PrimaryLeaseRefreshDoesNotWipeReplicaMembership(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "vol-core-primary-lease-refresh")

	withReplica := blockvol.BlockVolumeAssignment{
		Path:            path,
		Epoch:           1,
		Role:            blockvol.RoleToWire(blockvol.RolePrimary),
		LeaseTtlMs:      30000,
		ReplicaServerID: "vs-2",
		ReplicaDataAddr: "10.0.0.2:4260",
		ReplicaCtrlAddr: "10.0.0.2:4261",
	}
	errs := bs.ApplyAssignments([]blockvol.BlockVolumeAssignment{withReplica})
	if len(errs) != 1 || errs[0] != nil {
		t.Fatalf("initial apply errs=%v", errs)
	}
	before, ok := bs.CoreProjection(path)
	if !ok {
		t.Fatal("expected core projection after initial assignment")
	}
	if !before.Readiness.RoleApplied || !before.Readiness.ShipperConfigured {
		t.Fatalf("expected role_applied + shipper_configured before lease refresh, got %+v", before.Readiness)
	}
	if len(before.ReplicaIDs) != 1 {
		t.Fatalf("before replica_ids=%v", before.ReplicaIDs)
	}

	leaseRefresh := blockvol.BlockVolumeAssignment{
		Path:       path,
		Epoch:      1,
		Role:       blockvol.RoleToWire(blockvol.RolePrimary),
		LeaseTtlMs: 60000,
	}
	errs = bs.ApplyAssignments([]blockvol.BlockVolumeAssignment{leaseRefresh})
	if len(errs) != 1 || errs[0] != nil {
		t.Fatalf("lease refresh errs=%v", errs)
	}

	after, ok := bs.CoreProjection(path)
	if !ok {
		t.Fatal("expected core projection after lease refresh")
	}
	if !after.Readiness.RoleApplied || !after.Readiness.ShipperConfigured {
		t.Fatalf("lease refresh should preserve role_applied + shipper_configured, got %+v", after.Readiness)
	}
	if len(after.ReplicaIDs) != 1 {
		t.Fatalf("after replica_ids=%v", after.ReplicaIDs)
	}
	if !reflect.DeepEqual(before.ReplicaIDs, after.ReplicaIDs) {
		t.Fatalf("lease refresh wiped replica membership: before=%v after=%v", before.ReplicaIDs, after.ReplicaIDs)
	}
	if after.Mode.Name != engine.ModeBootstrapPending {
		t.Fatalf("lease refresh should keep bootstrap_pending while waiting for shipper connection, got %s", after.Mode.Name)
	}
}

func TestBlockService_ApplyAssignments_RepeatedUnchangedStaysInSyncWithCore(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "vol-core-repeat")

	assign := blockvol.BlockVolumeAssignment{
		Path:            path,
		Epoch:           1,
		Role:            blockvol.RoleToWire(blockvol.RolePrimary),
		LeaseTtlMs:      30000,
		ReplicaServerID: "vs-2",
		ReplicaDataAddr: "10.0.0.2:4260",
		ReplicaCtrlAddr: "10.0.0.2:4261",
	}

	errs := bs.ApplyAssignments([]blockvol.BlockVolumeAssignment{assign})
	if len(errs) != 1 || errs[0] != nil {
		t.Fatalf("first apply errs=%v", errs)
	}
	firstCache, ok := bs.CoreProjection(path)
	if !ok {
		t.Fatal("expected cached projection after first apply")
	}
	firstCore, ok := bs.V2Core().Projection(path)
	if !ok {
		t.Fatal("expected core projection after first apply")
	}
	if !reflect.DeepEqual(firstCache, firstCore) {
		t.Fatalf("first cache/core mismatch:\ncache=%+v\ncore=%+v", firstCache, firstCore)
	}

	errs = bs.ApplyAssignments([]blockvol.BlockVolumeAssignment{assign})
	if len(errs) != 1 || errs[0] != nil {
		t.Fatalf("second apply errs=%v", errs)
	}
	secondCache, ok := bs.CoreProjection(path)
	if !ok {
		t.Fatal("expected cached projection after second apply")
	}
	secondCore, ok := bs.V2Core().Projection(path)
	if !ok {
		t.Fatal("expected core projection after second apply")
	}
	if !reflect.DeepEqual(secondCache, secondCore) {
		t.Fatalf("second cache/core mismatch:\ncache=%+v\ncore=%+v", secondCache, secondCore)
	}
	if !reflect.DeepEqual(firstCache, secondCache) {
		t.Fatalf("unchanged assignment should not split cached projection:\nfirst=%+v\nsecond=%+v", firstCache, secondCache)
	}
	if mismatches := bs.CoreProjectionMismatches(path); len(mismatches) != 0 {
		t.Fatalf("readiness/core mismatches=%v", mismatches)
	}
}

func TestBlockService_ApplyAssignments_ExecutesCoreCommands_PrimaryRoleApplyAndConfigureShipper(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "vol-core-cmd-primary")

	assign := blockvol.BlockVolumeAssignment{
		Path:            path,
		Epoch:           1,
		Role:            blockvol.RoleToWire(blockvol.RolePrimary),
		LeaseTtlMs:      30000,
		ReplicaServerID: "vs-2",
		ReplicaDataAddr: "10.0.0.2:4260",
		ReplicaCtrlAddr: "10.0.0.2:4261",
	}

	errs := bs.ApplyAssignments([]blockvol.BlockVolumeAssignment{assign})
	if len(errs) != 1 || errs[0] != nil {
		t.Fatalf("apply errs=%v", errs)
	}

	vol, ok := bs.blockStore.GetBlockVolume(path)
	if !ok {
		t.Fatal("volume not found")
	}
	status := vol.Status()
	if status.Role != blockvol.RolePrimary || status.Epoch != 1 {
		t.Fatalf("status=%+v", status)
	}
	if got := bs.ExecutedCoreCommands(path); !reflect.DeepEqual(got, []string{"apply_role", "configure_shipper"}) {
		t.Fatalf("executed commands=%v", got)
	}

	errs = bs.ApplyAssignments([]blockvol.BlockVolumeAssignment{assign})
	if len(errs) != 1 || errs[0] != nil {
		t.Fatalf("second apply errs=%v", errs)
	}
	if got := bs.ExecutedCoreCommands(path); !reflect.DeepEqual(got, []string{"apply_role", "configure_shipper"}) {
		t.Fatalf("unchanged assignment should not re-execute command chain, got %v", got)
	}
}

func TestBlockService_ApplyAssignments_ExecutesCoreCommands_ReplicaRoleAndReceiver(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "vol-core-cmd-replica")

	errs := bs.ApplyAssignments([]blockvol.BlockVolumeAssignment{{
		Path:            path,
		Epoch:           1,
		Role:            blockvol.RoleToWire(blockvol.RoleReplica),
		LeaseTtlMs:      30000,
		ReplicaDataAddr: "127.0.0.1:0",
		ReplicaCtrlAddr: "127.0.0.1:0",
	}})
	if len(errs) != 1 || errs[0] != nil {
		t.Fatalf("apply errs=%v", errs)
	}

	vol, ok := bs.blockStore.GetBlockVolume(path)
	if !ok {
		t.Fatal("volume not found")
	}
	status := vol.Status()
	if status.Role != blockvol.RoleReplica || status.Epoch != 1 {
		t.Fatalf("status=%+v", status)
	}
	if got := bs.ExecutedCoreCommands(path); !reflect.DeepEqual(got, []string{"apply_role", "start_receiver"}) {
		t.Fatalf("executed commands=%v", got)
	}
	readiness := bs.ReadinessSnapshot(path)
	if !readiness.RoleApplied || !readiness.ReceiverReady {
		t.Fatalf("readiness=%+v", readiness)
	}
}

func TestBlockService_ApplyAssignments_PrimaryRole_UsesCoreStartRecoveryTaskForCatchUp(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	bs.v2Bridge = newTestControlBridge()
	bs.v2Orchestrator = newTestOrchestrator()
	bs.v2Recovery = NewRecoveryManager(bs)
	defer bs.v2Recovery.Shutdown()

	path := createTestVolDirect(t, bs, "vol-core-cmd-catchup-start")
	if err := bs.blockStore.WithVolume(path, func(vol *blockvol.BlockVol) error {
		for i := 0; i < 5; i++ {
			if err := vol.WriteLBA(uint64(i), make([]byte, 4096)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("write: %v", err)
	}

	bs.v2Recovery.OnPendingExecution = func(volumeID string, pending *rt.PendingExecution) {
		if volumeID == path && pending != nil && pending.Plan != nil {
			pending.CatchUpIO = fakeCatchUpIO{transferredTo: pending.Plan.CatchUpTarget}
		}
	}

	errs := bs.ApplyAssignments([]blockvol.BlockVolumeAssignment{{
		Path:            path,
		Epoch:           1,
		Role:            blockvol.RoleToWire(blockvol.RolePrimary),
		LeaseTtlMs:      30000,
		ReplicaServerID: "vs-2",
		ReplicaDataAddr: "10.0.0.2:4260",
		ReplicaCtrlAddr: "10.0.0.2:4261",
	}})
	if len(errs) != 1 || errs[0] != nil {
		t.Fatalf("apply errs=%v", errs)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		proj, ok := bs.CoreProjection(path)
		sender := bs.v2Orchestrator.Registry.Sender(path + "/vs-2")
		if ok && proj.Recovery.Phase == engine.RecoveryIdle && sender != nil && sender.State() == engine.StateInSync {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	cmds := bs.ExecutedCoreCommands(path)
	if !reflect.DeepEqual(cmds, []string{"apply_role", "configure_shipper", "start_recovery_task", "start_catchup"}) {
		t.Fatalf("expected primary assignment to execute apply_role + configure_shipper + start_recovery_task + start_catchup, got %v", cmds)
	}
	proj, ok := bs.CoreProjection(path)
	if !ok {
		t.Fatal("expected core projection")
	}
	if proj.Recovery.Phase != engine.RecoveryIdle {
		t.Fatalf("recovery_phase=%s", proj.Recovery.Phase)
	}
	if proj.Boundary.DurableLSN == 0 {
		t.Fatalf("durable_lsn=%d", proj.Boundary.DurableLSN)
	}
	sender := bs.v2Orchestrator.Registry.Sender(path + "/vs-2")
	if sender == nil {
		t.Fatal("sender not found")
	}
	if sender.State() != engine.StateInSync {
		t.Fatalf("sender state=%s", sender.State())
	}
}

func TestBlockService_ApplyAssignments_PrimaryMultiReplica_UsesCoreStartRecoveryTaskPerReplica(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	bs.v2Bridge = newTestControlBridge()
	bs.v2Orchestrator = newTestOrchestrator()
	bs.v2Recovery = NewRecoveryManager(bs)
	defer bs.v2Recovery.Shutdown()

	path := createTestVolDirect(t, bs, "vol-core-cmd-catchup-start-multi")
	if err := bs.blockStore.WithVolume(path, func(vol *blockvol.BlockVol) error {
		for i := 0; i < 5; i++ {
			if err := vol.WriteLBA(uint64(i), make([]byte, 4096)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("write: %v", err)
	}

	bs.v2Recovery.OnPendingExecution = func(volumeID string, pending *rt.PendingExecution) {
		if volumeID == path && pending != nil && pending.Plan != nil {
			pending.CatchUpIO = fakeCatchUpIO{transferredTo: pending.Plan.CatchUpTarget}
		}
	}

	errs := bs.ApplyAssignments([]blockvol.BlockVolumeAssignment{{
		Path:       path,
		Epoch:      1,
		Role:       blockvol.RoleToWire(blockvol.RolePrimary),
		LeaseTtlMs: 30000,
		ReplicaAddrs: []blockvol.ReplicaAddr{
			{ServerID: "vs-2", DataAddr: "10.0.0.2:4260", CtrlAddr: "10.0.0.2:4261"},
			{ServerID: "vs-3", DataAddr: "10.0.0.3:4260", CtrlAddr: "10.0.0.3:4261"},
		},
	}})
	if len(errs) != 1 || errs[0] != nil {
		t.Fatalf("apply errs=%v", errs)
	}

	var (
		proj    engine.PublicationProjection
		ok      bool
		sender2 *engine.Sender
		sender3 *engine.Sender
	)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		proj, ok = bs.CoreProjection(path)
		sender2 = bs.v2Orchestrator.Registry.Sender(path + "/vs-2")
		sender3 = bs.v2Orchestrator.Registry.Sender(path + "/vs-3")
		if ok &&
			proj.Recovery.Phase == engine.RecoveryIdle &&
			sender2 != nil && sender2.State() == engine.StateInSync &&
			sender3 != nil && sender3.State() == engine.StateInSync {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if !ok {
		t.Fatal("expected core projection")
	}
	if proj.Recovery.Phase != engine.RecoveryIdle {
		t.Fatalf("recovery_phase=%s", proj.Recovery.Phase)
	}
	if proj.Boundary.DurableLSN == 0 {
		t.Fatalf("durable_lsn=%d", proj.Boundary.DurableLSN)
	}
	if sender2 == nil || sender3 == nil {
		t.Fatalf("senders not found: vs-2=%v vs-3=%v", sender2 != nil, sender3 != nil)
	}
	if sender2.State() != engine.StateInSync || sender3.State() != engine.StateInSync {
		t.Fatalf("sender states: vs-2=%s vs-3=%s", sender2.State(), sender3.State())
	}

	cmds := bs.ExecutedCoreCommands(path)
	if got := countCommandName(cmds, "apply_role"); got != 1 {
		t.Fatalf("apply_role count=%d cmds=%v", got, cmds)
	}
	if got := countCommandName(cmds, "configure_shipper"); got != 1 {
		t.Fatalf("configure_shipper count=%d cmds=%v", got, cmds)
	}
	if got := countCommandName(cmds, "start_recovery_task"); got != 2 {
		t.Fatalf("start_recovery_task count=%d cmds=%v", got, cmds)
	}
	if got := countCommandName(cmds, "start_catchup"); got != 2 {
		t.Fatalf("start_catchup count=%d cmds=%v", got, cmds)
	}
}

func TestBlockService_ApplyAssignments_RemovedReplica_UsesCoreDrainRecoveryTask(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	bs.v2Bridge = newTestControlBridge()
	bs.v2Orchestrator = newTestOrchestrator()
	bs.v2Recovery = NewRecoveryManager(bs)
	defer bs.v2Recovery.Shutdown()

	path := createTestVolDirect(t, bs, "vol-core-cmd-drain-removed")
	if err := bs.blockStore.WithVolume(path, func(vol *blockvol.BlockVol) error {
		for i := 0; i < 5; i++ {
			if err := vol.WriteLBA(uint64(i), make([]byte, 4096)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("write: %v", err)
	}

	holdTask := make(chan struct{})
	taskReached := make(chan struct{}, 1)
	bs.v2Recovery.OnBeforeExecute = func(replicaID string) {
		if replicaID == path+"/vs-2" {
			taskReached <- struct{}{}
			<-holdTask
		}
	}

	errs := bs.ApplyAssignments([]blockvol.BlockVolumeAssignment{{
		Path:            path,
		Epoch:           1,
		Role:            blockvol.RoleToWire(blockvol.RolePrimary),
		LeaseTtlMs:      30000,
		ReplicaServerID: "vs-2",
		ReplicaDataAddr: "10.0.0.2:4260",
		ReplicaCtrlAddr: "10.0.0.2:4261",
	}})
	if len(errs) != 1 || errs[0] != nil {
		t.Fatalf("apply errs=%v", errs)
	}

	select {
	case <-taskReached:
	case <-time.After(5 * time.Second):
		t.Fatal("recovery task did not reach OnBeforeExecute")
	}

	replicaID := path + "/vs-2"
	bs.v2Recovery.mu.Lock()
	oldTask := bs.v2Recovery.tasks[replicaID]
	bs.v2Recovery.mu.Unlock()
	if oldTask == nil {
		t.Fatal("expected active recovery task before removal")
	}
	oldDone := oldTask.done

	go func() {
		time.Sleep(50 * time.Millisecond)
		close(holdTask)
	}()

	errs = bs.ApplyAssignments([]blockvol.BlockVolumeAssignment{{
		Path:       path,
		Epoch:      2,
		Role:       blockvol.RoleToWire(blockvol.RolePrimary),
		LeaseTtlMs: 30000,
	}})
	if len(errs) != 1 || errs[0] != nil {
		t.Fatalf("remove apply errs=%v", errs)
	}

	select {
	case <-oldDone:
	case <-time.After(5 * time.Second):
		t.Fatal("removed recovery task did not drain")
	}

	if got := bs.v2Recovery.ActiveTaskCount(); got != 0 {
		t.Fatalf("active tasks=%d, want 0", got)
	}
	if sender := bs.v2Orchestrator.Registry.Sender(replicaID); sender != nil {
		t.Fatalf("removed sender should be gone, got %v", sender)
	}

	cmds := bs.ExecutedCoreCommands(path)
	if got := countCommandName(cmds, "start_recovery_task"); got != 1 {
		t.Fatalf("start_recovery_task count=%d cmds=%v", got, cmds)
	}
	if got := countCommandName(cmds, "drain_recovery_task"); got != 1 {
		t.Fatalf("drain_recovery_task count=%d cmds=%v", got, cmds)
	}
}

func TestBlockService_ProtocolExecutionState_ActiveCatchUpBlocksLiveShipping(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "vol-protocol-catchup")

	errs := bs.ApplyAssignments([]blockvol.BlockVolumeAssignment{{
		Path:       path,
		Epoch:      2,
		Role:       blockvol.RoleToWire(blockvol.RolePrimary),
		LeaseTtlMs: 30000,
		ReplicaAddrs: []blockvol.ReplicaAddr{{
			ServerID: "vs-2",
			DataAddr: "127.0.0.1:15060",
			CtrlAddr: "127.0.0.1:15061",
		}},
	}})
	if len(errs) != 1 || errs[0] != nil {
		t.Fatalf("apply errs=%v", errs)
	}

	state, ok := bs.ProtocolExecutionState(path)
	if !ok {
		t.Fatal("missing protocol execution state")
	}
	replicaID := path + "/vs-2"
	replica, ok := state.Replicas[replicaID]
	if !ok {
		t.Fatalf("missing replica state for %s", replicaID)
	}
	if replica.SessionKind != engine.SessionCatchUp {
		t.Fatalf("SessionKind=%s, want %s", replica.SessionKind, engine.SessionCatchUp)
	}
	if !replica.SessionActive {
		t.Fatal("SessionActive=false, want true")
	}
	if replica.LiveEligible {
		t.Fatal("LiveEligible=true, want false during active catch-up")
	}

	allow, reason := bs.protocolLiveShippingAllowed(path, replicaID, 12)
	if allow {
		t.Fatal("live shipping should be blocked while catch-up session is active")
	}
	if !strings.Contains(reason, "active_catchup_session") {
		t.Fatalf("reason=%q", reason)
	}
}

func TestBlockService_ProtocolExecutionState_InSyncSenderAllowsLiveShipping(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "vol-protocol-live")

	errs := bs.ApplyAssignments([]blockvol.BlockVolumeAssignment{{
		Path:       path,
		Epoch:      2,
		Role:       blockvol.RoleToWire(blockvol.RolePrimary),
		LeaseTtlMs: 30000,
		ReplicaAddrs: []blockvol.ReplicaAddr{{
			ServerID: "vs-2",
			DataAddr: "127.0.0.1:15070",
			CtrlAddr: "127.0.0.1:15071",
		}},
	}})
	if len(errs) != 1 || errs[0] != nil {
		t.Fatalf("apply errs=%v", errs)
	}

	replicaID := path + "/vs-2"
	sender := bs.v2Orchestrator.Registry.Sender(replicaID)
	if sender == nil {
		t.Fatalf("missing sender for %s", replicaID)
	}
	sender.InvalidateSession("test_complete", engine.StateInSync)
	bs.syncProtocolExecutionState(path)

	state, ok := bs.ProtocolExecutionState(path)
	if !ok {
		t.Fatal("missing protocol execution state")
	}
	replica, ok := state.Replicas[replicaID]
	if !ok {
		t.Fatalf("missing replica state for %s", replicaID)
	}
	if replica.SessionActive {
		t.Fatal("SessionActive=true, want false after session completion")
	}
	if !replica.LiveEligible {
		t.Fatal("LiveEligible=false, want true after session completion")
	}

	allow, reason := bs.protocolLiveShippingAllowed(path, replicaID, 12)
	if !allow {
		t.Fatalf("live shipping blocked after completion: %q", reason)
	}
}

func TestBlockService_ApplyAssignments_RebuildingRole_UsesCoreRecoveryPathWithoutLegacyDirectStart(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	bs.v2Bridge = newTestControlBridge()
	bs.v2Orchestrator = newTestOrchestrator()
	bs.v2Recovery = NewRecoveryManager(bs)
	defer bs.v2Recovery.Shutdown()

	path := createTestVolDirect(t, bs, "vol-core-cmd-rebuild-assignment")
	if err := bs.blockStore.WithVolume(path, func(vol *blockvol.BlockVol) error {
		if err := vol.WriteLBA(0, make([]byte, 4096)); err != nil {
			return err
		}
		return vol.ForceFlush()
	}); err != nil {
		t.Fatalf("write+flush: %v", err)
	}

	legacyCalls := 0
	bs.onLegacyStartRebuild = func(path, rebuildAddr string, epoch uint64) {
		legacyCalls++
	}
	bs.v2Recovery.OnPendingExecution = func(volumeID string, pending *rt.PendingExecution) {
		if volumeID == path && pending != nil && pending.Plan != nil {
			pending.RebuildIO = fakeRebuildIO{achievedLSN: pending.Plan.RebuildTargetLSN}
		}
	}

	errs := bs.ApplyAssignments([]blockvol.BlockVolumeAssignment{{
		Path:            path,
		Epoch:           2,
		Role:            blockvol.RoleToWire(blockvol.RoleRebuilding),
		LeaseTtlMs:      30000,
		ReplicaDataAddr: "127.0.0.1:0",
		ReplicaCtrlAddr: "127.0.0.1:0",
		RebuildAddr:     "127.0.0.1:15000",
	}})
	if len(errs) != 1 || errs[0] != nil {
		t.Fatalf("apply errs=%v", errs)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		proj, ok := bs.CoreProjection(path)
		sender := bs.v2Orchestrator.Registry.Sender(path + "/vs-test")
		if ok && proj.Recovery.Phase == engine.RecoveryIdle && sender != nil && sender.State() == engine.StateInSync {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if legacyCalls != 0 {
		t.Fatalf("legacy direct rebuild should not run when core is present, calls=%d", legacyCalls)
	}
	vol, ok := bs.blockStore.GetBlockVolume(path)
	if !ok {
		t.Fatal("volume not found")
	}
	status := vol.Status()
	if status.Role != blockvol.RoleRebuilding || status.Epoch != 2 {
		t.Fatalf("status=%+v", status)
	}
	cmds := bs.ExecutedCoreCommands(path)
	if !reflect.DeepEqual(cmds, []string{"apply_role", "start_recovery_task", "start_rebuild"}) {
		t.Fatalf("expected rebuilding assignment to execute apply_role + start_recovery_task + start_rebuild, got %v", cmds)
	}
	proj, ok := bs.CoreProjection(path)
	if !ok {
		t.Fatal("expected core projection")
	}
	if proj.Recovery.Phase != engine.RecoveryIdle {
		t.Fatalf("recovery_phase=%s", proj.Recovery.Phase)
	}
	if proj.Mode.Reason == "awaiting_receiver_ready" {
		t.Fatalf("rebuilding assignment should not be reported as awaiting receiver readiness: %+v", proj.Mode)
	}
	sender := bs.v2Orchestrator.Registry.Sender(path + "/vs-test")
	if sender == nil {
		t.Fatal("sender not found")
	}
	if sender.State() != engine.StateInSync {
		t.Fatalf("sender state=%s", sender.State())
	}
}

func TestBlockService_ApplyAssignments_RebuildingRole_PreservesLegacyFallbackWithoutCore(t *testing.T) {
	dir := t.TempDir()
	store := storage.NewBlockVolumeStore()
	t.Cleanup(func() { store.Close() })
	bs := &BlockService{
		blockStore:    store,
		blockDir:      dir,
		listenAddr:    "127.0.0.1:3260",
		iqnPrefix:     "iqn.2024-01.com.seaweedfs:vol.",
		replStates:    make(map[string]*volReplState),
		localServerID: "vs-test",
	}

	path := createTestVolDirect(t, bs, "vol-legacy-rebuild")
	legacyCalls := 0
	legacyCalled := make(chan struct{}, 1)
	bs.onLegacyStartRebuild = func(path, rebuildAddr string, epoch uint64) {
		legacyCalls++
		select {
		case legacyCalled <- struct{}{}:
		default:
		}
	}

	errs := bs.ApplyAssignments([]blockvol.BlockVolumeAssignment{{
		Path:        path,
		Epoch:       3,
		Role:        blockvol.RoleToWire(blockvol.RoleRebuilding),
		LeaseTtlMs:  30000,
		RebuildAddr: "127.0.0.1:15000",
	}})
	if len(errs) != 1 || errs[0] != nil {
		t.Fatalf("apply errs=%v", errs)
	}

	select {
	case <-legacyCalled:
	case <-time.After(1 * time.Second):
		t.Fatal("expected legacy direct rebuild to be used without core")
	}
	if legacyCalls != 1 {
		t.Fatalf("legacy direct rebuild calls=%d", legacyCalls)
	}
}

func TestBlockService_BarrierRejected_ExecutesCoreInvalidateSession(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	bs.v2Bridge = newTestControlBridge()
	bs.v2Orchestrator = newTestOrchestrator()
	path := createTestVolDirect(t, bs, "vol-core-cmd-invalidate")

	errs := bs.ApplyAssignments([]blockvol.BlockVolumeAssignment{{
		Path:            path,
		Epoch:           1,
		Role:            blockvol.RoleToWire(blockvol.RolePrimary),
		LeaseTtlMs:      30000,
		ReplicaServerID: "vs-2",
		ReplicaDataAddr: "10.0.0.2:4260",
		ReplicaCtrlAddr: "10.0.0.2:4261",
	}})
	if len(errs) != 1 || errs[0] != nil {
		t.Fatalf("apply errs=%v", errs)
	}

	replicaID := path + "/vs-2"
	sender := bs.v2Orchestrator.Registry.Sender(replicaID)
	if sender == nil {
		t.Fatal("sender not found")
	}
	if !sender.HasActiveSession() {
		t.Fatal("sender should start with active session")
	}

	bs.applyCoreEvent(engine.BarrierRejected{ID: path, Reason: "timeout"})

	if sender.HasActiveSession() {
		t.Fatal("sender session should be invalidated by core command")
	}
	if got := bs.ExecutedCoreCommands(path); !reflect.DeepEqual(got, []string{"apply_role", "configure_shipper", "start_recovery_task", "invalidate_session"}) {
		t.Fatalf("executed commands=%v", got)
	}
}

func TestBlockService_BarrierRejected_DoesNotReexecuteInvalidateOnSameReason(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	bs.v2Bridge = newTestControlBridge()
	bs.v2Orchestrator = newTestOrchestrator()
	path := createTestVolDirect(t, bs, "vol-core-cmd-invalidate-repeat")

	errs := bs.ApplyAssignments([]blockvol.BlockVolumeAssignment{{
		Path:            path,
		Epoch:           1,
		Role:            blockvol.RoleToWire(blockvol.RolePrimary),
		LeaseTtlMs:      30000,
		ReplicaServerID: "vs-2",
		ReplicaDataAddr: "10.0.0.2:4260",
		ReplicaCtrlAddr: "10.0.0.2:4261",
	}})
	if len(errs) != 1 || errs[0] != nil {
		t.Fatalf("apply errs=%v", errs)
	}

	bs.applyCoreEvent(engine.BarrierRejected{ID: path, Reason: "timeout"})
	first := bs.ExecutedCoreCommands(path)
	bs.applyCoreEvent(engine.BarrierRejected{ID: path, Reason: "timeout"})
	second := bs.ExecutedCoreCommands(path)

	if !reflect.DeepEqual(first, []string{"apply_role", "configure_shipper", "start_recovery_task", "invalidate_session"}) {
		t.Fatalf("first executed commands=%v", first)
	}
	if !reflect.DeepEqual(second, first) {
		t.Fatalf("repeated failure should not re-execute invalidate_session: first=%v second=%v", first, second)
	}
}

func TestBlockService_NeedsRebuildObserved_InvalidatesOnlyTargetReplica(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	bs.v2Bridge = newTestControlBridge()
	bs.v2Orchestrator = newTestOrchestrator()
	path := createTestVolDirect(t, bs, "vol-core-cmd-targeted-invalidate")

	errs := bs.ApplyAssignments([]blockvol.BlockVolumeAssignment{{
		Path:       path,
		Epoch:      1,
		Role:       blockvol.RoleToWire(blockvol.RolePrimary),
		LeaseTtlMs: 30000,
		ReplicaAddrs: []blockvol.ReplicaAddr{
			{ServerID: "vs-2", DataAddr: "10.0.0.2:4260", CtrlAddr: "10.0.0.2:4261"},
			{ServerID: "vs-3", DataAddr: "10.0.0.3:4260", CtrlAddr: "10.0.0.3:4261"},
		},
	}})
	if len(errs) != 1 || errs[0] != nil {
		t.Fatalf("apply errs=%v", errs)
	}

	replica2 := bs.v2Orchestrator.Registry.Sender(path + "/vs-2")
	replica3 := bs.v2Orchestrator.Registry.Sender(path + "/vs-3")
	if replica2 == nil || replica3 == nil {
		t.Fatalf("senders not found: vs2=%v vs3=%v", replica2 != nil, replica3 != nil)
	}
	if !replica2.HasActiveSession() || !replica3.HasActiveSession() {
		t.Fatal("both senders should start with active sessions")
	}

	bs.applyCoreEvent(engine.NeedsRebuildObserved{ID: path, ReplicaID: path + "/vs-2", Reason: "gap_too_large"})

	if replica2.HasActiveSession() {
		t.Fatal("target replica session should be invalidated")
	}
	if !replica3.HasActiveSession() {
		t.Fatal("non-target replica session should remain active")
	}
	if got := bs.ExecutedCoreCommands(path); countCommandName(got, "invalidate_session") != 1 {
		t.Fatalf("executed commands=%v", got)
	}
}

func TestBlockService_DebugInfoForVolume_UsesCoreProjectionPrimaryPath(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "vol-debug-primary")

	errs := bs.ApplyAssignments([]blockvol.BlockVolumeAssignment{
		{
			Path:            path,
			Epoch:           1,
			Role:            blockvol.RoleToWire(blockvol.RolePrimary),
			LeaseTtlMs:      30000,
			ReplicaServerID: "vs-2",
			ReplicaDataAddr: "10.0.0.2:4260",
			ReplicaCtrlAddr: "10.0.0.2:4261",
		},
	})
	if len(errs) != 1 || errs[0] != nil {
		t.Fatalf("apply assignment errs=%v", errs)
	}

	readiness := bs.ReadinessSnapshot(path)
	if readiness.PublishHealthy {
		t.Fatalf("readiness snapshot must follow core publication truth on primary path without durable boundary, got %+v", readiness)
	}

	vol, ok := bs.blockStore.GetBlockVolume(path)
	if !ok {
		t.Fatal("volume not found")
	}
	info := bs.DebugInfoForVolume(path, vol)
	proj, ok := bs.CoreProjection(path)
	if !ok {
		t.Fatal("expected core projection for debug surface")
	}
	if info.Role != string(proj.Role) {
		t.Fatalf("role=%q projection_role=%q", info.Role, proj.Role)
	}
	if info.Mode != string(proj.Mode.Name) {
		t.Fatalf("mode=%q projection_mode=%q", info.Mode, proj.Mode.Name)
	}
	if info.PublishHealthy != proj.Publication.Healthy {
		t.Fatalf("publish_healthy=%v projection=%v", info.PublishHealthy, proj.Publication.Healthy)
	}
	if info.PublicationReason != proj.Publication.Reason {
		t.Fatalf("publication_reason=%q projection_reason=%q", info.PublicationReason, proj.Publication.Reason)
	}
	if info.CoreProjection == nil {
		t.Fatal("expected embedded core projection in debug info")
	}
	if info.CoreProjection.VolumeID != proj.VolumeID || info.CoreProjection.Mode.Name != proj.Mode.Name {
		t.Fatalf("embedded core projection diverged: got=%+v want=%+v", info.CoreProjection, proj)
	}
	if !reflect.DeepEqual(info.ExecutedCoreCommands, bs.ExecutedCoreCommands(path)) {
		t.Fatalf("executed_core_commands=%v want=%v", info.ExecutedCoreCommands, bs.ExecutedCoreCommands(path))
	}
	if len(info.ProjectionMismatches) != 0 {
		t.Fatalf("projection_mismatches=%v", info.ProjectionMismatches)
	}
	if info.PublishHealthy {
		t.Fatalf("debug surface must not overclaim healthy on primary path without durable boundary: %+v", info)
	}
}

func TestBlockService_DebugInfoForVolume_UsesCoreProjectionReplicaPath(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "vol-debug-replica")

	errs := bs.ApplyAssignments([]blockvol.BlockVolumeAssignment{
		{
			Path:            path,
			Epoch:           1,
			Role:            blockvol.RoleToWire(blockvol.RoleReplica),
			LeaseTtlMs:      30000,
			ReplicaDataAddr: "127.0.0.1:0",
			ReplicaCtrlAddr: "127.0.0.1:0",
		},
	})
	if len(errs) != 1 || errs[0] != nil {
		t.Fatalf("apply assignment errs=%v", errs)
	}

	vol, ok := bs.blockStore.GetBlockVolume(path)
	if !ok {
		t.Fatal("volume not found")
	}
	info := bs.DebugInfoForVolume(path, vol)
	proj, ok := bs.CoreProjection(path)
	if !ok {
		t.Fatal("expected core projection for debug surface")
	}
	if info.Role != string(proj.Role) {
		t.Fatalf("role=%q projection_role=%q", info.Role, proj.Role)
	}
	if info.Mode != string(proj.Mode.Name) {
		t.Fatalf("mode=%q projection_mode=%q", info.Mode, proj.Mode.Name)
	}
	if info.ReceiverReady != proj.Readiness.ReceiverReady {
		t.Fatalf("receiver_ready=%v projection=%v", info.ReceiverReady, proj.Readiness.ReceiverReady)
	}
	if info.ReplicaEligible != proj.Readiness.ReplicaReady {
		t.Fatalf("replica_eligible=%v projection_ready=%v", info.ReplicaEligible, proj.Readiness.ReplicaReady)
	}
	if info.PublishHealthy != proj.Publication.Healthy {
		t.Fatalf("publish_healthy=%v projection=%v", info.PublishHealthy, proj.Publication.Healthy)
	}
	if info.PublicationReason != proj.Publication.Reason {
		t.Fatalf("publication_reason=%q projection_reason=%q", info.PublicationReason, proj.Publication.Reason)
	}
	if info.CoreProjection == nil {
		t.Fatal("expected embedded core projection in debug info")
	}
	if info.CoreProjection.VolumeID != proj.VolumeID || info.CoreProjection.Role != proj.Role {
		t.Fatalf("embedded core projection diverged: got=%+v want=%+v", info.CoreProjection, proj)
	}
	if !reflect.DeepEqual(info.ExecutedCoreCommands, bs.ExecutedCoreCommands(path)) {
		t.Fatalf("executed_core_commands=%v want=%v", info.ExecutedCoreCommands, bs.ExecutedCoreCommands(path))
	}
	if len(info.ProjectionMismatches) != 0 {
		t.Fatalf("projection_mismatches=%v", info.ProjectionMismatches)
	}
}

func TestBlockService_CollectBlockVolumeHeartbeat_PrimaryUsesCoreReadinessGate(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "vol-heartbeat-primary")

	errs := bs.ApplyAssignments([]blockvol.BlockVolumeAssignment{
		{
			Path:            path,
			Epoch:           1,
			Role:            blockvol.RoleToWire(blockvol.RolePrimary),
			LeaseTtlMs:      30000,
			ReplicaServerID: "vs-2",
			ReplicaDataAddr: "10.0.0.2:4260",
			ReplicaCtrlAddr: "10.0.0.2:4261",
		},
	})
	if len(errs) != 1 || errs[0] != nil {
		t.Fatalf("apply assignment errs=%v", errs)
	}

	proj, ok := bs.CoreProjection(path)
	if !ok {
		t.Fatal("expected core projection for heartbeat surface")
	}
	if !proj.Readiness.ShipperConfigured {
		t.Fatalf("projection readiness=%+v", proj.Readiness)
	}
	if proj.Publication.Healthy {
		t.Fatalf("primary projection must still be publication-unhealthy here: %+v", proj.Publication)
	}

	bs.replMu.Lock()
	state := bs.replStates[path]
	if state == nil {
		bs.replMu.Unlock()
		t.Fatal("missing repl state")
	}
	state.publishHealthy = false
	bs.replMu.Unlock()

	msgs := bs.CollectBlockVolumeHeartbeat()
	msg := findHeartbeatMsg(msgs, path)
	if msg == nil {
		t.Fatal("volume missing from heartbeat")
	}
	if msg.ReplicaDataAddr != "10.0.0.2:4260" {
		t.Fatalf("ReplicaDataAddr=%q", msg.ReplicaDataAddr)
	}
	if msg.ReplicaCtrlAddr != "10.0.0.2:4261" {
		t.Fatalf("ReplicaCtrlAddr=%q", msg.ReplicaCtrlAddr)
	}
	if msg.ReplicaReady {
		t.Fatalf("primary heartbeat must not claim replica-ready truth, msg=%+v", msg)
	}
	if msg.VolumeMode != "bootstrap_pending" {
		t.Fatalf("expected explicit bootstrap_pending volume_mode on heartbeat, msg=%+v", msg)
	}
	if msg.VolumeModeReason != "awaiting_shipper_connected" {
		t.Fatalf("expected explicit bootstrap reason on heartbeat, msg=%+v", msg)
	}
}

func TestBlockService_CollectBlockVolumeHeartbeat_ReplicaUsesCoreReadinessGate(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "vol-heartbeat-replica")

	errs := bs.ApplyAssignments([]blockvol.BlockVolumeAssignment{
		{
			Path:            path,
			Epoch:           1,
			Role:            blockvol.RoleToWire(blockvol.RoleReplica),
			LeaseTtlMs:      30000,
			ReplicaDataAddr: "127.0.0.1:0",
			ReplicaCtrlAddr: "127.0.0.1:0",
		},
	})
	if len(errs) != 1 || errs[0] != nil {
		t.Fatalf("apply assignment errs=%v", errs)
	}

	proj, ok := bs.CoreProjection(path)
	if !ok {
		t.Fatal("expected core projection for heartbeat surface")
	}
	if !proj.Readiness.ReceiverReady {
		t.Fatalf("projection readiness=%+v", proj.Readiness)
	}
	if proj.Publication.Healthy {
		t.Fatalf("replica projection must not own healthy publication: %+v", proj.Publication)
	}

	bs.replMu.Lock()
	state := bs.replStates[path]
	if state == nil {
		bs.replMu.Unlock()
		t.Fatal("missing repl state")
	}
	expectedData := state.replicaDataAddr
	expectedCtrl := state.replicaCtrlAddr
	state.publishHealthy = false
	bs.replMu.Unlock()

	msgs := bs.CollectBlockVolumeHeartbeat()
	msg := findHeartbeatMsg(msgs, path)
	if msg == nil {
		t.Fatal("volume missing from heartbeat")
	}
	if msg.ReplicaDataAddr != expectedData {
		t.Fatalf("ReplicaDataAddr=%q expected=%q", msg.ReplicaDataAddr, expectedData)
	}
	if msg.ReplicaCtrlAddr != expectedCtrl {
		t.Fatalf("ReplicaCtrlAddr=%q expected=%q", msg.ReplicaCtrlAddr, expectedCtrl)
	}
	if !msg.ReplicaReady {
		t.Fatalf("replica heartbeat should carry explicit replica-ready truth, msg=%+v", msg)
	}
}

func TestBlockService_ReadinessSnapshot_PrefersCoreProjectionPrimaryFields(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "vol-readiness-primary")

	errs := bs.ApplyAssignments([]blockvol.BlockVolumeAssignment{
		{
			Path:            path,
			Epoch:           1,
			Role:            blockvol.RoleToWire(blockvol.RolePrimary),
			LeaseTtlMs:      30000,
			ReplicaServerID: "vs-2",
			ReplicaDataAddr: "10.0.0.2:4260",
			ReplicaCtrlAddr: "10.0.0.2:4261",
		},
	})
	if len(errs) != 1 || errs[0] != nil {
		t.Fatalf("apply assignment errs=%v", errs)
	}

	bs.replMu.Lock()
	state := bs.replStates[path]
	if state == nil {
		bs.replMu.Unlock()
		t.Fatal("missing repl state")
	}
	state.roleApplied = false
	state.shipperConfigured = false
	state.publishHealthy = false
	bs.replMu.Unlock()

	snap := bs.ReadinessSnapshot(path)
	if !snap.RoleApplied {
		t.Fatalf("expected snapshot role_applied from core projection, got %+v", snap)
	}
	if !snap.ShipperConfigured {
		t.Fatalf("expected snapshot shipper_configured from core projection, got %+v", snap)
	}
	if snap.PublishHealthy {
		t.Fatalf("publish_healthy should follow core publication truth on primary path without durable boundary, got %+v", snap)
	}
}

func TestBlockService_ReadinessSnapshot_PrefersCorePublicationHealth(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "vol-readiness-publish-healthy")

	errs := bs.ApplyAssignments([]blockvol.BlockVolumeAssignment{
		{
			Path:            path,
			Epoch:           1,
			Role:            blockvol.RoleToWire(blockvol.RolePrimary),
			LeaseTtlMs:      30000,
			ReplicaServerID: "vs-2",
			ReplicaDataAddr: "10.0.0.2:4260",
			ReplicaCtrlAddr: "10.0.0.2:4261",
		},
	})
	if len(errs) != 1 || errs[0] != nil {
		t.Fatalf("apply assignment errs=%v", errs)
	}

	bs.applyCoreEvent(engine.ShipperConnectedObserved{ID: path})
	bs.applyCoreEvent(engine.BarrierAccepted{ID: path, FlushedLSN: 12})

	bs.replMu.Lock()
	state := bs.replStates[path]
	if state == nil {
		bs.replMu.Unlock()
		t.Fatal("missing repl state")
	}
	state.publishHealthy = false
	bs.replMu.Unlock()

	snap := bs.ReadinessSnapshot(path)
	if !snap.PublishHealthy {
		t.Fatalf("publish_healthy should follow core publication truth once healthy, got %+v", snap)
	}
	if mismatches := bs.CoreProjectionMismatches(path); len(mismatches) != 0 {
		t.Fatalf("readiness/core mismatches=%v", mismatches)
	}
}

func TestBlockService_PrimaryPublicationChain_BootstrapPendingUntilBarrierThenHealthy(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "vol-publication-chain")

	errs := bs.ApplyAssignments([]blockvol.BlockVolumeAssignment{
		{
			Path:            path,
			Epoch:           1,
			Role:            blockvol.RoleToWire(blockvol.RolePrimary),
			LeaseTtlMs:      30000,
			ReplicaServerID: "vs-2",
			ReplicaDataAddr: "10.0.0.2:4260",
			ReplicaCtrlAddr: "10.0.0.2:4261",
		},
	})
	if len(errs) != 1 || errs[0] != nil {
		t.Fatalf("apply assignment errs=%v", errs)
	}

	initial, ok := bs.CoreProjection(path)
	if !ok {
		t.Fatal("expected initial core projection")
	}
	if initial.Mode.Name != engine.ModeBootstrapPending {
		t.Fatalf("initial mode=%s", initial.Mode.Name)
	}
	if initial.Publication.Reason != "awaiting_shipper_connected" {
		t.Fatalf("initial publication_reason=%q", initial.Publication.Reason)
	}

	bs.applyCoreEvent(engine.ShipperConnectedObserved{ID: path})

	connected, ok := bs.CoreProjection(path)
	if !ok {
		t.Fatal("expected connected projection")
	}
	if connected.Publication.Healthy {
		t.Fatalf("shipper contact alone must not publish healthy, projection=%+v", connected.Publication)
	}
	if connected.Publication.Reason != "awaiting_barrier_durability" {
		t.Fatalf("connected publication_reason=%q", connected.Publication.Reason)
	}
	connectedMsg := findHeartbeatMsg(bs.CollectBlockVolumeHeartbeat(), path)
	if connectedMsg == nil {
		t.Fatal("volume missing from heartbeat after shipper connect")
	}
	if connectedMsg.PublishHealthy {
		t.Fatalf("heartbeat must not claim publish_healthy before barrier, msg=%+v", connectedMsg)
	}
	if connectedMsg.VolumeMode != "bootstrap_pending" {
		t.Fatalf("heartbeat mode before barrier=%q, want bootstrap_pending", connectedMsg.VolumeMode)
	}

	bs.applyCoreEvent(engine.BarrierAccepted{ID: path, FlushedLSN: 12})

	healthy, ok := bs.CoreProjection(path)
	if !ok {
		t.Fatal("expected healthy projection")
	}
	if healthy.Mode.Name != engine.ModePublishHealthy {
		t.Fatalf("healthy mode=%s", healthy.Mode.Name)
	}
	if !healthy.Publication.Healthy {
		t.Fatalf("expected healthy publication after barrier, projection=%+v", healthy.Publication)
	}
	healthyMsg := findHeartbeatMsg(bs.CollectBlockVolumeHeartbeat(), path)
	if healthyMsg == nil {
		t.Fatal("volume missing from heartbeat after barrier")
	}
	if !healthyMsg.PublishHealthy {
		t.Fatalf("heartbeat must claim publish_healthy after barrier, msg=%+v", healthyMsg)
	}
	if healthyMsg.VolumeMode != "publish_healthy" {
		t.Fatalf("heartbeat mode after barrier=%q, want publish_healthy", healthyMsg.VolumeMode)
	}
}

func TestBlockService_ReadinessSnapshot_PrefersCoreProjectionReplicaFields(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "vol-readiness-replica")

	errs := bs.ApplyAssignments([]blockvol.BlockVolumeAssignment{
		{
			Path:            path,
			Epoch:           1,
			Role:            blockvol.RoleToWire(blockvol.RoleReplica),
			LeaseTtlMs:      30000,
			ReplicaDataAddr: "127.0.0.1:0",
			ReplicaCtrlAddr: "127.0.0.1:0",
		},
	})
	if len(errs) != 1 || errs[0] != nil {
		t.Fatalf("apply assignment errs=%v", errs)
	}

	bs.replMu.Lock()
	state := bs.replStates[path]
	if state == nil {
		bs.replMu.Unlock()
		t.Fatal("missing repl state")
	}
	state.receiverReady = false
	state.replicaEligible = false
	state.publishHealthy = false
	bs.replMu.Unlock()

	snap := bs.ReadinessSnapshot(path)
	if !snap.ReceiverReady {
		t.Fatalf("expected snapshot receiver_ready from core projection, got %+v", snap)
	}
	if !snap.ReplicaEligible {
		t.Fatalf("expected snapshot replica_eligible from core projection, got %+v", snap)
	}
	if snap.PublishHealthy {
		t.Fatalf("publish_healthy should follow core publication truth on replica path, got %+v", snap)
	}
}

func TestBlockService_ShipperStateChange_InSyncEmitsCoreConnectedObservation(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "vol-shipper-connected-callback")
	ch := make(chan bool, 1)
	bs.WireStateChangeNotify(ch)

	errs := bs.ApplyAssignments([]blockvol.BlockVolumeAssignment{
		{
			Path:            path,
			Epoch:           1,
			Role:            blockvol.RoleToWire(blockvol.RolePrimary),
			LeaseTtlMs:      30000,
			ReplicaServerID: "vs-2",
			ReplicaDataAddr: "10.0.0.2:4260",
			ReplicaCtrlAddr: "10.0.0.2:4261",
		},
	})
	if len(errs) != 1 || errs[0] != nil {
		t.Fatalf("apply assignment errs=%v", errs)
	}

	before, ok := bs.CoreProjection(path)
	if !ok {
		t.Fatal("expected core projection before shipper state change")
	}
	if before.Publication.Reason != "awaiting_shipper_connected" {
		t.Fatalf("before reason=%q, want awaiting_shipper_connected", before.Publication.Reason)
	}

	bs.handleShipperStateChange(path, blockvol.ReplicaDisconnected, blockvol.ReplicaInSync, ch)

	select {
	case <-ch:
	default:
		t.Fatal("expected immediate heartbeat notification")
	}

	after, ok := bs.CoreProjection(path)
	if !ok {
		t.Fatal("expected core projection after shipper state change")
	}
	if !after.Readiness.ShipperConnected {
		t.Fatalf("expected shipper_connected=true after callback, projection=%+v", after)
	}
	if after.Publication.Reason != "awaiting_barrier_durability" {
		t.Fatalf("after reason=%q, want awaiting_barrier_durability", after.Publication.Reason)
	}
	if after.Publication.Healthy {
		t.Fatalf("shipper connect alone must not publish healthy, projection=%+v", after)
	}
}

func TestBlockService_ObservePrimaryShipperConnectivityStatus_EmitsCoreConnectedObservation(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "vol-shipper-connected-recheck")

	errs := bs.ApplyAssignments([]blockvol.BlockVolumeAssignment{
		{
			Path:            path,
			Epoch:           1,
			Role:            blockvol.RoleToWire(blockvol.RolePrimary),
			LeaseTtlMs:      30000,
			ReplicaServerID: "vs-2",
			ReplicaDataAddr: "10.0.0.2:4260",
			ReplicaCtrlAddr: "10.0.0.2:4261",
		},
	})
	if len(errs) != 1 || errs[0] != nil {
		t.Fatalf("apply assignment errs=%v", errs)
	}

	before, ok := bs.CoreProjection(path)
	if !ok {
		t.Fatal("expected core projection before connectivity observation")
	}
	if before.Publication.Reason != "awaiting_shipper_connected" {
		t.Fatalf("before reason=%q, want awaiting_shipper_connected", before.Publication.Reason)
	}

	bs.observePrimaryShipperConnectivityStatus(path, true)

	after, ok := bs.CoreProjection(path)
	if !ok {
		t.Fatal("expected core projection after connectivity observation")
	}
	if !after.Readiness.ShipperConnected {
		t.Fatalf("expected shipper_connected=true after recheck, projection=%+v", after)
	}
	if after.Publication.Reason != "awaiting_barrier_durability" {
		t.Fatalf("after reason=%q, want awaiting_barrier_durability", after.Publication.Reason)
	}
}

func TestBlockService_HeartbeatReplicaDegraded_UsesCoreMode(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "vol-heartbeat-degraded")

	errs := bs.ApplyAssignments([]blockvol.BlockVolumeAssignment{
		{
			Path:            path,
			Epoch:           1,
			Role:            blockvol.RoleToWire(blockvol.RoleReplica),
			LeaseTtlMs:      30000,
			ReplicaDataAddr: "127.0.0.1:0",
			ReplicaCtrlAddr: "127.0.0.1:0",
		},
	})
	if len(errs) != 1 || errs[0] != nil {
		t.Fatalf("apply assignment errs=%v", errs)
	}

	bs.applyCoreEvent(engine.NeedsRebuildObserved{ID: path, Reason: "gap_too_large"})

	proj, ok := bs.CoreProjection(path)
	if !ok {
		t.Fatal("expected core projection for heartbeat degraded mapping")
	}
	if proj.Mode.Name != engine.ModeNeedsRebuild {
		t.Fatalf("mode=%s", proj.Mode.Name)
	}
	if !bs.heartbeatReplicaDegraded(path, false) {
		t.Fatalf("expected degraded bit from core mode even when current=false, projection=%+v", proj)
	}
}

func TestBlockService_CollectBlockVolumeHeartbeat_PrimaryNeedsRebuildUsesCoreMode(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "vol-heartbeat-needs-rebuild-primary")

	errs := bs.ApplyAssignments([]blockvol.BlockVolumeAssignment{
		{
			Path:            path,
			Epoch:           1,
			Role:            blockvol.RoleToWire(blockvol.RolePrimary),
			LeaseTtlMs:      30000,
			ReplicaServerID: "vs-2",
			ReplicaDataAddr: "10.0.0.2:4260",
			ReplicaCtrlAddr: "10.0.0.2:4261",
		},
	})
	if len(errs) != 1 || errs[0] != nil {
		t.Fatalf("apply assignment errs=%v", errs)
	}

	bs.applyCoreEvent(engine.NeedsRebuildObserved{ID: path, ReplicaID: path + "/vs-2", Reason: "gap_too_large"})

	msg := findHeartbeatMsg(bs.CollectBlockVolumeHeartbeat(), path)
	if msg == nil {
		t.Fatal("volume missing from heartbeat")
	}
	if !msg.NeedsRebuild {
		t.Fatalf("expected explicit needs_rebuild truth on heartbeat, msg=%+v", msg)
	}
	if !msg.ReplicaDegraded {
		t.Fatalf("expected degraded bit to remain true on needs_rebuild path, msg=%+v", msg)
	}
	if msg.VolumeMode != "needs_rebuild" {
		t.Fatalf("expected explicit needs_rebuild volume_mode on heartbeat, msg=%+v", msg)
	}
	if msg.VolumeModeReason != "gap_too_large" {
		t.Fatalf("expected explicit needs_rebuild reason on heartbeat, msg=%+v", msg)
	}
}

func TestBlockService_CollectBlockVolumeHeartbeat_PrimaryPublishHealthyUsesCoreTruth(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "vol-heartbeat-publish-healthy-primary")

	errs := bs.ApplyAssignments([]blockvol.BlockVolumeAssignment{
		{
			Path:            path,
			Epoch:           1,
			Role:            blockvol.RoleToWire(blockvol.RolePrimary),
			LeaseTtlMs:      30000,
			ReplicaServerID: "vs-2",
			ReplicaDataAddr: "10.0.0.2:4260",
			ReplicaCtrlAddr: "10.0.0.2:4261",
		},
	})
	if len(errs) != 1 || errs[0] != nil {
		t.Fatalf("apply assignment errs=%v", errs)
	}

	bs.applyCoreEvent(engine.ShipperConnectedObserved{ID: path})
	bs.applyCoreEvent(engine.BarrierAccepted{ID: path, FlushedLSN: 12})

	msg := findHeartbeatMsg(bs.CollectBlockVolumeHeartbeat(), path)
	if msg == nil {
		t.Fatal("volume missing from heartbeat")
	}
	if !msg.PublishHealthy {
		t.Fatalf("expected explicit publish_healthy truth on heartbeat, msg=%+v", msg)
	}
	if msg.VolumeMode != "publish_healthy" {
		t.Fatalf("expected explicit publish_healthy volume_mode on heartbeat, msg=%+v", msg)
	}
	if msg.VolumeModeReason != "" {
		t.Fatalf("expected empty publish_healthy mode reason on heartbeat, msg=%+v", msg)
	}
}

func TestBlockService_CollectBlockVolumeHeartbeat_PrimaryDegradedUsesCoreModeTruth(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "vol-heartbeat-degraded-primary")

	errs := bs.ApplyAssignments([]blockvol.BlockVolumeAssignment{
		{
			Path:            path,
			Epoch:           1,
			Role:            blockvol.RoleToWire(blockvol.RolePrimary),
			LeaseTtlMs:      30000,
			ReplicaServerID: "vs-2",
			ReplicaDataAddr: "10.0.0.2:4260",
			ReplicaCtrlAddr: "10.0.0.2:4261",
		},
	})
	if len(errs) != 1 || errs[0] != nil {
		t.Fatalf("apply assignment errs=%v", errs)
	}

	bs.applyCoreEvent(engine.BarrierRejected{ID: path, Reason: "barrier_timeout"})

	msg := findHeartbeatMsg(bs.CollectBlockVolumeHeartbeat(), path)
	if msg == nil {
		t.Fatal("volume missing from heartbeat")
	}
	if msg.VolumeMode != "degraded" {
		t.Fatalf("expected explicit degraded volume_mode on heartbeat, msg=%+v", msg)
	}
	if msg.VolumeModeReason != "barrier_timeout" {
		t.Fatalf("expected explicit degraded reason on heartbeat, msg=%+v", msg)
	}
}

func TestBlockService_HeartbeatIncludesReplicaAddrs(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "vol1")

	bs.replMu.Lock()
	bs.replStates[path] = &volReplState{
		replicaDataAddr: "10.0.0.5:4260",
		replicaCtrlAddr: "10.0.0.5:4261",
	}
	bs.replMu.Unlock()

	dataAddr, ctrlAddr := bs.GetReplState(path)
	if dataAddr != "10.0.0.5:4260" || ctrlAddr != "10.0.0.5:4261" {
		t.Fatalf("got data=%q ctrl=%q", dataAddr, ctrlAddr)
	}
}

func TestBlockService_ReplicationPorts_Deterministic(t *testing.T) {
	bs := &BlockService{listenAddr: "0.0.0.0:3260"}
	d1, c1, r1 := bs.ReplicationPorts("/data/vol1.blk")
	d2, c2, r2 := bs.ReplicationPorts("/data/vol1.blk")
	if d1 != d2 || c1 != c2 || r1 != r2 {
		t.Fatalf("ports not deterministic")
	}
	if c1 != d1+1 || r1 != d1+2 {
		t.Fatalf("port offsets wrong: data=%d ctrl=%d rebuild=%d", d1, c1, r1)
	}
}

func TestBlockService_ReplicationPorts_StableAcrossRestarts(t *testing.T) {
	bs1 := &BlockService{listenAddr: "0.0.0.0:3260"}
	bs2 := &BlockService{listenAddr: "0.0.0.0:3260"}
	d1, _, _ := bs1.ReplicationPorts("/data/vol1.blk")
	d2, _, _ := bs2.ReplicationPorts("/data/vol1.blk")
	if d1 != d2 {
		t.Fatalf("ports not stable: %d vs %d", d1, d2)
	}
}

func TestBlockService_ProcessAssignment_InvalidTransition(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "vol1")

	// Assign as primary epoch 5.
	bs.ProcessAssignments([]blockvol.BlockVolumeAssignment{
		{Path: path, Epoch: 5, Role: blockvol.RoleToWire(blockvol.RolePrimary), LeaseTtlMs: 30000},
	})

	// Try to assign with lower epoch — should be rejected silently.
	bs.ProcessAssignments([]blockvol.BlockVolumeAssignment{
		{Path: path, Epoch: 3, Role: blockvol.RoleToWire(blockvol.RoleReplica), LeaseTtlMs: 30000},
	})

	vol, _ := bs.blockStore.GetBlockVolume(path)
	s := vol.Status()
	if s.Epoch != 5 {
		t.Fatalf("epoch should still be 5, got %d", s.Epoch)
	}
}

func countCommandName(cmds []string, want string) int {
	count := 0
	for _, cmd := range cmds {
		if cmd == want {
			count++
		}
	}
	return count
}
