package weed_server

import (
	"path/filepath"
	"reflect"
	"testing"

	engine "github.com/seaweedfs/seaweedfs/sw-block/engine/replication"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
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
	return &BlockService{
		blockStore:    store,
		blockDir:      dir,
		listenAddr:    "0.0.0.0:3260",
		iqnPrefix:     "iqn.2024-01.com.seaweedfs:vol.",
		replStates:    make(map[string]*volReplState),
		v2Core:        engine.NewCoreEngine(),
		coreProj:      make(map[string]engine.PublicationProjection),
		localServerID: "vs-test",
	}
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
	if got := bs.ExecutedCoreCommands(path); !reflect.DeepEqual(got, []string{"apply_role", "configure_shipper", "invalidate_session"}) {
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

	if !reflect.DeepEqual(first, []string{"apply_role", "configure_shipper", "invalidate_session"}) {
		t.Fatalf("first executed commands=%v", first)
	}
	if !reflect.DeepEqual(second, first) {
		t.Fatalf("repeated failure should not re-execute invalidate_session: first=%v second=%v", first, second)
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
	if !readiness.PublishHealthy {
		t.Fatalf("expected adapter-local readiness to still report publish healthy, got %+v", readiness)
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
		t.Fatalf("publish_healthy should remain adapter-local on readiness snapshot, got %+v", snap)
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
		t.Fatalf("publish_healthy should remain adapter-local on readiness snapshot, got %+v", snap)
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
