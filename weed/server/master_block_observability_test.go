package weed_server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	engine "github.com/seaweedfs/seaweedfs/sw-block/engine/replication"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/blockapi"
)

// --- Health-state derivation tests ---

func TestHealthState_Healthy_PrimaryWithReplicas(t *testing.T) {
	entry := &BlockVolumeEntry{
		Role:          blockvol.RoleToWire(blockvol.RolePrimary),
		ReplicaFactor: 2,
		Replicas: []ReplicaInfo{
			{Server: "vs2:9333", Role: blockvol.RoleToWire(blockvol.RoleReplica)},
		},
	}
	state := deriveHealthState(entry)
	if state != HealthStateHealthy {
		t.Fatalf("expected %q, got %q", HealthStateHealthy, state)
	}
}

func TestHealthState_Unsafe_NotPrimary(t *testing.T) {
	entry := &BlockVolumeEntry{
		Role:          blockvol.RoleToWire(blockvol.RoleReplica),
		ReplicaFactor: 2,
	}
	state := deriveHealthState(entry)
	if state != HealthStateUnsafe {
		t.Fatalf("expected %q, got %q", HealthStateUnsafe, state)
	}
}

func TestHealthState_Unsafe_StrictBelowRequired(t *testing.T) {
	entry := &BlockVolumeEntry{
		Role:           blockvol.RoleToWire(blockvol.RolePrimary),
		ReplicaFactor:  2,
		DurabilityMode: "sync_all",
		Replicas:       nil, // 0 replicas, sync_all requires 1
	}
	state := deriveHealthState(entry)
	if state != HealthStateUnsafe {
		t.Fatalf("expected %q for sync_all with 0 replicas, got %q", HealthStateUnsafe, state)
	}
}

func TestHealthState_Rebuilding(t *testing.T) {
	entry := &BlockVolumeEntry{
		Role:          blockvol.RoleToWire(blockvol.RolePrimary),
		ReplicaFactor: 2,
		Replicas: []ReplicaInfo{
			{Server: "vs2:9333", Role: blockvol.RoleToWire(blockvol.RoleRebuilding)},
		},
	}
	state := deriveHealthState(entry)
	if state != HealthStateRebuilding {
		t.Fatalf("expected %q, got %q", HealthStateRebuilding, state)
	}
}

func TestHealthState_Degraded_BelowDesired(t *testing.T) {
	entry := &BlockVolumeEntry{
		Role:          blockvol.RoleToWire(blockvol.RolePrimary),
		ReplicaFactor: 3,
		Replicas: []ReplicaInfo{
			{Server: "vs2:9333", Role: blockvol.RoleToWire(blockvol.RoleReplica)},
		},
		// RF=3 needs 2 replicas, only has 1
	}
	state := deriveHealthState(entry)
	if state != HealthStateDegraded {
		t.Fatalf("expected %q, got %q", HealthStateDegraded, state)
	}
}

func TestHealthState_Degraded_FlagSet(t *testing.T) {
	entry := &BlockVolumeEntry{
		Role:            blockvol.RoleToWire(blockvol.RolePrimary),
		ReplicaFactor:   2,
		Replicas:        []ReplicaInfo{{Server: "vs2:9333", Role: blockvol.RoleToWire(blockvol.RoleReplica)}},
		ReplicaDegraded: true,
	}
	state := deriveHealthState(entry)
	if state != HealthStateDegraded {
		t.Fatalf("expected %q, got %q", HealthStateDegraded, state)
	}
}

func TestHealthState_WithLiveness_PrimaryDead(t *testing.T) {
	entry := &BlockVolumeEntry{
		Role:          blockvol.RoleToWire(blockvol.RolePrimary),
		ReplicaFactor: 2,
		Replicas:      []ReplicaInfo{{Server: "vs2:9333", Role: blockvol.RoleToWire(blockvol.RoleReplica)}},
	}
	state := deriveHealthStateWithLiveness(entry, false)
	if state != HealthStateUnsafe {
		t.Fatalf("expected %q when primary dead, got %q", HealthStateUnsafe, state)
	}
}

func TestHealthState_BestEffort_ZeroReplicas_NotUnsafe(t *testing.T) {
	// best_effort with RF=2 but 0 replicas: degraded, not unsafe
	entry := &BlockVolumeEntry{
		Role:           blockvol.RoleToWire(blockvol.RolePrimary),
		ReplicaFactor:  2,
		DurabilityMode: "best_effort",
		Replicas:       nil,
	}
	state := deriveHealthState(entry)
	if state != HealthStateDegraded {
		t.Fatalf("expected %q for best_effort with 0 replicas, got %q", HealthStateDegraded, state)
	}
}

func TestHealthState_RF1_NoReplicas_Healthy(t *testing.T) {
	entry := &BlockVolumeEntry{
		Role:          blockvol.RoleToWire(blockvol.RolePrimary),
		ReplicaFactor: 1,
		Replicas:      nil,
	}
	state := deriveHealthState(entry)
	if state != HealthStateHealthy {
		t.Fatalf("expected %q for RF=1, got %q", HealthStateHealthy, state)
	}
}

// --- Cluster health summary test ---

func TestClusterHealthSummary(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("vs1:9333")
	r.MarkBlockCapable("vs2:9333")

	// Register a healthy volume
	r.Register(&BlockVolumeEntry{
		Name:          "healthy-vol",
		VolumeServer:  "vs1:9333",
		Path:          "/data/healthy.blk",
		Role:          blockvol.RoleToWire(blockvol.RolePrimary),
		ReplicaFactor: 2,
		ReplicaReady:  true,
		Replicas:      []ReplicaInfo{{Server: "vs2:9333", Role: blockvol.RoleToWire(blockvol.RoleReplica), Ready: true}},
		Status:        StatusActive,
	})

	// Register a degraded volume (missing replica)
	r.Register(&BlockVolumeEntry{
		Name:          "degraded-vol",
		VolumeServer:  "vs1:9333",
		Path:          "/data/degraded.blk",
		Role:          blockvol.RoleToWire(blockvol.RolePrimary),
		ReplicaFactor: 2,
		Replicas:      nil, // missing replica
		Status:        StatusActive,
	})

	summary := r.ComputeClusterHealthSummary()
	if summary.Healthy != 1 {
		t.Fatalf("expected 1 healthy, got %d", summary.Healthy)
	}
	if summary.Degraded != 1 {
		t.Fatalf("expected 1 degraded, got %d", summary.Degraded)
	}
	if summary.Unsafe != 0 {
		t.Fatalf("expected 0 unsafe, got %d", summary.Unsafe)
	}
}

// --- Status handler test ---

func TestBlockStatusHandler_IncludesHealthCounts(t *testing.T) {
	ms := qaPlanMaster(t)

	// Create a volume to have some state
	r := ms.blockRegistry
	r.Register(&BlockVolumeEntry{
		Name:          "status-test",
		VolumeServer:  "vs1:9333",
		Path:          "/data/status.blk",
		Role:          blockvol.RoleToWire(blockvol.RolePrimary),
		ReplicaFactor: 2,
		ReplicaReady:  true,
		Replicas:      []ReplicaInfo{{Server: "vs2:9333", Role: blockvol.RoleToWire(blockvol.RoleReplica), Ready: true}},
		Status:        StatusActive,
	})

	req := httptest.NewRequest(http.MethodGet, "/block/status", nil)
	w := httptest.NewRecorder()
	ms.blockStatusHandler(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp blockapi.BlockStatusResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.VolumeCount != 1 {
		t.Fatalf("expected volume_count=1, got %d", resp.VolumeCount)
	}
	if resp.HealthyCount != 1 {
		t.Fatalf("expected healthy_count=1, got %d", resp.HealthyCount)
	}
	if resp.NvmeCapableServers < 0 {
		t.Fatal("nvme_capable_servers should be >= 0")
	}
}

func TestBlockStatusHandler_ReflectsCoreInfluencedConsumeCounts(t *testing.T) {
	ms := qaPlanMaster(t)

	// Ready path: replica heartbeat publishes receiver addrs from the
	// core-influenced ready path, then registry consumes it into a healthy volume.
	readyBS := newTestBlockServiceDirect(t)
	readyPath := createTestVolDirect(t, readyBS, "status-ready")
	errs := readyBS.ApplyAssignments([]blockvol.BlockVolumeAssignment{{
		Path:            readyPath,
		Epoch:           1,
		Role:            blockvol.RoleToWire(blockvol.RoleReplica),
		LeaseTtlMs:      30000,
		ReplicaDataAddr: "127.0.0.1:0",
		ReplicaCtrlAddr: "127.0.0.1:0",
	}})
	if len(errs) != 1 || errs[0] != nil {
		t.Fatalf("ready apply errs=%v", errs)
	}
	readyBS.replMu.Lock()
	readyState := readyBS.replStates[readyPath]
	if readyState == nil {
		readyBS.replMu.Unlock()
		t.Fatal("missing ready repl state")
	}
	readyState.publishHealthy = false
	readyBS.replMu.Unlock()
	readyHB := findHeartbeatMsg(readyBS.CollectBlockVolumeHeartbeat(), readyPath)
	if readyHB == nil {
		t.Fatal("ready heartbeat missing")
	}

	if err := ms.blockRegistry.Register(&BlockVolumeEntry{
		Name:          "status-ready",
		VolumeServer:  "vs1:9333",
		Path:          "/blocks/status-ready-primary.blk",
		Epoch:         1,
		Status:        StatusActive,
		Role:          blockvol.RoleToWire(blockvol.RolePrimary),
		ReplicaFactor: 2,
		Replicas: []ReplicaInfo{{
			Server: "vs2:9333",
			Path:   readyPath,
		}},
	}); err != nil {
		t.Fatalf("register ready: %v", err)
	}
	ms.blockRegistry.mu.Lock()
	ms.blockRegistry.addToServer("vs2:9333", "status-ready")
	ms.blockRegistry.mu.Unlock()
	ms.blockRegistry.UpdateFullHeartbeat("vs2:9333", blockvol.InfoMessagesToProto([]blockvol.BlockVolumeInfoMessage{*readyHB}), "")

	// Degraded path: primary heartbeat publishes degraded bit from the
	// core-influenced degraded path, then registry consumes it into a degraded volume.
	degradedBS := newTestBlockServiceDirect(t)
	degradedPath := createTestVolDirect(t, degradedBS, "status-degraded")
	errs = degradedBS.ApplyAssignments([]blockvol.BlockVolumeAssignment{{
		Path:            degradedPath,
		Epoch:           1,
		Role:            blockvol.RoleToWire(blockvol.RolePrimary),
		LeaseTtlMs:      30000,
		ReplicaServerID: "vs1:9333",
		ReplicaDataAddr: "10.0.0.3:4260",
		ReplicaCtrlAddr: "10.0.0.3:4261",
	}})
	if len(errs) != 1 || errs[0] != nil {
		t.Fatalf("degraded apply errs=%v", errs)
	}
	degradedBS.applyCoreEvent(engine.BarrierRejected{ID: degradedPath, Reason: "barrier_timeout"})
	degradedHB := findHeartbeatMsg(degradedBS.CollectBlockVolumeHeartbeat(), degradedPath)
	if degradedHB == nil {
		t.Fatal("degraded heartbeat missing")
	}

	if err := ms.blockRegistry.Register(&BlockVolumeEntry{
		Name:          "status-degraded",
		VolumeServer:  "vs3:9333",
		Path:          degradedPath,
		Epoch:         1,
		Status:        StatusActive,
		Role:          blockvol.RoleToWire(blockvol.RolePrimary),
		ReplicaFactor: 2,
		Replicas: []ReplicaInfo{{
			Server: "vs1:9333",
			Path:   "/blocks/status-degraded-replica.blk",
			Ready:  true,
		}},
	}); err != nil {
		t.Fatalf("register degraded: %v", err)
	}
	ms.blockRegistry.UpdateFullHeartbeat("vs3:9333", blockvol.InfoMessagesToProto([]blockvol.BlockVolumeInfoMessage{*degradedHB}), "")

	req := httptest.NewRequest(http.MethodGet, "/block/status", nil)
	w := httptest.NewRecorder()
	ms.blockStatusHandler(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp blockapi.BlockStatusResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.VolumeCount != 2 {
		t.Fatalf("expected volume_count=2, got %d", resp.VolumeCount)
	}
	if resp.HealthyCount != 1 {
		t.Fatalf("expected healthy_count=1, got %d", resp.HealthyCount)
	}
	if resp.DegradedCount != 1 {
		t.Fatalf("expected degraded_count=1, got %d", resp.DegradedCount)
	}
	if resp.RebuildingCount != 0 {
		t.Fatalf("expected rebuilding_count=0, got %d", resp.RebuildingCount)
	}
	if resp.UnsafeCount != 0 {
		t.Fatalf("expected unsafe_count=0, got %d", resp.UnsafeCount)
	}
}

// --- VolumeInfo includes health_state ---

func TestEntryToVolumeInfo_IncludesHealthState(t *testing.T) {
	entry := &BlockVolumeEntry{
		Name:          "health-vol",
		VolumeServer:  "vs1:9333",
		Role:          blockvol.RoleToWire(blockvol.RolePrimary),
		ReplicaFactor: 2,
		Replicas:      []ReplicaInfo{{Server: "vs2:9333", Role: blockvol.RoleToWire(blockvol.RoleReplica)}},
		Status:        StatusActive,
	}
	info := entryToVolumeInfo(entry, true) // primary alive
	if info.HealthState != HealthStateHealthy {
		t.Fatalf("expected %q, got %q", HealthStateHealthy, info.HealthState)
	}
}

func TestEntryToVolumeInfo_ReflectsCoreInfluencedReadyConsume(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "vol-info-ready")

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
	state.publishHealthy = false
	bs.replMu.Unlock()

	hb := findHeartbeatMsg(bs.CollectBlockVolumeHeartbeat(), path)
	if hb == nil {
		t.Fatal("heartbeat volume missing")
	}

	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("primary-server:8080")
	if err := r.Register(&BlockVolumeEntry{
		Name:          "vol-info-ready",
		VolumeServer:  "primary-server:8080",
		Path:          "/blocks/vol-info-ready-primary.blk",
		Status:        StatusActive,
		Role:          blockvol.RoleToWire(blockvol.RolePrimary),
		ReplicaFactor: 2,
		Replicas: []ReplicaInfo{{
			Server: "replica-server:8080",
			Path:   path,
		}},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	r.UpdateFullHeartbeat("replica-server:8080", blockvol.InfoMessagesToProto([]blockvol.BlockVolumeInfoMessage{*hb}), "")

	entry, _ := r.Lookup("vol-info-ready")
	info := entryToVolumeInfo(&entry, true)
	if !info.ReplicaReady {
		t.Fatalf("expected outward ReplicaReady=true, info=%+v", info)
	}
	if info.ReplicaDegraded {
		t.Fatalf("expected outward ReplicaDegraded=false, info=%+v", info)
	}
	if info.VolumeMode != "publish_healthy" {
		t.Fatalf("expected outward VolumeMode=publish_healthy, got %q", info.VolumeMode)
	}
	if info.HealthState != HealthStateHealthy {
		t.Fatalf("expected outward HealthState=%q, got %q", HealthStateHealthy, info.HealthState)
	}
}

func TestEntryToVolumeInfo_ReflectsCoreInfluencedDegradedConsume(t *testing.T) {
	bs := newTestBlockServiceDirect(t)
	path := createTestVolDirect(t, bs, "vol-info-degraded")

	errs := bs.ApplyAssignments([]blockvol.BlockVolumeAssignment{
		{
			Path:            path,
			Epoch:           1,
			Role:            blockvol.RoleToWire(blockvol.RolePrimary),
			LeaseTtlMs:      30000,
			ReplicaServerID: "replica-server:8080",
			ReplicaDataAddr: "10.0.0.2:4260",
			ReplicaCtrlAddr: "10.0.0.2:4261",
		},
	})
	if len(errs) != 1 || errs[0] != nil {
		t.Fatalf("apply assignment errs=%v", errs)
	}
	bs.applyCoreEvent(engine.BarrierRejected{ID: path, Reason: "barrier_timeout"})

	hb := findHeartbeatMsg(bs.CollectBlockVolumeHeartbeat(), path)
	if hb == nil {
		t.Fatal("heartbeat volume missing")
	}

	r := NewBlockVolumeRegistry()
	r.MarkBlockCapable("primary-server:8080")
	if err := r.Register(&BlockVolumeEntry{
		Name:          "vol-info-degraded",
		VolumeServer:  "primary-server:8080",
		Path:          path,
		Status:        StatusActive,
		Role:          blockvol.RoleToWire(blockvol.RolePrimary),
		ReplicaFactor: 2,
		Replicas: []ReplicaInfo{{
			Server: "replica-server:8080",
			Path:   "/blocks/vol-info-degraded-replica.blk",
			Ready:  true,
		}},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	r.UpdateFullHeartbeat("primary-server:8080", blockvol.InfoMessagesToProto([]blockvol.BlockVolumeInfoMessage{*hb}), "")

	entry, _ := r.Lookup("vol-info-degraded")
	info := entryToVolumeInfo(&entry, true)
	if !info.ReplicaDegraded {
		t.Fatalf("expected outward ReplicaDegraded=true, info=%+v", info)
	}
	if info.VolumeMode != "degraded" {
		t.Fatalf("expected outward VolumeMode=degraded, got %q", info.VolumeMode)
	}
	if info.HealthState != HealthStateDegraded {
		t.Fatalf("expected outward HealthState=%q, got %q", HealthStateDegraded, info.HealthState)
	}
}

func TestEntryToVolumeInfo_PrimaryDead_Unsafe(t *testing.T) {
	entry := &BlockVolumeEntry{
		Name:          "dead-primary-vol",
		VolumeServer:  "vs1:9333",
		Role:          blockvol.RoleToWire(blockvol.RolePrimary),
		ReplicaFactor: 2,
		Replicas:      []ReplicaInfo{{Server: "vs2:9333", Role: blockvol.RoleToWire(blockvol.RoleReplica)}},
		Status:        StatusActive,
	}
	info := entryToVolumeInfo(entry, false) // primary NOT alive
	if info.HealthState != HealthStateUnsafe {
		t.Fatalf("expected %q when primary dead, got %q", HealthStateUnsafe, info.HealthState)
	}
}

// --- Alert rules parse validity ---

func testdataDir() string {
	_, filename, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(filename), "..", "storage", "blockvol", "monitoring")
}

func TestAlertRules_FileExists(t *testing.T) {
	path := filepath.Join(testdataDir(), "alerts", "block-alerts.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("alert rules file not found: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("alert rules file is empty")
	}
	// Basic structure check: must contain "groups:" and "alert:" and known metric names
	content := string(data)
	for _, required := range []string{"groups:", "alert:", "seaweedfs_blockvol_"} {
		if !strings.Contains(content, required) {
			t.Fatalf("alert rules missing required content: %q", required)
		}
	}
}

func TestAlertRules_ReferencesRealMetrics(t *testing.T) {
	path := filepath.Join(testdataDir(), "alerts", "block-alerts.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Skipf("alert rules file not found: %v", err)
	}
	content := string(data)
	// All referenced metrics must be real exported metric names
	knownMetrics := []string{
		"seaweedfs_blockvol_wal_used_fraction",
		"seaweedfs_blockvol_replica_failed_barriers_total",
		"seaweedfs_blockvol_wal_pressure_state",
		"seaweedfs_blockvol_flusher_errors_total",
		"seaweedfs_blockvol_scrub_errors_total",
	}
	for _, m := range knownMetrics {
		if !strings.Contains(content, m) {
			t.Fatalf("alert rules should reference metric %q", m)
		}
	}
}

// --- Dashboard asset presence ---

func TestDashboard_FileExists(t *testing.T) {
	path := filepath.Join(testdataDir(), "dashboards", "block-overview.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("dashboard file not found: %v", err)
	}
	if len(data) == 0 {
		t.Fatal("dashboard file is empty")
	}
	// Must be valid JSON
	var dash map[string]interface{}
	if err := json.Unmarshal(data, &dash); err != nil {
		t.Fatalf("dashboard is not valid JSON: %v", err)
	}
	// Must have panels
	panels, ok := dash["panels"]
	if !ok {
		t.Fatal("dashboard missing 'panels' key")
	}
	panelList, ok := panels.([]interface{})
	if !ok || len(panelList) == 0 {
		t.Fatal("dashboard has no panels")
	}
}

func TestDashboard_QueriesReferenceRealMetrics(t *testing.T) {
	path := filepath.Join(testdataDir(), "dashboards", "block-overview.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Skipf("dashboard file not found: %v", err)
	}
	content := string(data)
	requiredMetrics := []string{
		"seaweedfs_blockvol_write_ops_total",
		"seaweedfs_blockvol_read_ops_total",
		"seaweedfs_blockvol_wal_used_fraction",
		"seaweedfs_blockvol_wal_pressure_state",
		"seaweedfs_blockvol_flusher_bytes_total",
		"seaweedfs_blockvol_flusher_errors_total",
	}
	for _, m := range requiredMetrics {
		if !strings.Contains(content, m) {
			t.Fatalf("dashboard should reference metric %q", m)
		}
	}
}
