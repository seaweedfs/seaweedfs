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
		Replicas:      []ReplicaInfo{{Server: "vs2:9333", Role: blockvol.RoleToWire(blockvol.RoleReplica)}},
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
		Replicas:      []ReplicaInfo{{Server: "vs2:9333", Role: blockvol.RoleToWire(blockvol.RoleReplica)}},
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
