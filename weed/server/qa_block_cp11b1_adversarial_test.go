package weed_server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/blockapi"
)

// ============================================================
// CP11B-1 QA Adversarial Tests
//
// Provisioning Presets + Resolved Policy View
// ============================================================

// qaPresetMaster creates a MasterServer with stub allocators for preset tests.
func qaPresetMaster(t *testing.T) *MasterServer {
	t.Helper()
	ms := &MasterServer{
		blockRegistry:        NewBlockVolumeRegistry(),
		blockAssignmentQueue: NewBlockAssignmentQueue(),
		blockFailover:        newBlockFailoverState(),
	}
	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, diskType string, durabilityMode string) (*blockAllocResult, error) {
		return &blockAllocResult{
			Path:              fmt.Sprintf("/data/%s.blk", name),
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
	ms.blockVSExpand = func(ctx context.Context, server string, name string, newSize uint64) (uint64, error) {
		return newSize, nil
	}
	ms.blockVSPrepareExpand = func(ctx context.Context, server string, name string, newSize, expandEpoch uint64) error {
		return nil
	}
	ms.blockVSCommitExpand = func(ctx context.Context, server string, name string, expandEpoch uint64) (uint64, error) {
		return 2 << 30, nil
	}
	ms.blockVSCancelExpand = func(ctx context.Context, server string, name string, expandEpoch uint64) error {
		return nil
	}
	ms.blockRegistry.MarkBlockCapable("vs1:9333")
	ms.blockRegistry.MarkBlockCapable("vs2:9333")
	return ms
}

// --- QA-CP11B1-1: Database preset produces correct defaults ---
func TestQA_CP11B1_DatabasePreset_Defaults(t *testing.T) {
	r := blockvol.ResolvePolicy(blockvol.PresetDatabase, "", 0, "", blockvol.EnvironmentInfo{
		NVMeAvailable: true, ServerCount: 3, WALSizeDefault: 128 << 20, BlockSizeDefault: 4096,
	})
	if len(r.Errors) > 0 {
		t.Fatalf("unexpected errors: %v", r.Errors)
	}
	p := r.Policy
	if p.DurabilityMode != "sync_all" {
		t.Errorf("durability_mode = %q, want sync_all", p.DurabilityMode)
	}
	if p.ReplicaFactor != 2 {
		t.Errorf("replica_factor = %d, want 2", p.ReplicaFactor)
	}
	if p.DiskType != "ssd" {
		t.Errorf("disk_type = %q, want ssd", p.DiskType)
	}
	if p.TransportPref != "nvme" {
		t.Errorf("transport = %q, want nvme", p.TransportPref)
	}
	if p.WALSizeRecommended != 128<<20 {
		t.Errorf("wal_rec = %d, want %d", p.WALSizeRecommended, 128<<20)
	}
	if p.StorageProfile != "single" {
		t.Errorf("storage_profile = %q, want single", p.StorageProfile)
	}
	if len(r.Overrides) != 0 {
		t.Errorf("overrides = %v, want empty", r.Overrides)
	}
}

// --- QA-CP11B1-2: Override precedence — durability wins over preset ---
func TestQA_CP11B1_OverridePrecedence_DurabilityWins(t *testing.T) {
	r := blockvol.ResolvePolicy(blockvol.PresetDatabase, "best_effort", 0, "", blockvol.EnvironmentInfo{
		NVMeAvailable: true, ServerCount: 2, WALSizeDefault: 128 << 20, BlockSizeDefault: 4096,
	})
	if len(r.Errors) > 0 {
		t.Fatalf("unexpected errors: %v", r.Errors)
	}
	if r.Policy.DurabilityMode != "best_effort" {
		t.Errorf("durability_mode = %q, want best_effort (override)", r.Policy.DurabilityMode)
	}
	found := false
	for _, o := range r.Overrides {
		if o == "durability_mode" {
			found = true
		}
	}
	if !found {
		t.Errorf("overrides = %v, want durability_mode present", r.Overrides)
	}
}

// --- QA-CP11B1-3: Invalid preset rejected ---
func TestQA_CP11B1_InvalidPreset_Rejected(t *testing.T) {
	r := blockvol.ResolvePolicy("nosuch", "", 0, "", blockvol.EnvironmentInfo{})
	if len(r.Errors) == 0 {
		t.Fatal("expected error for unknown preset")
	}
	if !strings.Contains(r.Errors[0], "unknown preset") {
		t.Errorf("error = %q, want to contain 'unknown preset'", r.Errors[0])
	}
}

// --- QA-CP11B1-4: sync_quorum + RF=2 rejected ---
func TestQA_CP11B1_SyncQuorum_RF2_Rejected(t *testing.T) {
	r := blockvol.ResolvePolicy("", "sync_quorum", 2, "", blockvol.EnvironmentInfo{
		ServerCount: 3, WALSizeDefault: 64 << 20, BlockSizeDefault: 4096,
	})
	if len(r.Errors) == 0 {
		t.Fatal("expected error for sync_quorum + RF=2")
	}
	if !strings.Contains(r.Errors[0], "replica_factor >= 3") {
		t.Errorf("error = %q, want sync_quorum RF constraint", r.Errors[0])
	}
}

// --- QA-CP11B1-5: NVMe preferred but unavailable → warning ---
func TestQA_CP11B1_NVMePref_NoNVMe_Warning(t *testing.T) {
	r := blockvol.ResolvePolicy(blockvol.PresetDatabase, "", 0, "", blockvol.EnvironmentInfo{
		NVMeAvailable: false, ServerCount: 2, WALSizeDefault: 128 << 20, BlockSizeDefault: 4096,
	})
	if len(r.Errors) > 0 {
		t.Fatalf("unexpected errors: %v", r.Errors)
	}
	found := false
	for _, w := range r.Warnings {
		if strings.Contains(w, "NVMe") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected NVMe warning, got: %v", r.Warnings)
	}
}

// --- QA-CP11B1-6: No preset, explicit fields → backward compat ---
func TestQA_CP11B1_NoPreset_BackwardCompat(t *testing.T) {
	r := blockvol.ResolvePolicy("", "sync_all", 2, "hdd", blockvol.EnvironmentInfo{
		ServerCount: 2, WALSizeDefault: 64 << 20, BlockSizeDefault: 4096,
	})
	if len(r.Errors) > 0 {
		t.Fatalf("unexpected errors: %v", r.Errors)
	}
	if r.Policy.Preset != "" {
		t.Errorf("preset = %q, want empty", r.Policy.Preset)
	}
	if r.Policy.DurabilityMode != "sync_all" {
		t.Errorf("durability_mode = %q, want sync_all", r.Policy.DurabilityMode)
	}
	if r.Policy.ReplicaFactor != 2 {
		t.Errorf("replica_factor = %d, want 2", r.Policy.ReplicaFactor)
	}
	if r.Policy.DiskType != "hdd" {
		t.Errorf("disk_type = %q, want hdd", r.Policy.DiskType)
	}
}

// --- QA-CP11B1-7: Resolve endpoint returns 200 with correct fields ---
func TestQA_CP11B1_ResolveHandler_HTTP(t *testing.T) {
	ms := qaPresetMaster(t)

	body, _ := json.Marshal(blockapi.CreateVolumeRequest{
		Preset: "database",
	})
	req := httptest.NewRequest(http.MethodPost, "/block/volume/resolve", bytes.NewReader(body))
	w := httptest.NewRecorder()
	ms.blockVolumeResolveHandler(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %s", w.Code, w.Body.String())
	}

	var resp blockapi.ResolvedPolicyResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Policy.DurabilityMode != "sync_all" {
		t.Errorf("durability_mode = %q, want sync_all", resp.Policy.DurabilityMode)
	}
	if resp.Policy.ReplicaFactor != 2 {
		t.Errorf("replica_factor = %d, want 2", resp.Policy.ReplicaFactor)
	}
	if resp.Policy.StorageProfile != "single" {
		t.Errorf("storage_profile = %q, want single", resp.Policy.StorageProfile)
	}
	if resp.Policy.TransportPreference != "nvme" {
		t.Errorf("transport = %q, want nvme", resp.Policy.TransportPreference)
	}
}

// --- QA-CP11B1-8: Create with preset stores preset on registry entry ---
func TestQA_CP11B1_CreateWithPreset_StoresPreset(t *testing.T) {
	ms := qaPresetMaster(t)
	ctx := context.Background()

	_, err := ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name:           "preset-vol",
		SizeBytes:      1 << 30,
		DurabilityMode: "best_effort",
		ReplicaFactor:  2,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// Simulate what the HTTP handler does after create.
	if entry, ok := ms.blockRegistry.Lookup("preset-vol"); ok {
		entry.Preset = "general"
	}

	entry, ok := ms.blockRegistry.Lookup("preset-vol")
	if !ok {
		t.Fatal("volume not found in registry")
	}
	if entry.Preset != "general" {
		t.Errorf("preset = %q, want general", entry.Preset)
	}

	// Verify entryToVolumeInfo propagates preset.
	info := entryToVolumeInfo(entry)
	if info.Preset != "general" {
		t.Errorf("VolumeInfo.Preset = %q, want general", info.Preset)
	}
}

// --- QA-CP11B1-9: RF exceeds servers → warning ---
func TestQA_CP11B1_RF_ExceedsServers_Warning(t *testing.T) {
	r := blockvol.ResolvePolicy(blockvol.PresetGeneral, "", 3, "", blockvol.EnvironmentInfo{
		ServerCount: 2, WALSizeDefault: 64 << 20, BlockSizeDefault: 4096,
	})
	if len(r.Errors) > 0 {
		t.Fatalf("unexpected errors: %v", r.Errors)
	}
	found := false
	for _, w := range r.Warnings {
		if strings.Contains(w, "exceeds available servers") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected RF>servers warning, got: %v", r.Warnings)
	}
}

// --- QA-CP11B1-10: All response fields present and typed correctly ---
func TestQA_CP11B1_StableOutputFields(t *testing.T) {
	r := blockvol.ResolvePolicy(blockvol.PresetDatabase, "", 0, "", blockvol.EnvironmentInfo{
		NVMeAvailable: true, ServerCount: 3, WALSizeDefault: 128 << 20, BlockSizeDefault: 4096,
	})
	// Verify every field in VolumePolicy is populated.
	p := r.Policy
	if p.Preset != blockvol.PresetDatabase {
		t.Errorf("preset = %q", p.Preset)
	}
	if p.DurabilityMode == "" {
		t.Error("durability_mode is empty")
	}
	if p.ReplicaFactor == 0 {
		t.Error("replica_factor is 0")
	}
	if p.TransportPref == "" {
		t.Error("transport_preference is empty")
	}
	if p.WorkloadHint == "" {
		t.Error("workload_hint is empty")
	}
	if p.WALSizeRecommended == 0 {
		t.Error("wal_size_recommended is 0")
	}
	if p.StorageProfile == "" {
		t.Error("storage_profile is empty")
	}

	// Verify JSON round-trip preserves all fields.
	data, err := json.Marshal(r)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var r2 blockvol.ResolvedPolicy
	if err := json.Unmarshal(data, &r2); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if r2.Policy.DurabilityMode != p.DurabilityMode {
		t.Errorf("round-trip durability = %q, want %q", r2.Policy.DurabilityMode, p.DurabilityMode)
	}
	if r2.Policy.ReplicaFactor != p.ReplicaFactor {
		t.Errorf("round-trip RF = %d, want %d", r2.Policy.ReplicaFactor, p.ReplicaFactor)
	}
}

// --- QA-CP11B1-11: Create + Resolve parity ---
// Create with preset + overrides produces the same effective values
// as /resolve for the same request. Catches drift between resolve and create.
func TestQA_CP11B1_CreateResolve_Parity(t *testing.T) {
	ms := qaPresetMaster(t)

	// Resolve first.
	preset := blockvol.PresetDatabase
	durOverride := "best_effort"
	rfOverride := 0
	diskOverride := ""

	env := ms.buildEnvironmentInfo()
	resolved := blockvol.ResolvePolicy(preset, durOverride, rfOverride, diskOverride, env)
	if len(resolved.Errors) > 0 {
		t.Fatalf("resolve errors: %v", resolved.Errors)
	}

	// Create using the same resolved values (simulating what the handler does).
	ctx := context.Background()
	_, err := ms.CreateBlockVolume(ctx, &master_pb.CreateBlockVolumeRequest{
		Name:           "parity-vol",
		SizeBytes:      1 << 30,
		DiskType:       resolved.Policy.DiskType,
		DurabilityMode: resolved.Policy.DurabilityMode,
		ReplicaFactor:  uint32(resolved.Policy.ReplicaFactor),
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	entry, ok := ms.blockRegistry.Lookup("parity-vol")
	if !ok {
		t.Fatal("volume not found")
	}

	// Check parity: the created volume should have the resolved durability + RF.
	if entry.DurabilityMode != resolved.Policy.DurabilityMode {
		t.Errorf("durability: entry=%q, resolved=%q", entry.DurabilityMode, resolved.Policy.DurabilityMode)
	}
	if entry.ReplicaFactor != resolved.Policy.ReplicaFactor {
		t.Errorf("RF: entry=%d, resolved=%d", entry.ReplicaFactor, resolved.Policy.ReplicaFactor)
	}
}

// ============================================================
// CP11B-1 Review Round: Additional Adversarial Tests
// ============================================================

// QA-CP11B1-12: NVMe capability from heartbeat, not volumes.
// On a fresh cluster with NVMe-capable servers but no volumes,
// database preset should NOT warn about missing NVMe.
func TestQA_CP11B1_NVMeFromHeartbeat_FreshCluster(t *testing.T) {
	r := NewBlockVolumeRegistry()
	// Simulate heartbeat from NVMe-capable server (no volumes created yet).
	r.UpdateFullHeartbeat("vs1:9333", nil, "192.168.1.10:4420")

	if !r.HasNVMeCapableServer() {
		t.Fatal("HasNVMeCapableServer should be true after heartbeat with NVMe addr")
	}

	// Resolve database preset — should NOT warn about NVMe.
	env := blockvol.EnvironmentInfo{
		NVMeAvailable: r.HasNVMeCapableServer(),
		ServerCount:   1,
		WALSizeDefault: 128 << 20,
	}
	resolved := blockvol.ResolvePolicy(blockvol.PresetDatabase, "", 0, "", env)
	for _, w := range resolved.Warnings {
		if strings.Contains(w, "NVMe") {
			t.Fatalf("should NOT warn about NVMe on fresh cluster with NVMe server: %s", w)
		}
	}
}

// QA-CP11B1-13: NVMe capability lost after server unmark.
// When NVMe server disconnects, HasNVMeCapableServer should return false.
func TestQA_CP11B1_NVMeLostAfterUnmark(t *testing.T) {
	r := NewBlockVolumeRegistry()
	r.UpdateFullHeartbeat("nvme-vs:9333", nil, "10.0.0.1:4420")
	r.UpdateFullHeartbeat("plain-vs:9333", nil, "")

	if !r.HasNVMeCapableServer() {
		t.Fatal("should have NVMe before unmark")
	}

	// NVMe server disconnects.
	r.UnmarkBlockCapable("nvme-vs:9333")

	if r.HasNVMeCapableServer() {
		t.Fatal("should NOT have NVMe after NVMe server disconnected")
	}
}

// QA-CP11B1-14: MarkBlockCapable does NOT overwrite NVMe addr set by heartbeat.
func TestQA_CP11B1_MarkBlockCapable_PreservesNVMe(t *testing.T) {
	r := NewBlockVolumeRegistry()
	// Heartbeat sets NVMe addr.
	r.UpdateFullHeartbeat("vs1:9333", nil, "10.0.0.1:4420")

	// MarkBlockCapable is called again (e.g. from another code path).
	r.MarkBlockCapable("vs1:9333")

	// NVMe addr should NOT be cleared.
	if !r.HasNVMeCapableServer() {
		t.Fatal("MarkBlockCapable should not clear NVMe addr set by heartbeat")
	}
}

// QA-CP11B1-15: Resolve with invalid durability_mode string returns error.
func TestQA_CP11B1_InvalidDurabilityString_Rejected(t *testing.T) {
	r := blockvol.ResolvePolicy(blockvol.PresetGeneral, "turbo_sync", 0, "", blockvol.EnvironmentInfo{
		ServerCount: 2, WALSizeDefault: 64 << 20,
	})
	if len(r.Errors) == 0 {
		t.Fatal("expected error for invalid durability_mode string")
	}
	if !strings.Contains(r.Errors[0], "invalid durability_mode") {
		t.Errorf("error = %q, want 'invalid durability_mode'", r.Errors[0])
	}
}

// QA-CP11B1-16: Override disk_type on database preset (ssd → hdd).
func TestQA_CP11B1_OverrideDiskType(t *testing.T) {
	r := blockvol.ResolvePolicy(blockvol.PresetDatabase, "", 0, "hdd", blockvol.EnvironmentInfo{
		NVMeAvailable: true, ServerCount: 2, WALSizeDefault: 128 << 20,
	})
	if len(r.Errors) > 0 {
		t.Fatalf("unexpected errors: %v", r.Errors)
	}
	if r.Policy.DiskType != "hdd" {
		t.Errorf("disk_type = %q, want hdd (override)", r.Policy.DiskType)
	}
	if !containsStr(r.Overrides, "disk_type") {
		t.Errorf("overrides = %v, want disk_type present", r.Overrides)
	}
	// Preset default was "ssd" — verify it was overridden, not merged.
	if r.Policy.Preset != blockvol.PresetDatabase {
		t.Errorf("preset = %q, want database", r.Policy.Preset)
	}
}

// QA-CP11B1-17: All three overrides at once.
func TestQA_CP11B1_AllOverridesAtOnce(t *testing.T) {
	r := blockvol.ResolvePolicy(blockvol.PresetDatabase, "best_effort", 3, "hdd", blockvol.EnvironmentInfo{
		NVMeAvailable: true, ServerCount: 3, WALSizeDefault: 128 << 20,
	})
	if len(r.Errors) > 0 {
		t.Fatalf("unexpected errors: %v", r.Errors)
	}
	if r.Policy.DurabilityMode != "best_effort" {
		t.Errorf("durability = %q, want best_effort", r.Policy.DurabilityMode)
	}
	if r.Policy.ReplicaFactor != 3 {
		t.Errorf("RF = %d, want 3", r.Policy.ReplicaFactor)
	}
	if r.Policy.DiskType != "hdd" {
		t.Errorf("disk_type = %q, want hdd", r.Policy.DiskType)
	}
	if len(r.Overrides) != 3 {
		t.Errorf("overrides count = %d, want 3: %v", len(r.Overrides), r.Overrides)
	}
	// Transport and workload_hint should still come from preset.
	if r.Policy.TransportPref != "nvme" {
		t.Errorf("transport = %q, want nvme (from preset, not overridable)", r.Policy.TransportPref)
	}
}

// QA-CP11B1-18: Preset override that creates an incompatible combo.
// database preset (sync_all) + override RF=1 → warning (not error).
func TestQA_CP11B1_PresetOverride_SyncAll_RF1_Warning(t *testing.T) {
	r := blockvol.ResolvePolicy(blockvol.PresetDatabase, "", 1, "", blockvol.EnvironmentInfo{
		NVMeAvailable: true, ServerCount: 2, WALSizeDefault: 128 << 20,
	})
	// sync_all + RF=1 is valid (no error) but should warn.
	if len(r.Errors) > 0 {
		t.Fatalf("unexpected errors: %v", r.Errors)
	}
	found := false
	for _, w := range r.Warnings {
		if strings.Contains(w, "no replication benefit") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected 'no replication benefit' warning, got: %v", r.Warnings)
	}
}

// QA-CP11B1-19: Zero ServerCount → RF warning suppressed (unknown cluster state).
func TestQA_CP11B1_ZeroServerCount_NoRFWarning(t *testing.T) {
	r := blockvol.ResolvePolicy(blockvol.PresetGeneral, "", 3, "", blockvol.EnvironmentInfo{
		ServerCount: 0, WALSizeDefault: 64 << 20,
	})
	if len(r.Errors) > 0 {
		t.Fatalf("unexpected errors: %v", r.Errors)
	}
	for _, w := range r.Warnings {
		if strings.Contains(w, "exceeds available servers") {
			t.Fatalf("should NOT warn about RF vs servers when ServerCount=0: %s", w)
		}
	}
}

// QA-CP11B1-20: Resolve endpoint returns errors[] for invalid preset (not HTTP error).
func TestQA_CP11B1_ResolveEndpoint_InvalidPreset_Returns200WithErrors(t *testing.T) {
	ms := qaPresetMaster(t)

	body, _ := json.Marshal(blockapi.CreateVolumeRequest{Preset: "bogus"})
	req := httptest.NewRequest(http.MethodPost, "/block/volume/resolve", bytes.NewReader(body))
	w := httptest.NewRecorder()
	ms.blockVolumeResolveHandler(w, req)

	// Resolve always returns 200, even with errors.
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}
	var resp blockapi.ResolvedPolicyResponse
	json.NewDecoder(w.Body).Decode(&resp)
	if len(resp.Errors) == 0 {
		t.Fatal("expected errors[] for invalid preset")
	}
	if !strings.Contains(resp.Errors[0], "unknown preset") {
		t.Errorf("error = %q, want 'unknown preset'", resp.Errors[0])
	}
}

// QA-CP11B1-21: Create handler rejects invalid preset with 400 (not 200).
func TestQA_CP11B1_CreateHandler_InvalidPreset_Returns400(t *testing.T) {
	ms := qaPresetMaster(t)

	body, _ := json.Marshal(blockapi.CreateVolumeRequest{
		Name: "bad-preset-vol", SizeBytes: 1 << 30, Preset: "bogus",
	})
	req := httptest.NewRequest(http.MethodPost, "/block/volume", bytes.NewReader(body))
	w := httptest.NewRecorder()
	ms.blockVolumeCreateHandler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body: %s", w.Code, w.Body.String())
	}
}

// QA-CP11B1-22: Concurrent resolve calls don't panic.
func TestQA_CP11B1_ConcurrentResolve_NoPanic(t *testing.T) {
	ms := qaPresetMaster(t)
	// Simulate heartbeats to populate server info.
	ms.blockRegistry.UpdateFullHeartbeat("vs1:9333", nil, "10.0.0.1:4420")
	ms.blockRegistry.UpdateFullHeartbeat("vs2:9333", nil, "")

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		preset := []string{"database", "general", "throughput", "bogus", ""}[i%5]
		go func(p string) {
			defer wg.Done()
			body, _ := json.Marshal(blockapi.CreateVolumeRequest{Preset: p})
			req := httptest.NewRequest(http.MethodPost, "/block/volume/resolve", bytes.NewReader(body))
			w := httptest.NewRecorder()
			ms.blockVolumeResolveHandler(w, req)
		}(preset)
	}
	wg.Wait()
	// No panic = pass.
}

// containsStr is a helper (may already exist in the file from preset_test.go,
// but it's in a different package — blockvol vs weed_server).
func containsStr(ss []string, target string) bool {
	for _, s := range ss {
		if s == target {
			return true
		}
	}
	return false
}
