package weed_server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/blockapi"
)

// --- evaluateBlockPlacement unit tests ---

func TestEvaluateBlockPlacement_SingleCandidate_RF1(t *testing.T) {
	candidates := []PlacementCandidateInfo{
		{Address: "vs1:9333", VolumeCount: 0},
	}
	result := evaluateBlockPlacement(candidates, 1, "", 0, blockvol.DurabilityBestEffort)
	if result.Primary != "vs1:9333" {
		t.Fatalf("expected primary vs1:9333, got %q", result.Primary)
	}
	if len(result.Replicas) != 0 {
		t.Fatalf("expected 0 replicas, got %d", len(result.Replicas))
	}
	if len(result.Rejections) != 0 {
		t.Fatalf("expected 0 rejections, got %d", len(result.Rejections))
	}
	if len(result.Candidates) != 1 || result.Candidates[0] != "vs1:9333" {
		t.Fatalf("expected candidates [vs1:9333], got %v", result.Candidates)
	}
}

func TestEvaluateBlockPlacement_LeastLoaded(t *testing.T) {
	candidates := []PlacementCandidateInfo{
		{Address: "vs1:9333", VolumeCount: 5},
		{Address: "vs2:9333", VolumeCount: 2},
		{Address: "vs3:9333", VolumeCount: 8},
	}
	result := evaluateBlockPlacement(candidates, 1, "", 0, blockvol.DurabilityBestEffort)
	if result.Primary != "vs2:9333" {
		t.Fatalf("expected least-loaded vs2:9333 as primary, got %q", result.Primary)
	}
	// Candidates should be sorted: vs2 (2), vs1 (5), vs3 (8)
	expected := []string{"vs2:9333", "vs1:9333", "vs3:9333"}
	if len(result.Candidates) != 3 {
		t.Fatalf("expected 3 candidates, got %d", len(result.Candidates))
	}
	for i, e := range expected {
		if result.Candidates[i] != e {
			t.Fatalf("candidates[%d]: expected %q, got %q", i, e, result.Candidates[i])
		}
	}
}

func TestEvaluateBlockPlacement_DeterministicTiebreak(t *testing.T) {
	candidates := []PlacementCandidateInfo{
		{Address: "vs3:9333", VolumeCount: 0},
		{Address: "vs1:9333", VolumeCount: 0},
		{Address: "vs2:9333", VolumeCount: 0},
	}
	result := evaluateBlockPlacement(candidates, 1, "", 0, blockvol.DurabilityBestEffort)
	// All same count — address tiebreaker: vs1, vs2, vs3
	if result.Primary != "vs1:9333" {
		t.Fatalf("expected vs1:9333 (lowest address), got %q", result.Primary)
	}
	expected := []string{"vs1:9333", "vs2:9333", "vs3:9333"}
	for i, e := range expected {
		if result.Candidates[i] != e {
			t.Fatalf("candidates[%d]: expected %q, got %q", i, e, result.Candidates[i])
		}
	}
}

func TestEvaluateBlockPlacement_RF2_PrimaryAndReplica(t *testing.T) {
	candidates := []PlacementCandidateInfo{
		{Address: "vs1:9333", VolumeCount: 3},
		{Address: "vs2:9333", VolumeCount: 1},
		{Address: "vs3:9333", VolumeCount: 5},
	}
	result := evaluateBlockPlacement(candidates, 2, "", 0, blockvol.DurabilityBestEffort)
	if result.Primary != "vs2:9333" {
		t.Fatalf("expected primary vs2:9333, got %q", result.Primary)
	}
	if len(result.Replicas) != 1 || result.Replicas[0] != "vs1:9333" {
		t.Fatalf("expected replicas [vs1:9333], got %v", result.Replicas)
	}
	if len(result.Errors) != 0 {
		t.Fatalf("unexpected errors: %v", result.Errors)
	}
}

func TestEvaluateBlockPlacement_RF3_AllSelected(t *testing.T) {
	candidates := []PlacementCandidateInfo{
		{Address: "vs1:9333", VolumeCount: 0},
		{Address: "vs2:9333", VolumeCount: 0},
		{Address: "vs3:9333", VolumeCount: 0},
	}
	result := evaluateBlockPlacement(candidates, 3, "", 0, blockvol.DurabilityBestEffort)
	if result.Primary != "vs1:9333" {
		t.Fatalf("expected primary vs1:9333, got %q", result.Primary)
	}
	if len(result.Replicas) != 2 {
		t.Fatalf("expected 2 replicas, got %d", len(result.Replicas))
	}
	if len(result.Errors) != 0 {
		t.Fatalf("unexpected errors: %v", result.Errors)
	}
}

func TestEvaluateBlockPlacement_RF_ExceedsServers(t *testing.T) {
	candidates := []PlacementCandidateInfo{
		{Address: "vs1:9333", VolumeCount: 0},
		{Address: "vs2:9333", VolumeCount: 0},
	}
	result := evaluateBlockPlacement(candidates, 3, "", 0, blockvol.DurabilityBestEffort)
	// Primary should be selected, but only 1 replica possible out of 2 needed
	if result.Primary != "vs1:9333" {
		t.Fatalf("expected primary vs1:9333, got %q", result.Primary)
	}
	if len(result.Replicas) != 1 {
		t.Fatalf("expected 1 partial replica, got %d", len(result.Replicas))
	}
	// Should have rf_not_satisfiable warning
	found := false
	for _, w := range result.Warnings {
		if w == ReasonRFNotSatisfiable {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected warning %q, got %v", ReasonRFNotSatisfiable, result.Warnings)
	}
}

// TestEvaluateBlockPlacement_SingleServer_BestEffort_RF2 verifies that with one server,
// RF=2, best_effort: plan warns (not errors), matching create behavior which succeeds as single-copy.
func TestEvaluateBlockPlacement_SingleServer_BestEffort_RF2(t *testing.T) {
	candidates := []PlacementCandidateInfo{
		{Address: "vs1:9333", VolumeCount: 0},
	}
	result := evaluateBlockPlacement(candidates, 2, "", 0, blockvol.DurabilityBestEffort)
	if result.Primary != "vs1:9333" {
		t.Fatalf("expected primary vs1:9333, got %q", result.Primary)
	}
	// best_effort: zero replicas available should be a warning, not error
	if len(result.Errors) != 0 {
		t.Fatalf("best_effort should not produce errors, got %v", result.Errors)
	}
	found := false
	for _, w := range result.Warnings {
		if w == ReasonRFNotSatisfiable {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected warning %q, got %v", ReasonRFNotSatisfiable, result.Warnings)
	}
}

// TestEvaluateBlockPlacement_SingleServer_SyncAll_RF2 verifies that with one server,
// RF=2, sync_all: plan errors because sync_all requires replicas.
func TestEvaluateBlockPlacement_SingleServer_SyncAll_RF2(t *testing.T) {
	candidates := []PlacementCandidateInfo{
		{Address: "vs1:9333", VolumeCount: 0},
	}
	result := evaluateBlockPlacement(candidates, 2, "", 0, blockvol.DurabilitySyncAll)
	if result.Primary != "vs1:9333" {
		t.Fatalf("expected primary vs1:9333, got %q", result.Primary)
	}
	// sync_all: zero replicas available should be an error
	found := false
	for _, e := range result.Errors {
		if e == ReasonRFNotSatisfiable {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected error %q for sync_all, got errors=%v warnings=%v",
			ReasonRFNotSatisfiable, result.Errors, result.Warnings)
	}
}

func TestEvaluateBlockPlacement_NoServers(t *testing.T) {
	result := evaluateBlockPlacement(nil, 1, "", 0, blockvol.DurabilityBestEffort)
	if result.Primary != "" {
		t.Fatalf("expected empty primary, got %q", result.Primary)
	}
	found := false
	for _, e := range result.Errors {
		if e == ReasonNoViablePrimary {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected error %q, got %v", ReasonNoViablePrimary, result.Errors)
	}
	if len(result.Candidates) != 0 {
		t.Fatalf("expected empty candidates, got %v", result.Candidates)
	}
}

func TestEvaluateBlockPlacement_DiskTypeMismatch(t *testing.T) {
	candidates := []PlacementCandidateInfo{
		{Address: "vs1:9333", VolumeCount: 0, DiskType: "ssd"},
		{Address: "vs2:9333", VolumeCount: 0, DiskType: "hdd"},
		{Address: "vs3:9333", VolumeCount: 0, DiskType: "ssd"},
	}
	result := evaluateBlockPlacement(candidates, 1, "ssd", 0, blockvol.DurabilityBestEffort)
	if result.Primary != "vs1:9333" {
		t.Fatalf("expected primary vs1:9333, got %q", result.Primary)
	}
	// vs2 should be rejected
	if len(result.Rejections) != 1 || result.Rejections[0].Server != "vs2:9333" {
		t.Fatalf("expected vs2:9333 rejected, got %v", result.Rejections)
	}
	if result.Rejections[0].Reason != ReasonDiskTypeMismatch {
		t.Fatalf("expected reason %q, got %q", ReasonDiskTypeMismatch, result.Rejections[0].Reason)
	}
	if len(result.Candidates) != 2 {
		t.Fatalf("expected 2 eligible candidates, got %d", len(result.Candidates))
	}
}

func TestEvaluateBlockPlacement_InsufficientSpace(t *testing.T) {
	candidates := []PlacementCandidateInfo{
		{Address: "vs1:9333", VolumeCount: 0, AvailableBytes: 100 << 30}, // 100GB
		{Address: "vs2:9333", VolumeCount: 0, AvailableBytes: 5 << 30},   // 5GB
	}
	result := evaluateBlockPlacement(candidates, 1, "", 10<<30, blockvol.DurabilityBestEffort) // request 10GB
	if result.Primary != "vs1:9333" {
		t.Fatalf("expected primary vs1:9333, got %q", result.Primary)
	}
	if len(result.Rejections) != 1 || result.Rejections[0].Server != "vs2:9333" {
		t.Fatalf("expected vs2:9333 rejected, got %v", result.Rejections)
	}
	if result.Rejections[0].Reason != ReasonInsufficientSpace {
		t.Fatalf("expected reason %q, got %q", ReasonInsufficientSpace, result.Rejections[0].Reason)
	}
}

func TestEvaluateBlockPlacement_UnknownCapacity_Allowed(t *testing.T) {
	candidates := []PlacementCandidateInfo{
		{Address: "vs1:9333", VolumeCount: 0, AvailableBytes: 0}, // unknown
		{Address: "vs2:9333", VolumeCount: 0, AvailableBytes: 0}, // unknown
	}
	result := evaluateBlockPlacement(candidates, 1, "", 10<<30, blockvol.DurabilityBestEffort)
	if result.Primary != "vs1:9333" {
		t.Fatalf("expected primary vs1:9333, got %q", result.Primary)
	}
	if len(result.Rejections) != 0 {
		t.Fatalf("expected no rejections for unknown capacity, got %v", result.Rejections)
	}
}

// --- HTTP handler tests ---

func qaPlanMaster(t *testing.T) *MasterServer {
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
	ms.blockRegistry.MarkBlockCapable("vs1:9333")
	ms.blockRegistry.MarkBlockCapable("vs2:9333")
	ms.blockRegistry.MarkBlockCapable("vs3:9333")
	return ms
}

func TestBlockVolumePlanHandler_HappyPath(t *testing.T) {
	ms := qaPlanMaster(t)
	body := `{"name":"test-vol","size_bytes":1073741824,"replica_factor":2}`
	req := httptest.NewRequest(http.MethodPost, "/block/volume/plan", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	ms.blockVolumePlanHandler(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp blockapi.VolumePlanResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Plan.Primary == "" {
		t.Fatal("expected non-empty primary")
	}
	if len(resp.Plan.Candidates) == 0 {
		t.Fatal("expected non-empty candidates")
	}
	if resp.Plan.Candidates == nil {
		t.Fatal("candidates must never be nil")
	}
	if len(resp.Plan.Replicas) != 1 {
		t.Fatalf("expected 1 replica for RF=2, got %d", len(resp.Plan.Replicas))
	}
	if len(resp.Errors) != 0 {
		t.Fatalf("unexpected errors: %v", resp.Errors)
	}
}

func TestBlockVolumePlanHandler_WithPreset(t *testing.T) {
	ms := qaPlanMaster(t)
	body := `{"name":"db-vol","size_bytes":1073741824,"preset":"database"}`
	req := httptest.NewRequest(http.MethodPost, "/block/volume/plan", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	ms.blockVolumePlanHandler(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp blockapi.VolumePlanResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.ResolvedPolicy.Preset != "database" {
		t.Fatalf("expected preset database, got %q", resp.ResolvedPolicy.Preset)
	}
	if resp.ResolvedPolicy.DurabilityMode != "sync_all" {
		t.Fatalf("expected sync_all, got %q", resp.ResolvedPolicy.DurabilityMode)
	}
	if resp.Plan.Primary == "" {
		t.Fatal("expected non-empty primary")
	}
}

func TestBlockVolumePlanHandler_NoServers(t *testing.T) {
	ms := &MasterServer{
		blockRegistry:        NewBlockVolumeRegistry(),
		blockAssignmentQueue: NewBlockAssignmentQueue(),
	}
	body := `{"name":"test-vol","size_bytes":1073741824}`
	req := httptest.NewRequest(http.MethodPost, "/block/volume/plan", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	ms.blockVolumePlanHandler(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 even with errors, got %d", w.Code)
	}

	var resp blockapi.VolumePlanResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Errors) == 0 {
		t.Fatal("expected errors for no servers")
	}
	found := false
	for _, e := range resp.Errors {
		if e == ReasonNoViablePrimary {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected %q in errors, got %v", ReasonNoViablePrimary, resp.Errors)
	}
	if resp.Plan.Candidates == nil {
		t.Fatal("candidates must never be nil, even on error")
	}
}
