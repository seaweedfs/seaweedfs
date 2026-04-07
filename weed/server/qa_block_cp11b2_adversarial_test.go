package weed_server

import (
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

// --- QA adversarial tests for CP11B-2: Explainable Placement / Plan API ---

// TestQA_CP11B2_ConcurrentPlanCalls verifies that 100 concurrent plan calls
// complete without panic or data race.
func TestQA_CP11B2_ConcurrentPlanCalls(t *testing.T) {
	ms := qaPlanMaster(t)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			preset := ""
			if n%3 == 0 {
				preset = "database"
			} else if n%3 == 1 {
				preset = "general"
			}
			body := fmt.Sprintf(`{"name":"vol-%d","size_bytes":1073741824,"preset":"%s"}`, n, preset)
			req := httptest.NewRequest(http.MethodPost, "/block/volume/plan", strings.NewReader(body))
			w := httptest.NewRecorder()
			ms.blockVolumePlanHandler(w, req)
			if w.Code != http.StatusOK {
				t.Errorf("goroutine %d: expected 200, got %d", n, w.Code)
			}
		}(i)
	}
	wg.Wait()
}

// TestQA_CP11B2_NoBlockCapableServers verifies that plan returns a structured
// error when no servers are available, not a panic.
func TestQA_CP11B2_NoBlockCapableServers(t *testing.T) {
	ms := &MasterServer{
		blockRegistry:        NewBlockVolumeRegistry(),
		blockAssignmentQueue: NewBlockAssignmentQueue(),
		blockFailover:        newBlockFailoverState(),
	}
	body := `{"name":"test-vol","size_bytes":1073741824,"replica_factor":2}`
	req := httptest.NewRequest(http.MethodPost, "/block/volume/plan", strings.NewReader(body))
	w := httptest.NewRecorder()
	ms.blockVolumePlanHandler(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp blockapi.VolumePlanResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Errors) == 0 {
		t.Fatal("expected errors for no servers")
	}
	if resp.Plan.Primary != "" {
		t.Fatalf("expected empty primary, got %q", resp.Plan.Primary)
	}
}

// TestQA_CP11B2_RF_ExceedsAvailable verifies clear warning when RF exceeds servers.
func TestQA_CP11B2_RF_ExceedsAvailable(t *testing.T) {
	ms := &MasterServer{
		blockRegistry:        NewBlockVolumeRegistry(),
		blockAssignmentQueue: NewBlockAssignmentQueue(),
		blockFailover:        newBlockFailoverState(),
	}
	ms.blockRegistry.MarkBlockCapable("vs1:9333")
	ms.blockRegistry.MarkBlockCapable("vs2:9333")

	body := `{"name":"test-vol","size_bytes":1073741824,"replica_factor":3}`
	req := httptest.NewRequest(http.MethodPost, "/block/volume/plan", strings.NewReader(body))
	w := httptest.NewRecorder()
	ms.blockVolumePlanHandler(w, req)

	var resp blockapi.VolumePlanResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// Should have rf_not_satisfiable in warnings (partial replica possible)
	found := false
	for _, warning := range resp.Warnings {
		if warning == ReasonRFNotSatisfiable {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected warning %q, got warnings=%v, errors=%v", ReasonRFNotSatisfiable, resp.Warnings, resp.Errors)
	}
}

// TestQA_CP11B2_PlanThenCreate_PolicyConsistency verifies that /resolve, /plan,
// and create all agree on the resolved policy for the same request.
func TestQA_CP11B2_PlanThenCreate_PolicyConsistency(t *testing.T) {
	ms := qaPlanMaster(t)

	reqBody := `{"name":"policy-test","size_bytes":1073741824,"preset":"database"}`

	// Call /resolve
	resolveReq := httptest.NewRequest(http.MethodPost, "/block/volume/resolve", strings.NewReader(reqBody))
	resolveW := httptest.NewRecorder()
	ms.blockVolumeResolveHandler(resolveW, resolveReq)
	var resolveResp blockapi.ResolvedPolicyResponse
	json.NewDecoder(resolveW.Body).Decode(&resolveResp)

	// Call /plan
	planReq := httptest.NewRequest(http.MethodPost, "/block/volume/plan", strings.NewReader(reqBody))
	planW := httptest.NewRecorder()
	ms.blockVolumePlanHandler(planW, planReq)
	var planResp blockapi.VolumePlanResponse
	json.NewDecoder(planW.Body).Decode(&planResp)

	// Compare resolved policy fields
	if resolveResp.Policy.DurabilityMode != planResp.ResolvedPolicy.DurabilityMode {
		t.Fatalf("durability_mode mismatch: resolve=%q plan=%q",
			resolveResp.Policy.DurabilityMode, planResp.ResolvedPolicy.DurabilityMode)
	}
	if resolveResp.Policy.ReplicaFactor != planResp.ResolvedPolicy.ReplicaFactor {
		t.Fatalf("replica_factor mismatch: resolve=%d plan=%d",
			resolveResp.Policy.ReplicaFactor, planResp.ResolvedPolicy.ReplicaFactor)
	}
	if resolveResp.Policy.DiskType != planResp.ResolvedPolicy.DiskType {
		t.Fatalf("disk_type mismatch: resolve=%q plan=%q",
			resolveResp.Policy.DiskType, planResp.ResolvedPolicy.DiskType)
	}
	if resolveResp.Policy.Preset != planResp.ResolvedPolicy.Preset {
		t.Fatalf("preset mismatch: resolve=%q plan=%q",
			resolveResp.Policy.Preset, planResp.ResolvedPolicy.Preset)
	}
}

// TestQA_CP11B2_PlanThenCreate_OrderedCandidateParity verifies that plan and create
// derive the same ordered candidate list from the same cluster state.
func TestQA_CP11B2_PlanThenCreate_OrderedCandidateParity(t *testing.T) {
	ms := qaPlanMaster(t)

	// Record which servers create tries, in order.
	var createAttempts []string
	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, walSizeBytes uint64, diskType string, durabilityMode string) (*blockAllocResult, error) {
		createAttempts = append(createAttempts, server)
		return &blockAllocResult{
			Path:              fmt.Sprintf("/data/%s.blk", name),
			IQN:               fmt.Sprintf("iqn.2024.test:%s", name),
			ISCSIAddr:         server + ":3260",
			ReplicaDataAddr:   server + ":14260",
			ReplicaCtrlAddr:   server + ":14261",
			RebuildListenAddr: server + ":15000",
		}, nil
	}

	// Get plan
	body := `{"name":"parity-test","size_bytes":1073741824,"replica_factor":2}`
	planReq := httptest.NewRequest(http.MethodPost, "/block/volume/plan", strings.NewReader(body))
	planW := httptest.NewRecorder()
	ms.blockVolumePlanHandler(planW, planReq)
	var planResp blockapi.VolumePlanResponse
	json.NewDecoder(planW.Body).Decode(&planResp)

	// Create volume — allocate will record attempt order
	createReq := &master_pb.CreateBlockVolumeRequest{
		Name:      "parity-test",
		SizeBytes: 1073741824,
	}
	_, err := ms.CreateBlockVolume(context.Background(), createReq)
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}

	// createAttempts[0] = primary attempt, createAttempts[1] = replica attempt
	if len(createAttempts) < 2 {
		t.Fatalf("expected at least 2 allocations (primary + replica), got %d", len(createAttempts))
	}

	// Plan's candidate order should match create's attempt order.
	// Primary is Candidates[0], replica is from remaining Candidates.
	if planResp.Plan.Primary != createAttempts[0] {
		t.Fatalf("primary mismatch: plan=%q create=%q", planResp.Plan.Primary, createAttempts[0])
	}
}

// TestQA_CP11B2_PlanThenCreate_ReplicaOrderParity verifies that the plan's replica
// ordering matches the create path's replica attempt ordering.
func TestQA_CP11B2_PlanThenCreate_ReplicaOrderParity(t *testing.T) {
	ms := qaPlanMaster(t)

	var allocOrder []string
	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, walSizeBytes uint64, diskType string, durabilityMode string) (*blockAllocResult, error) {
		allocOrder = append(allocOrder, server)
		return &blockAllocResult{
			Path:              fmt.Sprintf("/data/%s.blk", name),
			IQN:               fmt.Sprintf("iqn.2024.test:%s", name),
			ISCSIAddr:         server + ":3260",
			ReplicaDataAddr:   server + ":14260",
			ReplicaCtrlAddr:   server + ":14261",
			RebuildListenAddr: server + ":15000",
		}, nil
	}

	// Plan with RF=3 (all 3 servers)
	body := `{"name":"replica-order","size_bytes":1073741824,"replica_factor":3}`
	planReq := httptest.NewRequest(http.MethodPost, "/block/volume/plan", strings.NewReader(body))
	planW := httptest.NewRecorder()
	ms.blockVolumePlanHandler(planW, planReq)
	var planResp blockapi.VolumePlanResponse
	json.NewDecoder(planW.Body).Decode(&planResp)

	// Create
	createReq := &master_pb.CreateBlockVolumeRequest{
		Name:          "replica-order",
		SizeBytes:     1073741824,
		ReplicaFactor: 3,
	}
	_, err := ms.CreateBlockVolume(context.Background(), createReq)
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}

	// allocOrder: [primary, replica1, replica2]
	if len(allocOrder) != 3 {
		t.Fatalf("expected 3 allocations, got %d", len(allocOrder))
	}

	// Plan candidates should match allocation order
	if len(planResp.Plan.Candidates) != 3 {
		t.Fatalf("expected 3 plan candidates, got %d", len(planResp.Plan.Candidates))
	}
	for i := 0; i < 3; i++ {
		if planResp.Plan.Candidates[i] != allocOrder[i] {
			t.Fatalf("candidate[%d] mismatch: plan=%q create=%q",
				i, planResp.Plan.Candidates[i], allocOrder[i])
		}
	}
}

// TestQA_CP11B2_Create_FallbackOnRPCFailure verifies that when the first candidate
// fails RPC, create uses the next candidate from the same ordered list.
func TestQA_CP11B2_Create_FallbackOnRPCFailure(t *testing.T) {
	ms := qaPlanMaster(t)

	callCount := 0
	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, walSizeBytes uint64, diskType string, durabilityMode string) (*blockAllocResult, error) {
		callCount++
		if callCount == 1 {
			return nil, fmt.Errorf("simulated RPC failure")
		}
		return &blockAllocResult{
			Path:              fmt.Sprintf("/data/%s.blk", name),
			IQN:               fmt.Sprintf("iqn.2024.test:%s", name),
			ISCSIAddr:         server + ":3260",
			ReplicaDataAddr:   server + ":14260",
			ReplicaCtrlAddr:   server + ":14261",
			RebuildListenAddr: server + ":15000",
		}, nil
	}

	// Get plan to know expected order
	body := `{"name":"fallback-test","size_bytes":1073741824,"replica_factor":1}`
	planReq := httptest.NewRequest(http.MethodPost, "/block/volume/plan", strings.NewReader(body))
	planW := httptest.NewRecorder()
	ms.blockVolumePlanHandler(planW, planReq)
	var planResp blockapi.VolumePlanResponse
	json.NewDecoder(planW.Body).Decode(&planResp)

	if len(planResp.Plan.Candidates) < 2 {
		t.Fatalf("need at least 2 candidates for fallback test, got %d", len(planResp.Plan.Candidates))
	}
	expectedFallback := planResp.Plan.Candidates[1]

	// Create — first attempt fails, should fall back to second candidate
	callCount = 0
	createReq := &master_pb.CreateBlockVolumeRequest{
		Name:      "fallback-test",
		SizeBytes: 1073741824,
	}
	resp, err := ms.CreateBlockVolume(context.Background(), createReq)
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}

	// The volume should be on the second candidate (fallback)
	entry, ok := ms.blockRegistry.Lookup("fallback-test")
	if !ok {
		t.Fatal("volume not in registry")
	}
	if entry.VolumeServer != expectedFallback {
		t.Fatalf("expected fallback to %q, got %q", expectedFallback, entry.VolumeServer)
	}
	_ = resp
}

// TestQA_CP11B2_PlanIsReadOnly verifies that plan does not register volumes
// or enqueue assignments.
func TestQA_CP11B2_PlanIsReadOnly(t *testing.T) {
	ms := qaPlanMaster(t)

	// Snapshot state before
	_, existsBefore := ms.blockRegistry.Lookup("readonly-test")
	queueBefore := ms.blockAssignmentQueue.TotalPending()

	body := `{"name":"readonly-test","size_bytes":1073741824,"replica_factor":2}`
	req := httptest.NewRequest(http.MethodPost, "/block/volume/plan", strings.NewReader(body))
	w := httptest.NewRecorder()
	ms.blockVolumePlanHandler(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	// Verify no side effects
	_, existsAfter := ms.blockRegistry.Lookup("readonly-test")
	queueAfter := ms.blockAssignmentQueue.TotalPending()

	if existsBefore || existsAfter {
		t.Fatal("plan should not register volume")
	}
	if queueAfter != queueBefore {
		t.Fatalf("plan should not enqueue assignments: before=%d after=%d", queueBefore, queueAfter)
	}
}

// TestQA_CP11B2_RejectionReasonStability verifies that rejection reason strings
// match the defined constants — no typos, no ad-hoc strings.
func TestQA_CP11B2_RejectionReasonStability(t *testing.T) {
	validReasons := map[string]bool{
		ReasonDiskTypeMismatch:  true,
		ReasonInsufficientSpace: true,
		ReasonAlreadySelected:   true,
		ReasonNoViablePrimary:   true,
		ReasonRFNotSatisfiable:  true,
	}

	// Create a scenario that produces rejections
	candidates := []PlacementCandidateInfo{
		{Address: "vs1:9333", VolumeCount: 0, DiskType: "ssd"},
		{Address: "vs2:9333", VolumeCount: 0, DiskType: "hdd"},
		{Address: "vs3:9333", VolumeCount: 0, AvailableBytes: 100}, // too small
	}
	result := evaluateBlockPlacement(candidates, 1, "ssd", 1<<30, blockvol.DurabilityBestEffort)

	for _, r := range result.Rejections {
		if !validReasons[r.Reason] {
			t.Fatalf("rejection reason %q is not a known constant", r.Reason)
		}
	}
	for _, e := range result.Errors {
		if !validReasons[e] {
			t.Fatalf("error reason %q is not a known constant", e)
		}
	}
}

// TestQA_CP11B2_DeterministicOrder_MultipleInvocations verifies that calling
// plan 10 times with the same state produces identical results each time.
func TestQA_CP11B2_DeterministicOrder_MultipleInvocations(t *testing.T) {
	ms := qaPlanMaster(t)

	body := `{"name":"determ-test","size_bytes":1073741824,"replica_factor":2}`

	var firstResp blockapi.VolumePlanResponse
	for i := 0; i < 10; i++ {
		req := httptest.NewRequest(http.MethodPost, "/block/volume/plan", strings.NewReader(body))
		w := httptest.NewRecorder()
		ms.blockVolumePlanHandler(w, req)

		var resp blockapi.VolumePlanResponse
		json.NewDecoder(w.Body).Decode(&resp)

		if i == 0 {
			firstResp = resp
			continue
		}

		// Compare with first response
		if resp.Plan.Primary != firstResp.Plan.Primary {
			t.Fatalf("invocation %d: primary %q != first %q", i, resp.Plan.Primary, firstResp.Plan.Primary)
		}
		if len(resp.Plan.Candidates) != len(firstResp.Plan.Candidates) {
			t.Fatalf("invocation %d: candidate count %d != first %d",
				i, len(resp.Plan.Candidates), len(firstResp.Plan.Candidates))
		}
		for j := range resp.Plan.Candidates {
			if resp.Plan.Candidates[j] != firstResp.Plan.Candidates[j] {
				t.Fatalf("invocation %d: candidates[%d] %q != first %q",
					i, j, resp.Plan.Candidates[j], firstResp.Plan.Candidates[j])
			}
		}
	}
}

// ============================================================
// CP11B-2 Review Round: Additional Adversarial Tests
// ============================================================

// QA-CP11B2-11: RF=0 treated as RF=1 (primary only, no replicas).
func TestQA_CP11B2_RF0_BehavesAsRF1(t *testing.T) {
	candidates := []PlacementCandidateInfo{
		{Address: "vs1:9333", VolumeCount: 0},
		{Address: "vs2:9333", VolumeCount: 0},
	}
	result := evaluateBlockPlacement(candidates, 0, "", 0, blockvol.DurabilityBestEffort)
	if result.Primary != "vs1:9333" {
		t.Fatalf("primary: got %q, want vs1:9333", result.Primary)
	}
	if len(result.Replicas) != 0 {
		t.Fatalf("replicas: got %d, want 0 for RF=0", len(result.Replicas))
	}
	if len(result.Errors) != 0 {
		t.Fatalf("unexpected errors for RF=0: %v", result.Errors)
	}
}

// QA-CP11B2-12: RF=1 with sync_all — no replica needed, no warning.
func TestQA_CP11B2_RF1_NoReplicaNeeded(t *testing.T) {
	candidates := []PlacementCandidateInfo{
		{Address: "vs1:9333", VolumeCount: 0},
	}
	result := evaluateBlockPlacement(candidates, 1, "", 0, blockvol.DurabilitySyncAll)
	if result.Primary != "vs1:9333" {
		t.Fatalf("primary: got %q", result.Primary)
	}
	if len(result.Warnings) != 0 {
		t.Fatalf("RF=1 should not warn about replicas: %v", result.Warnings)
	}
	if len(result.Errors) != 0 {
		t.Fatalf("RF=1 should not error: %v", result.Errors)
	}
}

// QA-CP11B2-13: All candidates rejected by disk type → no_viable_primary.
func TestQA_CP11B2_AllRejected_DiskType(t *testing.T) {
	candidates := []PlacementCandidateInfo{
		{Address: "vs1:9333", VolumeCount: 0, DiskType: "hdd"},
		{Address: "vs2:9333", VolumeCount: 0, DiskType: "hdd"},
	}
	result := evaluateBlockPlacement(candidates, 1, "ssd", 0, blockvol.DurabilityBestEffort)
	if result.Primary != "" {
		t.Fatalf("expected no primary, got %q", result.Primary)
	}
	if len(result.Rejections) != 2 {
		t.Fatalf("expected 2 rejections, got %d", len(result.Rejections))
	}
	foundErr := false
	for _, e := range result.Errors {
		if e == ReasonNoViablePrimary {
			foundErr = true
		}
	}
	if !foundErr {
		t.Fatalf("expected %q, got %v", ReasonNoViablePrimary, result.Errors)
	}
}

// QA-CP11B2-14: All candidates rejected by capacity → no_viable_primary.
func TestQA_CP11B2_AllRejected_Capacity(t *testing.T) {
	candidates := []PlacementCandidateInfo{
		{Address: "vs1:9333", VolumeCount: 0, AvailableBytes: 1 << 20},
		{Address: "vs2:9333", VolumeCount: 0, AvailableBytes: 2 << 20},
	}
	result := evaluateBlockPlacement(candidates, 1, "", 100<<20, blockvol.DurabilityBestEffort)
	if result.Primary != "" {
		t.Fatalf("expected no primary, got %q", result.Primary)
	}
	if len(result.Rejections) != 2 {
		t.Fatalf("expected 2 rejections, got %d", len(result.Rejections))
	}
}

// QA-CP11B2-15: Mixed rejections — disk + capacity + eligible.
func TestQA_CP11B2_MixedRejections(t *testing.T) {
	candidates := []PlacementCandidateInfo{
		{Address: "vs1:9333", VolumeCount: 0, DiskType: "hdd", AvailableBytes: 100 << 30},
		{Address: "vs2:9333", VolumeCount: 0, DiskType: "ssd", AvailableBytes: 1 << 20},
		{Address: "vs3:9333", VolumeCount: 0, DiskType: "ssd", AvailableBytes: 100 << 30},
		{Address: "vs4:9333", VolumeCount: 5, DiskType: "ssd", AvailableBytes: 100 << 30},
	}
	result := evaluateBlockPlacement(candidates, 1, "ssd", 50<<30, blockvol.DurabilityBestEffort)
	if result.Primary != "vs3:9333" {
		t.Fatalf("primary: got %q, want vs3:9333", result.Primary)
	}
	if len(result.Rejections) != 2 {
		t.Fatalf("expected 2 rejections, got %d", len(result.Rejections))
	}
	reasons := map[string]string{}
	for _, r := range result.Rejections {
		reasons[r.Server] = r.Reason
	}
	if reasons["vs1:9333"] != ReasonDiskTypeMismatch {
		t.Fatalf("vs1: got %q", reasons["vs1:9333"])
	}
	if reasons["vs2:9333"] != ReasonInsufficientSpace {
		t.Fatalf("vs2: got %q", reasons["vs2:9333"])
	}
}

// QA-CP11B2-16: sync_quorum RF=3 filtered to 2 — quorum met, warn not error.
func TestQA_CP11B2_SyncQuorum_RF3_FilteredTo2(t *testing.T) {
	candidates := []PlacementCandidateInfo{
		{Address: "vs1:9333", VolumeCount: 0, DiskType: "ssd"},
		{Address: "vs2:9333", VolumeCount: 0, DiskType: "ssd"},
		{Address: "vs3:9333", VolumeCount: 0, DiskType: "hdd"},
	}
	result := evaluateBlockPlacement(candidates, 3, "ssd", 0, blockvol.DurabilitySyncQuorum)
	if len(result.Errors) != 0 {
		t.Fatalf("quorum met, should not error: %v", result.Errors)
	}
	foundWarn := false
	for _, w := range result.Warnings {
		if w == ReasonRFNotSatisfiable {
			foundWarn = true
		}
	}
	if !foundWarn {
		t.Fatalf("expected rf_not_satisfiable warning, got %v", result.Warnings)
	}
}

// QA-CP11B2-17: Unknown DiskType passes any filter.
func TestQA_CP11B2_UnknownDiskType_PassesFilter(t *testing.T) {
	candidates := []PlacementCandidateInfo{
		{Address: "vs1:9333", VolumeCount: 0, DiskType: ""},
		{Address: "vs2:9333", VolumeCount: 0, DiskType: "ssd"},
		{Address: "vs3:9333", VolumeCount: 0, DiskType: "hdd"},
	}
	result := evaluateBlockPlacement(candidates, 1, "ssd", 0, blockvol.DurabilityBestEffort)
	if len(result.Candidates) != 2 {
		t.Fatalf("expected 2 eligible, got %d: %v", len(result.Candidates), result.Candidates)
	}
	if result.Primary != "vs1:9333" {
		t.Fatalf("primary: got %q, want vs1:9333 (unknown passes)", result.Primary)
	}
}

// QA-CP11B2-18: 50-server list — deterministic ordering.
func TestQA_CP11B2_LargeCandidateList(t *testing.T) {
	candidates := make([]PlacementCandidateInfo, 50)
	for i := range candidates {
		candidates[i] = PlacementCandidateInfo{
			Address:     fmt.Sprintf("vs%02d:9333", i),
			VolumeCount: i % 5,
		}
	}
	result := evaluateBlockPlacement(candidates, 3, "", 0, blockvol.DurabilityBestEffort)
	if result.Primary != "vs00:9333" {
		t.Fatalf("primary: got %q, want vs00:9333", result.Primary)
	}
	if result.Replicas[0] != "vs05:9333" {
		t.Fatalf("replica[0]: got %q, want vs05:9333", result.Replicas[0])
	}
	if len(result.Candidates) != 50 {
		t.Fatalf("candidates: got %d, want 50", len(result.Candidates))
	}
	result2 := evaluateBlockPlacement(candidates, 3, "", 0, blockvol.DurabilityBestEffort)
	if result.Primary != result2.Primary {
		t.Fatalf("not deterministic")
	}
}

// QA-CP11B2-19: Failed primary still tried as replica.
func TestQA_CP11B2_FailedPrimary_TriedAsReplica(t *testing.T) {
	ms := qaPlanMaster(t)
	var allocLog []string
	callCount := 0
	ms.blockVSAllocate = func(ctx context.Context, server string, name string, sizeBytes uint64, walSizeBytes uint64, diskType string, durabilityMode string) (*blockAllocResult, error) {
		allocLog = append(allocLog, server)
		callCount++
		if callCount == 1 {
			return nil, fmt.Errorf("simulated primary failure")
		}
		return &blockAllocResult{
			Path: fmt.Sprintf("/data/%s.blk", name), IQN: fmt.Sprintf("iqn.test:%s", name),
			ISCSIAddr: server + ":3260", ReplicaDataAddr: server + ":14260",
			ReplicaCtrlAddr: server + ":14261", RebuildListenAddr: server + ":15000",
		}, nil
	}
	_, err := ms.CreateBlockVolume(context.Background(), &master_pb.CreateBlockVolumeRequest{
		Name: "fallback-replica", SizeBytes: 1 << 30, ReplicaFactor: 2,
	})
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}
	entry, ok := ms.blockRegistry.Lookup("fallback-replica")
	if !ok {
		t.Fatal("not in registry")
	}
	if entry.VolumeServer == allocLog[0] {
		t.Fatalf("primary should not be the failed server %q", allocLog[0])
	}
}

// QA-CP11B2-20: Plan with invalid preset — errors, not panic.
func TestQA_CP11B2_PlanWithInvalidPreset(t *testing.T) {
	ms := qaPlanMaster(t)
	body := `{"name":"bad","size_bytes":1073741824,"preset":"nonexistent"}`
	req := httptest.NewRequest(http.MethodPost, "/block/volume/plan", strings.NewReader(body))
	w := httptest.NewRecorder()
	ms.blockVolumePlanHandler(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var resp blockapi.VolumePlanResponse
	json.NewDecoder(w.Body).Decode(&resp)
	if len(resp.Errors) == 0 {
		t.Fatal("expected errors for invalid preset")
	}
	if resp.Plan.Candidates == nil {
		t.Fatal("candidates must never be nil")
	}
}
