package weed_server

import (
	"sort"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/blockapi"
)

// Stable placement rejection reason constants.
// These are product-grade strings suitable for UI/automation consumption.
const (
	ReasonDiskTypeMismatch  = "disk_type_mismatch"
	ReasonInsufficientSpace = "insufficient_space"
	ReasonAlreadySelected   = "already_selected"
)

// Plan-level error reasons (not per-server).
const (
	ReasonNoViablePrimary  = "no_viable_primary"
	ReasonRFNotSatisfiable = "rf_not_satisfiable"
)

// PlacementRejection records one server rejection with stable reason.
type PlacementRejection struct {
	Server string
	Reason string
}

// PlacementResult is the full output of the placement planner.
// Candidates is the ordered eligible list — Primary is Candidates[0],
// Replicas is Candidates[1:RF]. All derived from the same sequence.
type PlacementResult struct {
	Candidates []string // full ordered eligible list
	Primary    string
	Replicas   []string
	Rejections []PlacementRejection
	Warnings   []string
	Errors     []string
}

// evaluateBlockPlacement takes candidates and request parameters,
// applies filters, scores deterministically, and returns a placement plan.
// Pure function: no side effects, no registry/topology dependency.
func evaluateBlockPlacement(
	candidates []PlacementCandidateInfo,
	replicaFactor int,
	diskType string,
	sizeBytes uint64,
	durabilityMode blockvol.DurabilityMode,
) PlacementResult {
	var result PlacementResult

	// Filter phase: reject ineligible candidates.
	type eligible struct {
		address     string
		volumeCount int
	}
	var kept []eligible

	for _, c := range candidates {
		// Disk type filter: skip when either side is empty (unknown/any).
		if diskType != "" && c.DiskType != "" && c.DiskType != diskType {
			result.Rejections = append(result.Rejections, PlacementRejection{
				Server: c.Address,
				Reason: ReasonDiskTypeMismatch,
			})
			continue
		}
		// Capacity filter: skip when AvailableBytes is 0 (unknown).
		if sizeBytes > 0 && c.AvailableBytes > 0 && c.AvailableBytes < sizeBytes {
			result.Rejections = append(result.Rejections, PlacementRejection{
				Server: c.Address,
				Reason: ReasonInsufficientSpace,
			})
			continue
		}
		kept = append(kept, eligible{address: c.Address, volumeCount: c.VolumeCount})
	}

	// Sort phase: volume count ascending, then address ascending (deterministic).
	sort.Slice(kept, func(i, j int) bool {
		if kept[i].volumeCount != kept[j].volumeCount {
			return kept[i].volumeCount < kept[j].volumeCount
		}
		return kept[i].address < kept[j].address
	})

	// Build ordered candidate list.
	result.Candidates = make([]string, len(kept))
	for i, k := range kept {
		result.Candidates[i] = k.address
	}

	// Select phase.
	if len(result.Candidates) == 0 {
		result.Errors = append(result.Errors, ReasonNoViablePrimary)
		return result
	}

	result.Primary = result.Candidates[0]

	// Replicas: RF means total copies including primary.
	replicasNeeded := replicaFactor - 1
	if replicasNeeded > 0 {
		available := len(result.Candidates) - 1 // exclude primary
		if available >= replicasNeeded {
			result.Replicas = result.Candidates[1 : 1+replicasNeeded]
		} else {
			// Partial or zero replicas available.
			if available > 0 {
				result.Replicas = result.Candidates[1:]
			}
			// Severity depends on durability mode: strict modes error, best_effort warns.
			requiredReplicas := durabilityMode.RequiredReplicas(replicaFactor)
			if available < requiredReplicas {
				result.Errors = append(result.Errors, ReasonRFNotSatisfiable)
			} else {
				result.Warnings = append(result.Warnings, ReasonRFNotSatisfiable)
			}
		}
	}

	return result
}

// gatherPlacementCandidates reads candidate data from the block registry.
// This is the topology bridge point: today it reads from the registry,
// long-term it would read from weed/topology.
func (ms *MasterServer) gatherPlacementCandidates() []PlacementCandidateInfo {
	return ms.blockRegistry.PlacementCandidates()
}

// PlanBlockVolume is the top-level planning function.
// Resolves policy, gathers candidates, evaluates placement, builds response.
func (ms *MasterServer) PlanBlockVolume(req *blockapi.CreateVolumeRequest) *blockapi.VolumePlanResponse {
	env := ms.buildEnvironmentInfo()
	resolved := blockvol.ResolvePolicy(blockvol.PresetName(req.Preset),
		req.DurabilityMode, req.ReplicaFactor, req.DiskType, env)

	resp := &blockapi.VolumePlanResponse{
		ResolvedPolicy: blockapi.ResolvedPolicyView{
			Preset:              string(resolved.Policy.Preset),
			DurabilityMode:      resolved.Policy.DurabilityMode,
			ReplicaFactor:       resolved.Policy.ReplicaFactor,
			DiskType:            resolved.Policy.DiskType,
			TransportPreference: resolved.Policy.TransportPref,
			WorkloadHint:        resolved.Policy.WorkloadHint,
			WALSizeRecommended:  resolved.Policy.WALSizeRecommended,
			StorageProfile:      resolved.Policy.StorageProfile,
		},
		Warnings: resolved.Warnings,
		Errors:   resolved.Errors,
		Plan:     blockapi.VolumePlanView{Candidates: []string{}}, // never nil
	}

	// If resolve has errors, return without placement evaluation.
	if len(resolved.Errors) > 0 {
		return resp
	}

	durMode, _ := blockvol.ParseDurabilityMode(resolved.Policy.DurabilityMode)
	candidates := ms.gatherPlacementCandidates()
	placement := evaluateBlockPlacement(candidates, resolved.Policy.ReplicaFactor,
		resolved.Policy.DiskType, req.SizeBytes, durMode)

	resp.Plan = blockapi.VolumePlanView{
		Primary:    placement.Primary,
		Replicas:   placement.Replicas,
		Candidates: placement.Candidates,
	}
	// Ensure Candidates is never nil (stable response shape).
	if resp.Plan.Candidates == nil {
		resp.Plan.Candidates = []string{}
	}

	// Convert internal rejections to API type.
	for _, r := range placement.Rejections {
		resp.Plan.Rejections = append(resp.Plan.Rejections, blockapi.VolumePlanRejection{
			Server: r.Server,
			Reason: r.Reason,
		})
	}

	// Merge placement warnings and errors.
	resp.Warnings = append(resp.Warnings, placement.Warnings...)
	resp.Errors = append(resp.Errors, placement.Errors...)

	return resp
}
