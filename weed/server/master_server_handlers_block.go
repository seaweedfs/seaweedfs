package weed_server

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gorilla/mux"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/blockapi"
)

// buildEnvironmentInfo constructs a blockvol.EnvironmentInfo from registry state.
func (ms *MasterServer) buildEnvironmentInfo() blockvol.EnvironmentInfo {
	return blockvol.EnvironmentInfo{
		NVMeAvailable:    ms.blockRegistry.HasNVMeCapableServer(),
		ServerCount:      len(ms.blockRegistry.BlockCapableServers()),
		WALSizeDefault:   64 << 20, // engine default
		BlockSizeDefault: 4096,     // engine default
	}
}

// blockVolumeCreateHandler handles POST /block/volume.
func (ms *MasterServer) blockVolumeCreateHandler(w http.ResponseWriter, r *http.Request) {
	var req blockapi.CreateVolumeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("invalid request body: %w", err))
		return
	}

	// Store replica_placement in registry after creation.
	replicaPlacement := req.ReplicaPlacement
	if replicaPlacement == "" {
		replicaPlacement = "000"
	}

	// Resolve preset + overrides.
	env := ms.buildEnvironmentInfo()
	resolved := blockvol.ResolvePolicy(blockvol.PresetName(req.Preset),
		req.DurabilityMode, req.ReplicaFactor, req.DiskType, env)
	if len(resolved.Errors) > 0 {
		writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("%s", resolved.Errors[0]))
		return
	}

	// Use resolved values for the gRPC call.
	resp, err := ms.CreateBlockVolume(r.Context(), &master_pb.CreateBlockVolumeRequest{
		Name:           req.Name,
		SizeBytes:      req.SizeBytes,
		DiskType:       resolved.Policy.DiskType,
		DurabilityMode: resolved.Policy.DurabilityMode,
		ReplicaFactor:  uint32(resolved.Policy.ReplicaFactor),
	})
	if err != nil {
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	}

	// Store replica_placement and preset on the registry entry (locked mutation).
	ms.blockRegistry.UpdateEntry(resp.VolumeId, func(e *BlockVolumeEntry) {
		e.ReplicaPlacement = replicaPlacement
		e.Preset = req.Preset
	})

	// Look up the full entry to populate all fields.
	info := blockapi.VolumeInfo{
		Name:             resp.VolumeId,
		VolumeServer:     resp.VolumeServer,
		SizeBytes:        resp.CapacityBytes,
		ReplicaPlacement: replicaPlacement,
		ISCSIAddr:        resp.IscsiAddr,
		IQN:              resp.Iqn,
	}
	if entry, ok := ms.blockRegistry.Lookup(resp.VolumeId); ok {
		info = entryToVolumeInfo(&entry, ms.blockRegistry.IsBlockCapable(entry.VolumeServer))
	}
	writeJsonQuiet(w, r, http.StatusOK, info)
}

// blockVolumeResolveHandler handles POST /block/volume/resolve.
// Diagnostic endpoint: always returns 200, even with errors[].
func (ms *MasterServer) blockVolumeResolveHandler(w http.ResponseWriter, r *http.Request) {
	var req blockapi.CreateVolumeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("invalid request body: %w", err))
		return
	}

	env := ms.buildEnvironmentInfo()
	resolved := blockvol.ResolvePolicy(blockvol.PresetName(req.Preset),
		req.DurabilityMode, req.ReplicaFactor, req.DiskType, env)

	resp := blockapi.ResolvedPolicyResponse{
		Policy: blockapi.ResolvedPolicyView{
			Preset:              string(resolved.Policy.Preset),
			DurabilityMode:      resolved.Policy.DurabilityMode,
			ReplicaFactor:       resolved.Policy.ReplicaFactor,
			DiskType:            resolved.Policy.DiskType,
			TransportPreference: resolved.Policy.TransportPref,
			WorkloadHint:        resolved.Policy.WorkloadHint,
			WALSizeRecommended:  resolved.Policy.WALSizeRecommended,
			StorageProfile:      resolved.Policy.StorageProfile,
		},
		Overrides: resolved.Overrides,
		Warnings:  resolved.Warnings,
		Errors:    resolved.Errors,
	}
	writeJsonQuiet(w, r, http.StatusOK, resp)
}

// blockVolumePlanHandler handles POST /block/volume/plan.
// Read-only: no cluster mutation. Proxied to leader for consistent placement state.
// Always returns 200 with errors[] in body for error conditions.
func (ms *MasterServer) blockVolumePlanHandler(w http.ResponseWriter, r *http.Request) {
	var req blockapi.CreateVolumeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("invalid request body: %w", err))
		return
	}
	resp := ms.PlanBlockVolume(&req)
	writeJsonQuiet(w, r, http.StatusOK, resp)
}

// blockVolumeDeleteHandler handles DELETE /block/volume/{name}.
func (ms *MasterServer) blockVolumeDeleteHandler(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]
	if name == "" {
		writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("name is required"))
		return
	}

	_, err := ms.DeleteBlockVolume(r.Context(), &master_pb.DeleteBlockVolumeRequest{
		Name: name,
	})
	if err != nil {
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	}
	writeJsonQuiet(w, r, http.StatusOK, map[string]string{"status": "deleted"})
}

// blockVolumeLookupHandler handles GET /block/volume/{name}.
func (ms *MasterServer) blockVolumeLookupHandler(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]
	if name == "" {
		writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("name is required"))
		return
	}

	entry, ok := ms.blockRegistry.Lookup(name)
	if !ok {
		writeJsonError(w, r, http.StatusNotFound, fmt.Errorf("block volume %q not found", name))
		return
	}
	writeJsonQuiet(w, r, http.StatusOK, entryToVolumeInfo(&entry, ms.blockRegistry.IsBlockCapable(entry.VolumeServer)))
}

// blockVolumeListHandler handles GET /block/volumes.
func (ms *MasterServer) blockVolumeListHandler(w http.ResponseWriter, r *http.Request) {
	entries := ms.blockRegistry.ListAll()
	infos := make([]blockapi.VolumeInfo, len(entries))
	for i := range entries {
		infos[i] = entryToVolumeInfo(&entries[i], ms.blockRegistry.IsBlockCapable(entries[i].VolumeServer))
	}
	writeJsonQuiet(w, r, http.StatusOK, infos)
}

// blockAssignHandler handles POST /block/assign.
func (ms *MasterServer) blockAssignHandler(w http.ResponseWriter, r *http.Request) {
	var req blockapi.AssignRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("invalid request body: %w", err))
		return
	}
	if req.Name == "" {
		writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("name is required"))
		return
	}

	// Resolve name → registry entry.
	entry, ok := ms.blockRegistry.Lookup(req.Name)
	if !ok {
		writeJsonError(w, r, http.StatusNotFound, fmt.Errorf("block volume %q not found", req.Name))
		return
	}

	// Determine target server + path based on role.
	server, path := entry.VolumeServer, entry.Path
	if req.Role == "replica" {
		if entry.ReplicaServer == "" {
			writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("block volume %q has no replica", req.Name))
			return
		}
		server, path = entry.ReplicaServer, entry.ReplicaPath
	}

	ms.blockAssignmentQueue.Enqueue(server, blockvol.BlockVolumeAssignment{
		Path:       path,
		Epoch:      req.Epoch,
		Role:       blockapi.RoleFromString(req.Role),
		LeaseTtlMs: uint32(req.LeaseTTLMs),
	})
	writeJsonQuiet(w, r, http.StatusOK, map[string]string{"status": "queued"})
}

// blockServersHandler handles GET /block/servers.
func (ms *MasterServer) blockServersHandler(w http.ResponseWriter, r *http.Request) {
	summaries := ms.blockRegistry.ServerSummaries()
	infos := make([]blockapi.ServerInfo, len(summaries))
	for i, s := range summaries {
		infos[i] = blockapi.ServerInfo{
			Address:      s.Address,
			VolumeCount:  s.VolumeCount,
			BlockCapable: s.BlockCapable,
		}
	}
	writeJsonQuiet(w, r, http.StatusOK, infos)
}

// blockVolumeExpandHandler handles POST /block/volume/{name}/expand.
func (ms *MasterServer) blockVolumeExpandHandler(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]
	if name == "" {
		writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("name is required"))
		return
	}

	var req blockapi.ExpandVolumeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("invalid request body: %w", err))
		return
	}
	if req.NewSizeBytes == 0 {
		writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("new_size_bytes must be > 0"))
		return
	}

	resp, err := ms.ExpandBlockVolume(r.Context(), &master_pb.ExpandBlockVolumeRequest{
		Name:         name,
		NewSizeBytes: req.NewSizeBytes,
	})
	if err != nil {
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	}
	writeJsonQuiet(w, r, http.StatusOK, blockapi.ExpandVolumeResponse{CapacityBytes: resp.CapacityBytes})
}

// blockStatusHandler handles GET /block/status — cluster summary with health counts.
func (ms *MasterServer) blockStatusHandler(w http.ResponseWriter, r *http.Request) {
	healthSummary := ms.blockRegistry.ComputeClusterHealthSummary()
	status := blockapi.BlockStatusResponse{
		VolumeCount:           len(ms.blockRegistry.ListAll()),
		ServerCount:           len(ms.blockRegistry.BlockCapableServers()),
		PromotionLSNTolerance: ms.blockRegistry.PromotionLSNTolerance(),
		BarrierLagLSN:         ms.blockRegistry.MaxBarrierLagLSN(),
		PromotionsTotal:       int64(ms.blockRegistry.PromotionsTotal.Load()),
		FailoversTotal:        int64(ms.blockRegistry.FailoversTotal.Load()),
		RebuildsTotal:         int64(ms.blockRegistry.RebuildsTotal.Load()),
		AssignmentQueueDepth:  ms.blockAssignmentQueue.TotalPending(),
		HealthyCount:          healthSummary.Healthy,
		DegradedCount:         healthSummary.Degraded,
		RebuildingCount:       healthSummary.Rebuilding,
		UnsafeCount:           healthSummary.Unsafe,
		NvmeCapableServers:    ms.blockRegistry.NvmeCapableServerCount(),
	}
	writeJsonQuiet(w, r, http.StatusOK, status)
}

// blockVolumePreflightHandler handles GET /block/volume/{name}/preflight.
// Returns a read-only promotion preflight evaluation for the named volume.
func (ms *MasterServer) blockVolumePreflightHandler(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]
	if name == "" {
		writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("name is required"))
		return
	}

	pf, err := ms.blockRegistry.EvaluatePromotion(name)
	if err != nil {
		writeJsonError(w, r, http.StatusNotFound, err)
		return
	}

	resp := blockapi.PreflightResponse{
		VolumeName: pf.VolumeName,
		Promotable: pf.Promotable,
		Reason:     pf.Reason,
	}
	if pf.Candidate != nil {
		resp.CandidateServer = pf.Candidate.Server
		resp.CandidateHealth = pf.Candidate.HealthScore
		resp.CandidateWALLSN = pf.Candidate.WALHeadLSN
	}
	for _, rej := range pf.Rejections {
		resp.Rejections = append(resp.Rejections, blockapi.PreflightRejection{
			Server: rej.Server,
			Reason: rej.Reason,
		})
	}
	// Add primary liveness info.
	entry, ok := ms.blockRegistry.Lookup(name)
	if ok {
		resp.PrimaryServer = entry.VolumeServer
		resp.PrimaryAlive = ms.blockRegistry.IsBlockCapable(entry.VolumeServer)
	}
	writeJsonQuiet(w, r, http.StatusOK, resp)
}

// blockVolumePromoteHandler handles POST /block/volume/{name}/promote.
// Triggers a manual promotion for the named block volume.
func (ms *MasterServer) blockVolumePromoteHandler(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]
	if name == "" {
		writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("name is required"))
		return
	}

	var req blockapi.PromoteVolumeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("decode request: %w", err))
		return
	}

	// ManualPromote captures oldPrimary/oldPath under lock to avoid TOCTOU (BUG-T5-2).
	newEpoch, oldPrimary, oldPath, pf, err := ms.blockRegistry.ManualPromote(name, req.TargetServer, req.Force)
	if err != nil {
		// Distinguish not-found from rejection.
		status := http.StatusConflict
		if pf.Reason == "volume not found" {
			status = http.StatusNotFound
		}
		// Build structured rejection response.
		resp := blockapi.PromoteVolumeResponse{
			Reason: pf.Reason,
		}
		for _, rej := range pf.Rejections {
			resp.Rejections = append(resp.Rejections, blockapi.PreflightRejection{
				Server: rej.Server,
				Reason: rej.Reason,
			})
		}
		glog.V(0).Infof("manual promote %q rejected: %s", name, pf.Reason)
		writeJsonQuiet(w, r, status, resp)
		return
	}

	// Post-promotion orchestration (same as auto path).
	ms.finalizePromotion(name, oldPrimary, oldPath, newEpoch)

	if req.Reason != "" {
		glog.V(0).Infof("manual promote %q: reason=%q", name, req.Reason)
	}

	// Re-read to get the new primary server name.
	entry, _ := ms.blockRegistry.Lookup(name)
	writeJsonQuiet(w, r, http.StatusOK, blockapi.PromoteVolumeResponse{
		NewPrimary: entry.VolumeServer,
		Epoch:      newEpoch,
	})
}

// entryToVolumeInfo converts a BlockVolumeEntry to a blockapi.VolumeInfo.
// primaryAlive indicates whether the primary server is alive (in blockServers set).
func entryToVolumeInfo(e *BlockVolumeEntry, primaryAlive bool) blockapi.VolumeInfo {
	status := "pending"
	if e.Status == StatusActive {
		status = "active"
	}
	rf := e.ReplicaFactor
	if rf == 0 {
		rf = 2 // default
	}
	durMode := e.DurabilityMode
	if durMode == "" {
		durMode = "best_effort"
	}
	info := blockapi.VolumeInfo{
		Name:             e.Name,
		VolumeServer:     e.VolumeServer,
		SizeBytes:        e.SizeBytes,
		ReplicaPlacement: e.ReplicaPlacement,
		Epoch:            e.Epoch,
		Role:             blockvol.RoleFromWire(e.Role).String(),
		Status:           status,
		ISCSIAddr:        e.ISCSIAddr,
		IQN:              e.IQN,
		ReplicaServer:    e.ReplicaServer,
		ReplicaISCSIAddr: e.ReplicaISCSIAddr,
		ReplicaIQN:       e.ReplicaIQN,
		ReplicaDataAddr:  e.ReplicaDataAddr,
		ReplicaCtrlAddr:  e.ReplicaCtrlAddr,
		ReplicaFactor:    rf,
		ReplicaReady:     e.ReplicaReady,
		HealthScore:      e.HealthScore,
		ReplicaDegraded:  e.ReplicaDegraded,
		DurabilityMode:   durMode,
		Preset:           e.Preset,
		NvmeAddr:         e.NvmeAddr,
		NQN:              e.NQN,
		HealthState:      deriveHealthStateWithLiveness(e, primaryAlive),
		VolumeMode:       e.VolumeMode,
	}
	for _, ri := range e.Replicas {
		info.Replicas = append(info.Replicas, blockapi.ReplicaDetail{
			Server:      ri.Server,
			ISCSIAddr:   ri.ISCSIAddr,
			IQN:         ri.IQN,
			Ready:       ri.Ready,
			HealthScore: ri.HealthScore,
			WALLag:      ri.WALLag,
		})
	}
	return info
}
