package weed_server

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gorilla/mux"

	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol/blockapi"
)

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

	// Pre-validate durability_mode (cosmetic — real validation is in gRPC handler).
	if req.DurabilityMode != "" {
		if _, perr := blockvol.ParseDurabilityMode(req.DurabilityMode); perr != nil {
			writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("invalid durability_mode: %w", perr))
			return
		}
	}

	resp, err := ms.CreateBlockVolume(r.Context(), &master_pb.CreateBlockVolumeRequest{
		Name:           req.Name,
		SizeBytes:      req.SizeBytes,
		DiskType:       req.DiskType,
		DurabilityMode: req.DurabilityMode,
		ReplicaFactor:  uint32(req.ReplicaFactor),
	})
	if err != nil {
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	}

	// Store replica_placement on the registry entry.
	if entry, ok := ms.blockRegistry.Lookup(resp.VolumeId); ok {
		entry.ReplicaPlacement = replicaPlacement
	}

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
		info = entryToVolumeInfo(entry)
	}
	writeJsonQuiet(w, r, http.StatusOK, info)
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
	writeJsonQuiet(w, r, http.StatusOK, entryToVolumeInfo(entry))
}

// blockVolumeListHandler handles GET /block/volumes.
func (ms *MasterServer) blockVolumeListHandler(w http.ResponseWriter, r *http.Request) {
	entries := ms.blockRegistry.ListAll()
	infos := make([]blockapi.VolumeInfo, len(entries))
	for i, e := range entries {
		infos[i] = entryToVolumeInfo(e)
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

// blockStatusHandler handles GET /block/status — returns registry configuration for debugging.
func (ms *MasterServer) blockStatusHandler(w http.ResponseWriter, r *http.Request) {
	status := map[string]interface{}{
		"promotion_lsn_tolerance": ms.blockRegistry.PromotionLSNTolerance(),
		"volume_count":            len(ms.blockRegistry.ListAll()),
		"server_count":            len(ms.blockRegistry.BlockCapableServers()),
		"barrier_lag_lsn":         ms.blockRegistry.MaxBarrierLagLSN(),
		"promotions_total":        ms.blockRegistry.PromotionsTotal.Load(),
		"failovers_total":         ms.blockRegistry.FailoversTotal.Load(),
		"rebuilds_total":          ms.blockRegistry.RebuildsTotal.Load(),
		"assignment_queue_depth":  ms.blockAssignmentQueue.TotalPending(),
	}
	writeJsonQuiet(w, r, http.StatusOK, status)
}

// entryToVolumeInfo converts a BlockVolumeEntry to a blockapi.VolumeInfo.
func entryToVolumeInfo(e *BlockVolumeEntry) blockapi.VolumeInfo {
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
		HealthScore:      e.HealthScore,
		ReplicaDegraded:  e.ReplicaDegraded,
		DurabilityMode:   durMode,
	}
	for _, ri := range e.Replicas {
		info.Replicas = append(info.Replicas, blockapi.ReplicaDetail{
			Server:      ri.Server,
			ISCSIAddr:   ri.ISCSIAddr,
			IQN:         ri.IQN,
			HealthScore: ri.HealthScore,
			WALLag:      ri.WALLag,
		})
	}
	return info
}
