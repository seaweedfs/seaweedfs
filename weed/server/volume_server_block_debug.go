package weed_server

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/blockvol"
)

// ShipperDebugInfo is the real-time shipper state for one replica.
type ShipperDebugInfo struct {
	DataAddr   string `json:"data_addr"`
	State      string `json:"state"`
	FlushedLSN uint64 `json:"flushed_lsn"`
}

// BlockVolumeDebugInfo is the real-time block volume state.
type BlockVolumeDebugInfo struct {
	Path              string             `json:"path"`
	Role              string             `json:"role"`
	Mode              string             `json:"mode,omitempty"`
	Epoch             uint64             `json:"epoch"`
	HeadLSN           uint64             `json:"head_lsn"`
	Degraded          bool               `json:"degraded"`
	RoleApplied       bool               `json:"role_applied"`
	ReceiverReady     bool               `json:"receiver_ready"`
	ShipperConfigured bool               `json:"shipper_configured"`
	ShipperConnected  bool               `json:"shipper_connected"`
	ReplicaEligible   bool               `json:"replica_eligible"`
	PublishHealthy    bool               `json:"publish_healthy"`
	PublicationReason string             `json:"publication_reason,omitempty"`
	Shippers          []ShipperDebugInfo `json:"shippers,omitempty"`
	Timestamp         string             `json:"timestamp"`
}

// DebugInfoForVolume returns the current debug surface for one volume. When the
// Phase 15 core projection exists on the live path, this surface prefers the
// core-owned projection truth over adapter-local convenience flags.
func (bs *BlockService) DebugInfoForVolume(path string, vol *blockvol.BlockVol) BlockVolumeDebugInfo {
	status := vol.Status()
	readiness := bs.ReadinessSnapshot(path)
	info := BlockVolumeDebugInfo{
		Path:              path,
		Role:              status.Role.String(),
		Epoch:             status.Epoch,
		HeadLSN:           status.WALHeadLSN,
		Degraded:          status.ReplicaDegraded,
		RoleApplied:       readiness.RoleApplied,
		ReceiverReady:     readiness.ReceiverReady,
		ShipperConfigured: readiness.ShipperConfigured,
		ShipperConnected:  readiness.ShipperConnected,
		ReplicaEligible:   readiness.ReplicaEligible,
		PublishHealthy:    readiness.PublishHealthy,
		Timestamp:         time.Now().UTC().Format(time.RFC3339Nano),
	}
	if proj, ok := bs.CoreProjection(path); ok {
		info.Role = string(proj.Role)
		info.Mode = string(proj.Mode.Name)
		info.RoleApplied = proj.Readiness.RoleApplied
		info.ReceiverReady = proj.Readiness.ReceiverReady
		info.ShipperConfigured = proj.Readiness.ShipperConfigured
		info.ShipperConnected = proj.Readiness.ShipperConnected
		info.ReplicaEligible = proj.Readiness.ReplicaReady
		info.PublishHealthy = proj.Publication.Healthy
		info.PublicationReason = proj.Publication.Reason
	}
	return info
}

// debugBlockShipperHandler returns real-time shipper state for all block volumes.
// Unlike the master's replica_degraded (heartbeat-lagged), this reads directly
// from the shipper's atomic state field — no heartbeat delay.
//
// GET /debug/block/shipper
func (vs *VolumeServer) debugBlockShipperHandler(w http.ResponseWriter, r *http.Request) {
	if vs.blockService == nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]BlockVolumeDebugInfo{})
		return
	}

	store := vs.blockService.Store()
	if store == nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]BlockVolumeDebugInfo{})
		return
	}

	var infos []BlockVolumeDebugInfo
	store.IterateBlockVolumes(func(path string, vol *blockvol.BlockVol) {
		info := vs.blockService.DebugInfoForVolume(path, vol)

		// Get per-shipper state from ShipperGroup if available.
		sg := vol.GetShipperGroup()
		if sg != nil {
			for _, ss := range sg.ShipperStates() {
				info.Shippers = append(info.Shippers, ShipperDebugInfo{
					DataAddr:   ss.DataAddr,
					State:      ss.State,
					FlushedLSN: ss.FlushedLSN,
				})
			}
		}

		infos = append(infos, info)
	})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(infos)
}
