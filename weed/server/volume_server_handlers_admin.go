package weed_server

import (
	"net/http"
	"path/filepath"

	"github.com/seaweedfs/seaweedfs/weed/util/version"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
)

// healthzHandler checks the local health of the volume server.
// It only checks local conditions to avoid cascading failures when remote
// volume servers go down. Previously, this handler checked if all replicated
// volumes could reach their remote replicas, which caused healthy volume
// servers to fail health checks when a peer went down.
// See https://github.com/seaweedfs/seaweedfs/issues/6823
func (vs *VolumeServer) healthzHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Server", "SeaweedFS Volume "+version.VERSION)

	// Check if the server is shutting down
	if vs.store.IsStopping() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	// Check if we can communicate with master
	if !vs.isHeartbeating {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (vs *VolumeServer) statusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Server", "SeaweedFS Volume "+version.VERSION)
	m := make(map[string]interface{})
	m["Version"] = version.Version()
	var ds []*volume_server_pb.DiskStatus
	for _, loc := range vs.store.Locations {
		if dir, e := filepath.Abs(loc.Directory); e == nil {
			newDiskStatus := stats.NewDiskStatus(dir)
			newDiskStatus.DiskType = loc.DiskType.String()
			ds = append(ds, newDiskStatus)
		}
	}
	m["DiskStatuses"] = ds
	m["Volumes"] = vs.store.VolumeInfos()
	writeJsonQuiet(w, r, http.StatusOK, m)
}

func (vs *VolumeServer) statsDiskHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Server", "SeaweedFS Volume "+version.VERSION)
	m := make(map[string]interface{})
	m["Version"] = version.Version()
	var ds []*volume_server_pb.DiskStatus
	for _, loc := range vs.store.Locations {
		if dir, e := filepath.Abs(loc.Directory); e == nil {
			newDiskStatus := stats.NewDiskStatus(dir)
			newDiskStatus.DiskType = loc.DiskType.String()
			ds = append(ds, newDiskStatus)
		}
	}
	m["DiskStatuses"] = ds
	writeJsonQuiet(w, r, http.StatusOK, m)
}
