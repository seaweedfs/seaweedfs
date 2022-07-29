package weed_server

import (
	"github.com/seaweedfs/seaweedfs/weed/topology"
	"net/http"
	"path/filepath"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func (vs *VolumeServer) healthzHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Server", "SeaweedFS Volume "+util.VERSION)
	volumeInfos := vs.store.VolumeInfos()
	for _, vinfo := range volumeInfos {
		if len(vinfo.Collection) == 0 {
			continue
		}
		if vinfo.ReplicaPlacement.GetCopyCount() > 1 {
			_, err := topology.GetWritableRemoteReplications(vs.store, vs.grpcDialOption, vinfo.Id, vs.GetMaster)
			if err != nil {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
		}
	}
	w.WriteHeader(http.StatusOK)
}

func (vs *VolumeServer) statusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Server", "SeaweedFS Volume "+util.VERSION)
	m := make(map[string]interface{})
	m["Version"] = util.Version()
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
	w.Header().Set("Server", "SeaweedFS Volume "+util.VERSION)
	m := make(map[string]interface{})
	m["Version"] = util.Version()
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
