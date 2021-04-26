package weed_server

import (
	"net/http"
	"path/filepath"

	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/stats"
	"github.com/chrislusf/seaweedfs/weed/util"
)

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
