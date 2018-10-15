package weed_server

import (
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/stats"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/util"
	"net/http"
	"path/filepath"
)

func (vs *VolumeServer) statusHandler(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]interface{})
	m["Version"] = util.VERSION
	m["Volumes"] = vs.store.Status()
	writeJsonQuiet(w, r, http.StatusOK, m)
}

func (vs *VolumeServer) statsDiskHandler(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]interface{})
	m["Version"] = util.VERSION
	var ds []*stats.DiskStatus
	for _, loc := range vs.store.Locations {
		if dir, e := filepath.Abs(loc.Directory); e == nil {
			ds = append(ds, stats.NewDiskStatus(dir))
		}
	}
	m["DiskStatuses"] = ds
	writeJsonQuiet(w, r, http.StatusOK, m)
}

// TODO delete this when volume sync is all moved to grpc
func (vs *VolumeServer) getVolume(volumeParameterName string, r *http.Request) (*storage.Volume, error) {
	vid, err := vs.getVolumeId(volumeParameterName, r)
	if err != nil {
		return nil, err
	}
	v := vs.store.GetVolume(vid)
	if v == nil {
		return nil, fmt.Errorf("Not Found Volume Id %d", vid)
	}
	return v, nil
}
