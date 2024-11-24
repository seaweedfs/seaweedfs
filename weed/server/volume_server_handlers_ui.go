package weed_server

import (
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"net/http"
	"path/filepath"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	ui "github.com/seaweedfs/seaweedfs/weed/server/volume_server_ui"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func (vs *VolumeServer) uiStatusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Server", "SeaweedFS Volume "+util.VERSION)
	infos := make(map[string]interface{})
	infos["Up Time"] = time.Now().Sub(startTime).String()
	var ds []*volume_server_pb.DiskStatus
	for _, loc := range vs.store.Locations {
		if dir, e := filepath.Abs(loc.Directory); e == nil {
			newDiskStatus := stats.NewDiskStatus(dir)
			newDiskStatus.DiskType = loc.DiskType.String()
			ds = append(ds, newDiskStatus)
		}
	}
	volumeInfos := vs.store.VolumeInfos()
	var normalVolumeInfos, remoteVolumeInfos []*storage.VolumeInfo
	for _, vinfo := range volumeInfos {
		if vinfo.IsRemote() {
			remoteVolumeInfos = append(remoteVolumeInfos, vinfo)
		} else {
			normalVolumeInfos = append(normalVolumeInfos, vinfo)
		}
	}
	args := struct {
		Version       string
		Masters       []pb.ServerAddress
		Volumes       interface{}
		EcVolumes     interface{}
		RemoteVolumes interface{}
		DiskStatuses  interface{}
		Stats         interface{}
		Counters      *stats.ServerStats
	}{
		util.Version(),
		vs.SeedMasterNodes,
		normalVolumeInfos,
		vs.store.EcVolumes(),
		remoteVolumeInfos,
		ds,
		infos,
		serverStats,
	}
	ui.StatusTpl.Execute(w, args)
}
