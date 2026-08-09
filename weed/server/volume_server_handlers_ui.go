package weed_server

import (
	"net/http"
	"path/filepath"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/util/version"

	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	ui "github.com/seaweedfs/seaweedfs/weed/server/volume_server_ui"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage"
)

// remoteVolumeRow carries the remote key the store no longer keeps per volume,
// read from the volume this server holds rather than relayed by a master.
type remoteVolumeRow struct {
	*storage.VolumeInfo
	RemoteStorageKey string
}

func (vs *VolumeServer) uiStatusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Server", "SeaweedFS Volume "+version.VERSION)
	infos := make(map[string]interface{})
	infos["Up Time"] = time.Since(startTime).Truncate(time.Second).String()
	var ds []*volume_server_pb.DiskStatus
	for _, loc := range vs.store.Locations {
		if dir, e := filepath.Abs(loc.Directory); e == nil {
			newDiskStatus := stats.NewDiskStatus(dir)
			newDiskStatus.DiskType = loc.DiskType.String()
			ds = append(ds, newDiskStatus)
		}
	}
	volumeInfos := vs.store.VolumeInfos()
	var normalVolumeInfos []*storage.VolumeInfo
	var remoteVolumeInfos []remoteVolumeRow
	for _, vinfo := range volumeInfos {
		if !vinfo.IsRemote() {
			normalVolumeInfos = append(normalVolumeInfos, vinfo)
			continue
		}
		row := remoteVolumeRow{VolumeInfo: vinfo}
		if v := vs.store.GetVolume(vinfo.Id); v != nil {
			_, row.RemoteStorageKey = v.RemoteStorageNameKey()
		}
		remoteVolumeInfos = append(remoteVolumeInfos, row)
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
		version.Version(),
		vs.SeedMasterNodes,
		normalVolumeInfos,
		vs.store.EcVolumes(),
		remoteVolumeInfos,
		ds,
		infos,
		serverStats,
	}
	if err := ui.StatusTpl.Execute(w, args); err != nil {
		glog.Errorf("template execution error: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}
