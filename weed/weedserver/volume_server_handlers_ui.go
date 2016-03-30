package weedserver

import (
	"net/http"
	"path/filepath"
	"time"

	"github.com/chrislusf/seaweedfs/weed/stats"
	"github.com/chrislusf/seaweedfs/weed/util"
	ui "github.com/chrislusf/seaweedfs/weed/weedserver/volume_server_ui"
)

func (vs *VolumeServer) uiStatusHandler(w http.ResponseWriter, r *http.Request) {
	infos := make(map[string]interface{})
	infos["Version"] = util.VERSION
	infos["Up Time"] = time.Now().Sub(startTime).String()
	var ds []*stats.DiskStatus
	for _, loc := range vs.store.Locations {
		if dir, e := filepath.Abs(loc.Directory); e == nil {
			ds = append(ds, stats.NewDiskStatus(dir))
		}
	}
	args := struct {
		Version      string
		Master       string
		Volumes      interface{}
		DiskStatuses interface{}
		Stats        interface{}
		Counters     *stats.ServerStats
	}{
		util.VERSION,
		vs.GetMasterNode(),
		vs.store.Status(),
		ds,
		infos,
		stats.ServStats,
	}
	ui.StatusTpl.Execute(w, args)
}
