package weed_server

import (
	"net/http"
	"path/filepath"

	"github.com/chrislusf/weed-fs/go/stats"
	"github.com/chrislusf/weed-fs/go/util"
	ui "github.com/chrislusf/weed-fs/go/weed/weed_server/volume_server_ui"
)

func (vs *VolumeServer) uiStatusHandler(w http.ResponseWriter, r *http.Request) {
	infos := make(map[string]interface{})
	infos["Version"] = util.VERSION
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
	}{
		util.VERSION,
		vs.masterNode,
		vs.store.Status(),
		ds,
		infos,
	}
	ui.StatusTpl.Execute(w, args)
}
