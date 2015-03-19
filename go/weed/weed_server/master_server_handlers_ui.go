package weed_server

import (
	"net/http"

	"github.com/chrislusf/weed-fs/go/util"
	ui "github.com/chrislusf/weed-fs/go/weed/weed_server/master_ui"
)

func (ms *MasterServer) uiStatusHandler(w http.ResponseWriter, r *http.Request) {
	stats := make(map[string]interface{})
	stats["Version"] = util.VERSION
	args := struct {
		Version  string
		Topology interface{}
		Peers    interface{}
		Stats    interface{}
	}{
		util.VERSION,
		ms.Topo.ToMap(),
		ms.Topo.RaftServer.Peers(),
		stats,
		//serverStats,
	}
	ui.StatusTpl.Execute(w, args)
}
