package weed_server

import (
	"net/http"

	"github.com/chrislusf/weed-fs/go/stats"
	"github.com/chrislusf/weed-fs/go/util"
	ui "github.com/chrislusf/weed-fs/go/weed/weed_server/master_ui"
)

func (ms *MasterServer) uiStatusHandler(w http.ResponseWriter, r *http.Request) {
	infos := make(map[string]interface{})
	infos["Version"] = util.VERSION
	args := struct {
		Version  string
		Topology interface{}
		Leader   string
		Peers    interface{}
		Stats    map[string]interface{}
		Counters *stats.ServerStats
	}{
		util.VERSION,
		ms.Topo.ToMap(),
		ms.Topo.RaftServer.Leader(),
		ms.Topo.RaftServer.Peers(),
		infos,
		serverStats,
	}
	ui.StatusTpl.Execute(w, args)
}
