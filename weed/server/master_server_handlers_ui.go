package weed_server

import (
	"net/http"

	ui "github.com/HZ89/seaweedfs/weed/server/master_ui"
	"github.com/HZ89/seaweedfs/weed/stats"
	"github.com/HZ89/seaweedfs/weed/util"
	"github.com/chrislusf/raft"
)

func (ms *MasterServer) uiStatusHandler(w http.ResponseWriter, r *http.Request) {
	infos := make(map[string]interface{})
	infos["Version"] = util.VERSION
	args := struct {
		Version    string
		Topology   interface{}
		RaftServer raft.Server
		Stats      map[string]interface{}
		Counters   *stats.ServerStats
	}{
		util.VERSION,
		ms.Topo.ToMap(),
		ms.Topo.RaftServer,
		infos,
		serverStats,
	}
	ui.StatusTpl.Execute(w, args)
}
