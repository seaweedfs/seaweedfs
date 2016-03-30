package weed_server

import (
	"net/http"

	"github.com/chrislusf/seaweedfs/go/stats"
	"github.com/chrislusf/seaweedfs/go/util"
	ui "github.com/chrislusf/seaweedfs/go/weed/weed_server/master_ui"
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
		Version:  util.VERSION,
		Topology: ms.Topo.ToMap(),
		Leader:   ms.Topo.GetRaftServer().Leader(),
		Peers:    ms.Topo.GetRaftServer().Peers(),
		Stats:    infos,
		Counters: serverStats,
	}
	ui.StatusTpl.Execute(w, args)
}
