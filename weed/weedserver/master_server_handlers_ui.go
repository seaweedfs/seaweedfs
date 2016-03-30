package weedserver

import (
	"net/http"

	"github.com/chrislusf/seaweedfs/weed/stats"
	"github.com/chrislusf/seaweedfs/weed/util"
	ui "github.com/chrislusf/seaweedfs/weed/weedserver/master_ui"
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
