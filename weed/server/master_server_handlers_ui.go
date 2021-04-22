package weed_server

import (
	"net/http"
	"time"

	"github.com/chrislusf/raft"
	ui "github.com/chrislusf/seaweedfs/weed/server/master_ui"
	"github.com/chrislusf/seaweedfs/weed/stats"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func (ms *MasterServer) uiStatusHandler(w http.ResponseWriter, r *http.Request) {
	infos := make(map[string]interface{})
	infos["Up Time"] = time.Now().Sub(startTime).String()
	args := struct {
		Version           string
		Topology          interface{}
		RaftServer        raft.Server
		Stats             map[string]interface{}
		Counters          *stats.ServerStats
		VolumeSizeLimitMB uint
	}{
		util.Version(),
		ms.Topo.ToMap(),
		ms.Topo.RaftServer,
		infos,
		serverStats,
		ms.option.VolumeSizeLimitMB,
	}
	ui.StatusTpl.Execute(w, args)
}
