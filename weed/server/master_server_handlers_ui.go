package weed_server

import (
	"net/http"
	"time"

	"github.com/chrislusf/raft"
	hashicorpRaft "github.com/hashicorp/raft"

	ui "github.com/chrislusf/seaweedfs/weed/server/master_ui"
	"github.com/chrislusf/seaweedfs/weed/stats"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func (ms *MasterServer) uiStatusHandler(w http.ResponseWriter, r *http.Request) {
	infos := make(map[string]interface{})
	infos["Up Time"] = time.Now().Sub(startTime).String()
	infos["Max Volume Id"] = ms.Topo.GetMaxVolumeId()
	if ms.Topo.RaftServer != nil {
		args := struct {
			Version           string
			Topology          interface{}
			RaftServer        raft.Server
			Stats             map[string]interface{}
			Counters          *stats.ServerStats
			VolumeSizeLimitMB uint32
		}{
			util.Version(),
			ms.Topo.ToMap(),
			ms.Topo.RaftServer,
			infos,
			serverStats,
			ms.option.VolumeSizeLimitMB,
		}
		ui.StatusTpl.Execute(w, args)
	} else if ms.Topo.HashicorpRaft != nil {
		args := struct {
			Version           string
			Topology          interface{}
			RaftServer        *hashicorpRaft.Raft
			Stats             map[string]interface{}
			Counters          *stats.ServerStats
			VolumeSizeLimitMB uint32
		}{
			util.Version(),
			ms.Topo.ToMap(),
			ms.Topo.HashicorpRaft,
			infos,
			serverStats,
			ms.option.VolumeSizeLimitMB,
		}
		ui.StatusNewRaftTpl.Execute(w, args)
	}
}
