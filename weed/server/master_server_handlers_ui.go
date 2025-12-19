package weed_server

import (
	"net/http"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util/version"

	ui "github.com/seaweedfs/seaweedfs/weed/server/master_ui"
	"github.com/seaweedfs/seaweedfs/weed/stats"
)

func (ms *MasterServer) uiStatusHandler(w http.ResponseWriter, r *http.Request) {
	infos := make(map[string]interface{})
	infos["Up Time"] = time.Since(startTime).Truncate(time.Second).String()
	infos["Max Volume Id"] = ms.Topo.GetMaxVolumeId()

	ms.Topo.RaftServerAccessLock.RLock()
	defer ms.Topo.RaftServerAccessLock.RUnlock()

	if ms.Topo.HashicorpRaft != nil {
		args := struct {
			Version           string
			Topology          interface{}
			RaftServer        interface{}
			Stats             map[string]interface{}
			Counters          *stats.ServerStats
			VolumeSizeLimitMB uint32
		}{
			version.Version(),
			ms.Topo.ToInfo(),
			ms.Topo.HashicorpRaft,
			infos,
			serverStats,
			ms.option.VolumeSizeLimitMB,
		}
		ui.StatusNewRaftTpl.Execute(w, args)
	}
}
