package weed_server

import (
	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/replication"
	"code.google.com/p/weed-fs/go/sequence"
	"code.google.com/p/weed-fs/go/topology"
	"github.com/gorilla/mux"
	"path"
	"sync"
)

type MasterServer struct {
	port              int
	metaFolder        string
	volumeSizeLimitMB uint
	pulseSeconds      int
	defaultRepType    string
	garbageThreshold  string
	whiteList         []string
	version           string

	topo   *topology.Topology
	vg     *replication.VolumeGrowth
	vgLock sync.Mutex
}

func NewMasterServer(r *mux.Router, version string, port int, metaFolder string,
	volumeSizeLimitMB uint,
	pulseSeconds int,
	confFile string,
	defaultRepType string,
	garbageThreshold string,
	whiteList []string) *MasterServer {
	ms := &MasterServer{
		version:           version,
		volumeSizeLimitMB: volumeSizeLimitMB,
		pulseSeconds:      pulseSeconds,
		defaultRepType:    defaultRepType,
		garbageThreshold:  garbageThreshold,
		whiteList:         whiteList,
	}
	seq := sequence.NewFileSequencer(path.Join(metaFolder, "weed.seq"))
	var e error
	if ms.topo, e = topology.NewTopology("topo", confFile, seq,
		uint64(volumeSizeLimitMB)*1024*1024, pulseSeconds); e != nil {
		glog.Fatalf("cannot create topology:%s", e)
	}
	ms.vg = replication.NewDefaultVolumeGrowth()
	glog.V(0).Infoln("Volume Size Limit is", volumeSizeLimitMB, "MB")

	r.HandleFunc("/dir/assign", secure(ms.whiteList, ms.dirAssignHandler))
	r.HandleFunc("/dir/lookup", secure(ms.whiteList, ms.dirLookupHandler))
	r.HandleFunc("/dir/join", secure(ms.whiteList, ms.dirJoinHandler))
	r.HandleFunc("/dir/status", secure(ms.whiteList, ms.dirStatusHandler))
	r.HandleFunc("/vol/grow", secure(ms.whiteList, ms.volumeGrowHandler))
	r.HandleFunc("/vol/status", secure(ms.whiteList, ms.volumeStatusHandler))
	r.HandleFunc("/vol/vacuum", secure(ms.whiteList, ms.volumeVacuumHandler))
	r.HandleFunc("/submit", secure(ms.whiteList, ms.submitFromMasterServerHandler))
	r.HandleFunc("/", ms.redirectHandler)

	ms.topo.StartRefreshWritableVolumes(garbageThreshold)

	return ms
}
