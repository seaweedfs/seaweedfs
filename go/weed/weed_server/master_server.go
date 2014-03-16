package weed_server

import (
	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/replication"
	"code.google.com/p/weed-fs/go/sequence"
	"code.google.com/p/weed-fs/go/topology"
	"code.google.com/p/weed-fs/go/util"
	"github.com/goraft/raft"
	"github.com/gorilla/mux"
	"net/http"
	"net/http/httputil"
	"net/url"
	"path"
	"sync"
)

type MasterServer struct {
	port                    int
	metaFolder              string
	volumeSizeLimitMB       uint
	pulseSeconds            int
	defaultReplicaPlacement string
	garbageThreshold        string
	whiteList               []string
	version                 string

	Topo   *topology.Topology
	vg     *replication.VolumeGrowth
	vgLock sync.Mutex

	bounedLeaderChan chan int
}

func NewMasterServer(r *mux.Router, version string, port int, metaFolder string,
	volumeSizeLimitMB uint,
	pulseSeconds int,
	confFile string,
	defaultReplicaPlacement string,
	garbageThreshold string,
	whiteList []string,
) *MasterServer {
	ms := &MasterServer{
		version:                 version,
		volumeSizeLimitMB:       volumeSizeLimitMB,
		pulseSeconds:            pulseSeconds,
		defaultReplicaPlacement: defaultReplicaPlacement,
		garbageThreshold:        garbageThreshold,
		whiteList:               whiteList,
	}
	ms.bounedLeaderChan = make(chan int, 16)
	seq := sequence.NewFileSequencer(path.Join(metaFolder, "weed.seq"))
	var e error
	if ms.Topo, e = topology.NewTopology("topo", confFile, seq,
		uint64(volumeSizeLimitMB)*1024*1024, pulseSeconds); e != nil {
		glog.Fatalf("cannot create topology:%s", e)
	}
	ms.vg = replication.NewDefaultVolumeGrowth()
	glog.V(0).Infoln("Volume Size Limit is", volumeSizeLimitMB, "MB")

	r.HandleFunc("/dir/assign", ms.proxyToLeader(secure(ms.whiteList, ms.dirAssignHandler)))
	r.HandleFunc("/dir/lookup", ms.proxyToLeader(secure(ms.whiteList, ms.dirLookupHandler)))
	r.HandleFunc("/dir/join", ms.proxyToLeader(secure(ms.whiteList, ms.dirJoinHandler)))
	r.HandleFunc("/dir/status", ms.proxyToLeader(secure(ms.whiteList, ms.dirStatusHandler)))
	r.HandleFunc("/col/delete", ms.proxyToLeader(secure(ms.whiteList, ms.collectionDeleteHandler)))
	r.HandleFunc("/vol/grow", ms.proxyToLeader(secure(ms.whiteList, ms.volumeGrowHandler)))
	r.HandleFunc("/vol/status", ms.proxyToLeader(secure(ms.whiteList, ms.volumeStatusHandler)))
	r.HandleFunc("/vol/vacuum", ms.proxyToLeader(secure(ms.whiteList, ms.volumeVacuumHandler)))
	r.HandleFunc("/submit", secure(ms.whiteList, ms.submitFromMasterServerHandler))
	r.HandleFunc("/{filekey}", ms.redirectHandler)

	ms.Topo.StartRefreshWritableVolumes(garbageThreshold)

	return ms
}

func (ms *MasterServer) SetRaftServer(raftServer *RaftServer) {
	ms.Topo.RaftServer = raftServer.raftServer
	ms.Topo.RaftServer.AddEventListener(raft.LeaderChangeEventType, func(e raft.Event) {
		glog.V(0).Infoln("[", ms.Topo.RaftServer.Name(), "]", ms.Topo.RaftServer.Leader(), "becomes leader.")
	})
	if ms.Topo.IsLeader() {
		glog.V(0).Infoln("[", ms.Topo.RaftServer.Name(), "]", "I am the leader!")
	} else {
		glog.V(0).Infoln("[", ms.Topo.RaftServer.Name(), "]", ms.Topo.RaftServer.Leader(), "is the leader.")
	}
}

func (ms *MasterServer) proxyToLeader(f func(w http.ResponseWriter, r *http.Request)) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if ms.Topo.IsLeader() {
			f(w, r)
		} else {
			ms.bounedLeaderChan <- 1
			defer func() { <-ms.bounedLeaderChan }()
			targetUrl, err := url.Parse("http://" + ms.Topo.RaftServer.Leader())
			if err != nil {
				writeJsonQuiet(w, r, map[string]interface{}{"error": "Leader URL Parse Error " + err.Error()})
				return
			}
			glog.V(4).Infoln("proxying to leader", ms.Topo.RaftServer.Leader())
			proxy := httputil.NewSingleHostReverseProxy(targetUrl)
			proxy.Transport = util.Transport
			proxy.ServeHTTP(w, r)
		}
	}
}
