package weed_server

import (
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"

	"github.com/chrislusf/weed-fs/go/glog"
	"github.com/chrislusf/weed-fs/go/sequence"
	"github.com/chrislusf/weed-fs/go/topology"
	"github.com/chrislusf/weed-fs/go/util"
	"github.com/goraft/raft"
	"github.com/gorilla/mux"
)

type MasterServer struct {
	port                    int
	metaFolder              string
	volumeSizeLimitMB       uint
	pulseSeconds            int
	defaultReplicaPlacement string
	garbageThreshold        string
	whiteList               []string

	Topo   *topology.Topology
	vg     *topology.VolumeGrowth
	vgLock sync.Mutex

	bounedLeaderChan chan int
}

func NewMasterServer(r *mux.Router, port int, metaFolder string,
	volumeSizeLimitMB uint,
	pulseSeconds int,
	confFile string,
	defaultReplicaPlacement string,
	garbageThreshold string,
	whiteList []string,
) *MasterServer {
	ms := &MasterServer{
		port:                    port,
		volumeSizeLimitMB:       volumeSizeLimitMB,
		pulseSeconds:            pulseSeconds,
		defaultReplicaPlacement: defaultReplicaPlacement,
		garbageThreshold:        garbageThreshold,
		whiteList:               whiteList,
	}
	ms.bounedLeaderChan = make(chan int, 16)
	seq := sequence.NewMemorySequencer()
	var e error
	if ms.Topo, e = topology.NewTopology("topo", confFile, seq,
		uint64(volumeSizeLimitMB)*1024*1024, pulseSeconds); e != nil {
		glog.Fatalf("cannot create topology:%s", e)
	}
	ms.vg = topology.NewDefaultVolumeGrowth()
	glog.V(0).Infoln("Volume Size Limit is", volumeSizeLimitMB, "MB")

	r.HandleFunc("/dir/assign", ms.proxyToLeader(secure(ms.whiteList, ms.dirAssignHandler)))
	r.HandleFunc("/dir/lookup", ms.proxyToLeader(secure(ms.whiteList, ms.dirLookupHandler)))
	r.HandleFunc("/dir/join", ms.proxyToLeader(secure(ms.whiteList, ms.dirJoinHandler)))
	r.HandleFunc("/dir/status", ms.proxyToLeader(secure(ms.whiteList, ms.dirStatusHandler)))
	r.HandleFunc("/col/delete", ms.proxyToLeader(secure(ms.whiteList, ms.collectionDeleteHandler)))
	r.HandleFunc("/vol/lookup", ms.proxyToLeader(secure(ms.whiteList, ms.volumeLookupHandler)))
	r.HandleFunc("/vol/grow", ms.proxyToLeader(secure(ms.whiteList, ms.volumeGrowHandler)))
	r.HandleFunc("/vol/status", ms.proxyToLeader(secure(ms.whiteList, ms.volumeStatusHandler)))
	r.HandleFunc("/vol/vacuum", ms.proxyToLeader(secure(ms.whiteList, ms.volumeVacuumHandler)))
	r.HandleFunc("/submit", secure(ms.whiteList, ms.submitFromMasterServerHandler))
	r.HandleFunc("/delete", secure(ms.whiteList, ms.deleteFromMasterServerHandler))
	r.HandleFunc("/{fileId}", ms.redirectHandler)
	r.HandleFunc("/stats/counter", secure(ms.whiteList, statsCounterHandler))
	r.HandleFunc("/stats/memory", secure(ms.whiteList, statsMemoryHandler))

	ms.Topo.StartRefreshWritableVolumes(garbageThreshold)

	return ms
}

func (ms *MasterServer) SetRaftServer(raftServer *RaftServer) {
	ms.Topo.RaftServer = raftServer.raftServer
	ms.Topo.RaftServer.AddEventListener(raft.LeaderChangeEventType, func(e raft.Event) {
		if ms.Topo.RaftServer.Leader() != "" {
			glog.V(0).Infoln("[", ms.Topo.RaftServer.Name(), "]", ms.Topo.RaftServer.Leader(), "becomes leader.")
		}
	})
	if ms.Topo.IsLeader() {
		glog.V(0).Infoln("[", ms.Topo.RaftServer.Name(), "]", "I am the leader!")
	} else {
		if ms.Topo.RaftServer.Leader() != "" {
			glog.V(0).Infoln("[", ms.Topo.RaftServer.Name(), "]", ms.Topo.RaftServer.Leader(), "is the leader.")
		}
	}
}

func (ms *MasterServer) proxyToLeader(f func(w http.ResponseWriter, r *http.Request)) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if ms.Topo.IsLeader() {
			f(w, r)
		} else if ms.Topo.RaftServer != nil && ms.Topo.RaftServer.Leader() != "" {
			ms.bounedLeaderChan <- 1
			defer func() { <-ms.bounedLeaderChan }()
			targetUrl, err := url.Parse("http://" + ms.Topo.RaftServer.Leader())
			if err != nil {
				writeJsonQuiet(w, r, map[string]interface{}{"error": "Leader URL http://" + ms.Topo.RaftServer.Leader() + " Parse Error " + err.Error()})
				return
			}
			glog.V(4).Infoln("proxying to leader", ms.Topo.RaftServer.Leader())
			proxy := httputil.NewSingleHostReverseProxy(targetUrl)
			proxy.Transport = util.Transport
			proxy.ServeHTTP(w, r)
		} else {
			//drop it to the floor
			//writeJsonError(w, r, errors.New(ms.Topo.RaftServer.Name()+" does not know Leader yet:"+ms.Topo.RaftServer.Leader()))
		}
	}
}
