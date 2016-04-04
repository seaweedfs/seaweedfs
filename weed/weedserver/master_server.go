package weedserver

import (
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"

	"net/http/pprof"

	"github.com/chrislusf/raft"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/sequence"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/topology"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/gorilla/mux"
)

type MasterServer struct {
	port                    int
	metaFolder              string
	volumeSizeLimitMB       uint
	pulseSeconds            int
	defaultReplicaPlacement string
	garbageThreshold        string
	guard                   *security.Guard

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
	secureKey string,
) *MasterServer {
	ms := &MasterServer{
		port:                    port,
		volumeSizeLimitMB:       volumeSizeLimitMB,
		pulseSeconds:            pulseSeconds,
		defaultReplicaPlacement: defaultReplicaPlacement,
		garbageThreshold:        garbageThreshold,
	}
	ms.bounedLeaderChan = make(chan int, 16)
	seq := sequence.NewMemorySequencer()
	cs := storage.NewCollectionSettings(defaultReplicaPlacement, garbageThreshold)
	var e error
	if ms.Topo, e = topology.NewTopology("topo", confFile, cs,
		seq, uint64(volumeSizeLimitMB)*1024*1024, pulseSeconds); e != nil {
		glog.Fatalf("cannot create topology:%s", e)
	}
	ms.vg = topology.NewDefaultVolumeGrowth()
	glog.V(0).Infoln("Volume Size Limit is", volumeSizeLimitMB, "MB")

	ms.guard = security.NewGuard(whiteList, secureKey)

	r.HandleFunc("/", ms.uiStatusHandler)
	r.HandleFunc("/ui/index.html", ms.uiStatusHandler)
	r.HandleFunc("/dir/assign", ms.proxyToLeader(ms.guard.WhiteList(ms.dirAssignHandler)))
	r.HandleFunc("/dir/lookup", ms.proxyToLeader(ms.guard.WhiteList(ms.dirLookupHandler)))
	r.HandleFunc("/dir/join", ms.proxyToLeader(ms.guard.WhiteList(ms.dirJoinHandler)))
	r.HandleFunc("/dir/join2", ms.proxyToLeader(ms.guard.WhiteList(ms.dirJoin2Handler)))
	r.HandleFunc("/dir/status", ms.proxyToLeader(ms.guard.WhiteList(ms.dirStatusHandler)))
	r.HandleFunc("/col/delete", ms.proxyToLeader(ms.guard.WhiteList(ms.collectionDeleteHandler)))
	r.HandleFunc("/vol/lookup", ms.proxyToLeader(ms.guard.WhiteList(ms.volumeLookupHandler)))
	r.HandleFunc("/vol/grow", ms.proxyToLeader(ms.guard.WhiteList(ms.volumeGrowHandler)))
	r.HandleFunc("/vol/status", ms.proxyToLeader(ms.guard.WhiteList(ms.volumeStatusHandler)))
	r.HandleFunc("/vol/vacuum", ms.proxyToLeader(ms.guard.WhiteList(ms.volumeVacuumHandler)))
	r.HandleFunc("/vol/check_replicate", ms.proxyToLeader(ms.guard.WhiteList(ms.volumeCheckReplicateHandler)))
	r.HandleFunc("/submit", ms.guard.WhiteList(ms.submitFromMasterServerHandler))
	r.HandleFunc("/delete", ms.guard.WhiteList(ms.deleteFromMasterServerHandler))
	r.HandleFunc("/{fileId}", ms.proxyToLeader(ms.redirectHandler))
	r.HandleFunc("/stats/counter", ms.guard.WhiteList(statsCounterHandler))
	r.HandleFunc("/stats/memory", ms.guard.WhiteList(statsMemoryHandler))
	r.HandleFunc("/debug/pprof/", ms.guard.WhiteList(pprof.Index))
	r.HandleFunc("/debug/pprof/trace", ms.guard.WhiteList(pprof.Trace))
	r.HandleFunc("/debug/pprof/profile", ms.guard.WhiteList(pprof.Profile))
	r.HandleFunc("/debug/pprof/cmdline", ms.guard.WhiteList(pprof.Cmdline))
	r.HandleFunc("/debug/pprof/symbol", ms.guard.WhiteList(pprof.Symbol))
	r.HandleFunc("/debug/pprof/{name}", ms.guard.WhiteList(pprof.Index))
	ms.Topo.StartRefreshWritableVolumes()

	return ms
}

func (ms *MasterServer) SetRaftServer(raftServer *RaftServer) {
	ms.Topo.SetRaftServer(raftServer.raftServer)
	ms.Topo.GetRaftServer().AddEventListener(raft.LeaderChangeEventType, func(e raft.Event) {
		if ms.Topo.GetRaftServer().Leader() != "" {
			glog.V(0).Infoln("[", ms.Topo.GetRaftServer().Name(), "]", ms.Topo.GetRaftServer().Leader(), "becomes leader.")
		}
	})
	if ms.Topo.IsLeader() {
		glog.V(0).Infoln("[", ms.Topo.GetRaftServer().Name(), "]", "I am the leader!")
	} else {
		if ms.Topo.GetRaftServer().Leader() != "" {
			glog.V(0).Infoln("[", ms.Topo.GetRaftServer().Name(), "]", ms.Topo.GetRaftServer().Leader(), "is the leader.")
		}
	}
}

func (ms *MasterServer) proxyToLeader(f func(w http.ResponseWriter, r *http.Request)) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if ms.Topo.IsLeader() {
			f(w, r)
		} else if ms.Topo.GetRaftServer() != nil && ms.Topo.GetRaftServer().Leader() != "" {
			ms.bounedLeaderChan <- 1
			defer func() { <-ms.bounedLeaderChan }()
			targetUrl, err := url.Parse("http://" + ms.Topo.GetRaftServer().Leader())
			if err != nil {
				writeJsonError(w, r, http.StatusInternalServerError,
					fmt.Errorf("Leader URL http://%s Parse Error: %v", ms.Topo.GetRaftServer().Leader(), err))
				return
			}
			glog.V(4).Infoln("proxying to leader", ms.Topo.GetRaftServer().Leader())
			proxy := httputil.NewSingleHostReverseProxy(targetUrl)
			director := proxy.Director
			proxy.Director = func(req *http.Request) {
				actualHost, err := security.GetActualRemoteHost(req)
				if err == nil {
					req.Header.Set("HTTP_X_FORWARDED_FOR", actualHost)
				}
				director(req)
			}
			proxy.Transport = util.Transport
			proxy.ServeHTTP(w, r)
		} else {
			//drop it to the floor
			//writeJsonError(w, r, errors.New(ms.Topo.RaftServer.Name()+" does not know Leader yet:"+ms.Topo.RaftServer.Leader()))
		}
	}
}
