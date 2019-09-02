package weed_server

import (
	"context"
	"fmt"
	"github.com/joeslay/seaweedfs/weed/shell"
	"github.com/joeslay/seaweedfs/weed/wdclient"
	"google.golang.org/grpc"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/chrislusf/raft"
	"github.com/joeslay/seaweedfs/weed/glog"
	"github.com/joeslay/seaweedfs/weed/pb/master_pb"
	"github.com/joeslay/seaweedfs/weed/security"
	"github.com/joeslay/seaweedfs/weed/sequence"
	"github.com/joeslay/seaweedfs/weed/topology"
	"github.com/joeslay/seaweedfs/weed/util"
	"github.com/gorilla/mux"
	"github.com/spf13/viper"
)

type MasterOption struct {
	Port                    int
	MetaFolder              string
	VolumeSizeLimitMB       uint
	VolumePreallocate       bool
	PulseSeconds            int
	DefaultReplicaPlacement string
	GarbageThreshold        float64
	WhiteList               []string
	DisableHttp             bool
	MetricsAddress          string
	MetricsIntervalSec      int
}

type MasterServer struct {
	option *MasterOption
	guard  *security.Guard

	preallocateSize int64

	Topo   *topology.Topology
	vg     *topology.VolumeGrowth
	vgLock sync.Mutex

	bounedLeaderChan chan int

	// notifying clients
	clientChansLock sync.RWMutex
	clientChans     map[string]chan *master_pb.VolumeLocation

	grpcDialOpiton grpc.DialOption

	MasterClient *wdclient.MasterClient
}

func NewMasterServer(r *mux.Router, option *MasterOption, peers []string) *MasterServer {

	v := viper.GetViper()
	signingKey := v.GetString("jwt.signing.key")
	v.SetDefault("jwt.signing.expires_after_seconds", 10)
	expiresAfterSec := v.GetInt("jwt.signing.expires_after_seconds")

	readSigningKey := v.GetString("jwt.signing.read.key")
	v.SetDefault("jwt.signing.read.expires_after_seconds", 60)
	readExpiresAfterSec := v.GetInt("jwt.signing.read.expires_after_seconds")

	var preallocateSize int64
	if option.VolumePreallocate {
		preallocateSize = int64(option.VolumeSizeLimitMB) * (1 << 20)
	}

	grpcDialOption := security.LoadClientTLS(v.Sub("grpc"), "master")
	ms := &MasterServer{
		option:          option,
		preallocateSize: preallocateSize,
		clientChans:     make(map[string]chan *master_pb.VolumeLocation),
		grpcDialOpiton:  grpcDialOption,
		MasterClient:    wdclient.NewMasterClient(context.Background(), grpcDialOption, "master", peers),
	}
	ms.bounedLeaderChan = make(chan int, 16)
	seq := sequence.NewMemorySequencer()
	ms.Topo = topology.NewTopology("topo", seq, uint64(ms.option.VolumeSizeLimitMB)*1024*1024, ms.option.PulseSeconds)
	ms.vg = topology.NewDefaultVolumeGrowth()
	glog.V(0).Infoln("Volume Size Limit is", ms.option.VolumeSizeLimitMB, "MB")

	ms.guard = security.NewGuard(ms.option.WhiteList, signingKey, expiresAfterSec, readSigningKey, readExpiresAfterSec)

	if !ms.option.DisableHttp {
		handleStaticResources2(r)
		r.HandleFunc("/", ms.proxyToLeader(ms.uiStatusHandler))
		r.HandleFunc("/ui/index.html", ms.uiStatusHandler)
		r.HandleFunc("/dir/assign", ms.proxyToLeader(ms.guard.WhiteList(ms.dirAssignHandler)))
		r.HandleFunc("/dir/lookup", ms.guard.WhiteList(ms.dirLookupHandler))
		r.HandleFunc("/dir/status", ms.proxyToLeader(ms.guard.WhiteList(ms.dirStatusHandler)))
		r.HandleFunc("/col/delete", ms.proxyToLeader(ms.guard.WhiteList(ms.collectionDeleteHandler)))
		r.HandleFunc("/vol/grow", ms.proxyToLeader(ms.guard.WhiteList(ms.volumeGrowHandler)))
		r.HandleFunc("/vol/status", ms.proxyToLeader(ms.guard.WhiteList(ms.volumeStatusHandler)))
		r.HandleFunc("/vol/vacuum", ms.proxyToLeader(ms.guard.WhiteList(ms.volumeVacuumHandler)))
		r.HandleFunc("/submit", ms.guard.WhiteList(ms.submitFromMasterServerHandler))
		r.HandleFunc("/stats/health", ms.guard.WhiteList(statsHealthHandler))
		r.HandleFunc("/stats/counter", ms.guard.WhiteList(statsCounterHandler))
		r.HandleFunc("/stats/memory", ms.guard.WhiteList(statsMemoryHandler))
		r.HandleFunc("/{fileId}", ms.redirectHandler)
	}

	ms.Topo.StartRefreshWritableVolumes(ms.grpcDialOpiton, ms.option.GarbageThreshold, ms.preallocateSize)

	ms.startAdminScripts()

	return ms
}

func (ms *MasterServer) SetRaftServer(raftServer *RaftServer) {
	ms.Topo.RaftServer = raftServer.raftServer
	ms.Topo.RaftServer.AddEventListener(raft.LeaderChangeEventType, func(e raft.Event) {
		glog.V(0).Infof("event: %+v", e)
		if ms.Topo.RaftServer.Leader() != "" {
			glog.V(0).Infoln("[", ms.Topo.RaftServer.Name(), "]", ms.Topo.RaftServer.Leader(), "becomes leader.")
		}
	})
	ms.Topo.RaftServer.AddEventListener(raft.StateChangeEventType, func(e raft.Event) {
		glog.V(0).Infof("state change: %+v", e)
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
				writeJsonError(w, r, http.StatusInternalServerError,
					fmt.Errorf("Leader URL http://%s Parse Error: %v", ms.Topo.RaftServer.Leader(), err))
				return
			}
			glog.V(4).Infoln("proxying to leader", ms.Topo.RaftServer.Leader())
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

func (ms *MasterServer) startAdminScripts() {
	v := viper.GetViper()
	adminScripts := v.GetString("master.maintenance.scripts")
	v.SetDefault("master.maintenance.sleep_minutes", 17)
	sleepMinutes := v.GetInt("master.maintenance.sleep_minutes")

	glog.V(0).Infof("adminScripts:\n%v", adminScripts)
	if adminScripts == "" {
		return
	}

	scriptLines := strings.Split(adminScripts, "\n")

	masterAddress := "localhost:" + strconv.Itoa(ms.option.Port)

	var shellOptions shell.ShellOptions
	shellOptions.GrpcDialOption = security.LoadClientTLS(viper.Sub("grpc"), "master")
	shellOptions.Masters = &masterAddress
	shellOptions.FilerHost = "localhost"
	shellOptions.FilerPort = 8888
	shellOptions.Directory = "/"

	commandEnv := shell.NewCommandEnv(shellOptions)

	reg, _ := regexp.Compile(`'.*?'|".*?"|\S+`)

	go commandEnv.MasterClient.KeepConnectedToMaster()

	go func() {
		commandEnv.MasterClient.WaitUntilConnected()

		c := time.Tick(time.Duration(sleepMinutes) * time.Minute)
		for _ = range c {
			if ms.Topo.IsLeader() {
				for _, line := range scriptLines {

					cmds := reg.FindAllString(line, -1)
					if len(cmds) == 0 {
						continue
					}
					args := make([]string, len(cmds[1:]))
					for i := range args {
						args[i] = strings.Trim(string(cmds[1+i]), "\"'")
					}
					cmd := strings.ToLower(cmds[0])

					for _, c := range shell.Commands {
						if c.Name() == cmd {
							glog.V(0).Infof("executing: %s %v", cmd, args)
							if err := c.Do(args, commandEnv, os.Stdout); err != nil {
								glog.V(0).Infof("error: %v", err)
							}
						}
					}
				}
			}
		}
	}()
}
