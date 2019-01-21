package command

import (
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/server"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/gorilla/mux"
	"google.golang.org/grpc/reflection"
)

func init() {
	cmdMaster.Run = runMaster // break init cycle
}

var cmdMaster = &Command{
	UsageLine: "master -port=9333",
	Short:     "start a master server",
	Long: `start a master server to provide volume=>location mapping service
  and sequence number of file ids

  `,
}

var (
	mport                   = cmdMaster.Flag.Int("port", 9333, "http listen port")
	mGrpcPort               = cmdMaster.Flag.Int("port.grpc", 0, "grpc server listen port, default to http port + 10000")
	masterIp                = cmdMaster.Flag.String("ip", "localhost", "master <ip>|<server> address")
	masterBindIp            = cmdMaster.Flag.String("ip.bind", "0.0.0.0", "ip address to bind to")
	metaFolder              = cmdMaster.Flag.String("mdir", os.TempDir(), "data directory to store meta data")
	masterPeers             = cmdMaster.Flag.String("peers", "", "all master nodes in comma separated ip:port list, example: 127.0.0.1:9093,127.0.0.1:9094")
	volumeSizeLimitMiB      = cmdMaster.Flag.Uint("volumeSizeLimitMB", 30*1000, "Master stops directing writes to oversized volumes. (MiB)")
	volumePreallocate       = cmdMaster.Flag.Bool("volumePreallocate", false, "Preallocate disk space for volumes.")
	mpulse                  = cmdMaster.Flag.Int("pulseSeconds", 5, "number of seconds between heartbeats")
	defaultReplicaPlacement = cmdMaster.Flag.String("defaultReplication", "000", "Default replication type if not specified.")
	// mTimeout                = cmdMaster.Flag.Int("idleTimeout", 30, "connection idle seconds")
	mMaxCpu               = cmdMaster.Flag.Int("maxCpu", 0, "maximum number of CPUs. 0 means all available CPUs")
	garbageThreshold      = cmdMaster.Flag.Float64("garbageThreshold", 0.3, "threshold to vacuum and reclaim spaces")
	masterWhiteListOption = cmdMaster.Flag.String("whiteList", "", "comma separated Ip addresses having write permission. No limit if empty.")
	masterSecureKey       = cmdMaster.Flag.String("secure.secret", "", "secret to encrypt Json Web Token(JWT)")
	masterCpuProfile      = cmdMaster.Flag.String("cpuprofile", "", "cpu profile output file")
	masterMemProfile      = cmdMaster.Flag.String("memprofile", "", "memory profile output file")

	masterWhiteList []string
)

func runMaster(cmd *Command, args []string) bool {
	if *mMaxCpu < 1 {
		*mMaxCpu = runtime.NumCPU()
	}
	runtime.GOMAXPROCS(*mMaxCpu)
	util.SetupProfiling(*masterCpuProfile, *masterMemProfile)

	if err := util.TestFolderWritable(*metaFolder); err != nil {
		glog.Fatalf("Check Meta Folder (-mdir) Writable %s : %s", *metaFolder, err)
	}
	if *masterWhiteListOption != "" {
		masterWhiteList = strings.Split(*masterWhiteListOption, ",")
	}
	if *volumeSizeLimitMiB > 30*1000 {
		glog.Fatalf("volumeSizeLimitMB should be smaller than 30000")
	}

	r := mux.NewRouter()
	ms := weed_server.NewMasterServer(r, *mport, *metaFolder,
		*volumeSizeLimitMiB, *volumePreallocate,
		*mpulse, *defaultReplicaPlacement, *garbageThreshold,
		masterWhiteList, *masterSecureKey,
	)

	listeningAddress := *masterBindIp + ":" + strconv.Itoa(*mport)

	glog.V(0).Infoln("Start Seaweed Master", util.VERSION, "at", listeningAddress)

	masterListener, e := util.NewListener(listeningAddress, 0)
	if e != nil {
		glog.Fatalf("Master startup error: %v", e)
	}

	go func() {
		time.Sleep(100 * time.Millisecond)
		myMasterAddress, peers := checkPeers(*masterIp, *mport, *masterPeers)
		raftServer := weed_server.NewRaftServer(r, peers, myMasterAddress, *metaFolder, ms.Topo, *mpulse)
		ms.SetRaftServer(raftServer)
	}()

	go func() {
		// starting grpc server
		grpcPort := *mGrpcPort
		if grpcPort == 0 {
			grpcPort = *mport + 10000
		}
		grpcL, err := util.NewListener(*masterBindIp+":"+strconv.Itoa(grpcPort), 0)
		if err != nil {
			glog.Fatalf("master failed to listen on grpc port %d: %v", grpcPort, err)
		}
		// Create your protocol servers.
		grpcS := util.NewGrpcServer()
		master_pb.RegisterSeaweedServer(grpcS, ms)
		reflection.Register(grpcS)

		glog.V(0).Infof("Start Seaweed Master %s grpc server at %s:%d", util.VERSION, *masterBindIp, grpcPort)
		grpcS.Serve(grpcL)
	}()

	// start http server
	httpS := &http.Server{Handler: r}
	if err := httpS.Serve(masterListener); err != nil {
		glog.Fatalf("master server failed to serve: %v", err)
	}

	return true
}

func checkPeers(masterIp string, masterPort int, peers string) (masterAddress string, cleanedPeers []string) {
	masterAddress = masterIp + ":" + strconv.Itoa(masterPort)
	if peers != "" {
		cleanedPeers = strings.Split(peers, ",")
	}

	hasSelf := false
	for _, peer := range cleanedPeers {
		if peer == masterAddress {
			hasSelf = true
			break
		}
	}

	peerCount := len(cleanedPeers)
	if !hasSelf {
		peerCount += 1
	}
	if peerCount%2 == 0 {
		glog.Fatalf("Only odd number of masters are supported!")
	}
	return
}
