package command

import (
	"net/http"
	"os"
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
	volumeSizeLimitArg      = cmdMaster.Flag.String("volumeSizeLimit", "", "Master stops directing writes to oversized volumes. (eg: 30G, 20G)")
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
	util.GoMaxProcs(mMaxCpu)
	util.SetupProfiling(*masterCpuProfile, *masterMemProfile)

	err := util.TestFolderWritable(*metaFolder)
	util.LogFatalIfError(err, "Check Meta Folder (-mdir) Writable %s : %s", *metaFolder, err)

	if *masterWhiteListOption != "" {
		masterWhiteList = strings.Split(*masterWhiteListOption, ",")
	}

	volumeSizeLimit := util.ParseVolumeSizeLimit(*volumeSizeLimitMiB, *volumeSizeLimitArg)

	r := mux.NewRouter()
	ms := weed_server.NewMasterServer(r, *mport, *metaFolder,
		volumeSizeLimit, *volumePreallocate,
		*mpulse, *defaultReplicaPlacement, *garbageThreshold,
		masterWhiteList, *masterSecureKey,
	)

	listeningAddress := *masterBindIp + ":" + strconv.Itoa(*mport)

	glog.V(0).Infoln("Start Seaweed Master", util.VERSION, "at", listeningAddress)

	masterListener, e := util.NewListener(listeningAddress, 0)
	util.LogFatalIfError(e, "Master startup error: %v", e)

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
		grpcListener, err := util.NewListener(*masterBindIp+":"+strconv.Itoa(grpcPort), 0)
		util.LogFatalIfError(err, "master failed to listen on grpc port %d: %v", grpcPort, err)

		// Create your protocol servers.
		grpcServer := util.NewGrpcServer()
		master_pb.RegisterSeaweedServer(grpcServer, ms)
		reflection.Register(grpcServer)

		glog.V(0).Infof("Start Seaweed Master %s grpc server at %s:%d", util.VERSION, *masterBindIp, grpcPort)
		grpcServer.Serve(grpcListener)
	}()

	// start http server
	httpS := &http.Server{Handler: r}
	err = httpS.Serve(masterListener)
	util.LogFatalIfError(err, "master server failed to serve: %v", err)

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
		peerCount++
	}

	util.LogFatalIf(peerCount%2 == 0, "Only odd number of masters are supported!")

	return
}
