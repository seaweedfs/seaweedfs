package command

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util/version"

	hashicorpRaft "github.com/hashicorp/raft"

	"slices"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/raft/protobuf"
	"github.com/spf13/viper"
	"google.golang.org/grpc/reflection"

	stats_collect "github.com/seaweedfs/seaweedfs/weed/stats"

	"github.com/seaweedfs/seaweedfs/weed/util/grace"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	weed_server "github.com/seaweedfs/seaweedfs/weed/server"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var (
	m MasterOptions
)

const (
	raftJoinCheckDelay = 1500 * time.Millisecond // delay before checking if we should join a raft cluster
)

type MasterOptions struct {
	port                       *int
	portGrpc                   *int
	ip                         *string
	ipBind                     *string
	metaFolder                 *string
	peers                      *string
	mastersDeprecated          *string // deprecated, for backward compatibility in master.follower
	volumeSizeLimitMB          *uint
	volumePreallocate          *bool
	maxParallelVacuumPerServer *int
	// pulseSeconds       *int
	defaultReplication *string
	garbageThreshold   *float64
	whiteList          *string
	disableHttp        *bool
	metricsAddress     *string
	metricsIntervalSec *int
	raftResumeState    *bool
	metricsHttpPort    *int
	metricsHttpIp      *string
	heartbeatInterval  *time.Duration
	electionTimeout    *time.Duration
	raftHashicorp      *bool
	raftBootstrap      *bool
	telemetryUrl       *string
	telemetryEnabled   *bool
	debug              *bool
	debugPort          *int
}

func init() {
	cmdMaster.Run = runMaster // break init cycle
	m.port = cmdMaster.Flag.Int("port", 9333, "http listen port")
	m.portGrpc = cmdMaster.Flag.Int("port.grpc", 0, "grpc listen port")
	m.ip = cmdMaster.Flag.String("ip", util.DetectedHostAddress(), "master <ip>|<server> address, also used as identifier")
	m.ipBind = cmdMaster.Flag.String("ip.bind", "", "ip address to bind to. If empty, default to same as -ip option.")
	m.metaFolder = cmdMaster.Flag.String("mdir", os.TempDir(), "data directory to store meta data")
	m.peers = cmdMaster.Flag.String("peers", "", "all master nodes in comma separated ip:port list, example: 127.0.0.1:9093,127.0.0.1:9094,127.0.0.1:9095; use 'none' for single-master mode")
	m.volumeSizeLimitMB = cmdMaster.Flag.Uint("volumeSizeLimitMB", 30*1000, "Master stops directing writes to oversized volumes.")
	m.volumePreallocate = cmdMaster.Flag.Bool("volumePreallocate", false, "Preallocate disk space for volumes.")
	m.maxParallelVacuumPerServer = cmdMaster.Flag.Int("maxParallelVacuumPerServer", 1, "maximum number of volumes to vacuum in parallel per volume server")
	// m.pulseSeconds = cmdMaster.Flag.Int("pulseSeconds", 5, "number of seconds between heartbeats")
	m.defaultReplication = cmdMaster.Flag.String("defaultReplication", "", "Default replication type if not specified.")
	m.garbageThreshold = cmdMaster.Flag.Float64("garbageThreshold", 0.3, "threshold to vacuum and reclaim spaces")
	m.whiteList = cmdMaster.Flag.String("whiteList", "", "comma separated Ip addresses having write permission. No limit if empty.")
	m.disableHttp = cmdMaster.Flag.Bool("disableHttp", false, "disable http requests, only gRPC operations are allowed.")
	m.metricsAddress = cmdMaster.Flag.String("metrics.address", "", "Prometheus gateway address <host>:<port>")
	m.metricsIntervalSec = cmdMaster.Flag.Int("metrics.intervalSeconds", 15, "Prometheus push interval in seconds")
	m.metricsHttpPort = cmdMaster.Flag.Int("metricsPort", 0, "Prometheus metrics listen port")
	m.metricsHttpIp = cmdMaster.Flag.String("metricsIp", "", "metrics listen ip. If empty, default to same as -ip.bind option.")
	m.raftResumeState = cmdMaster.Flag.Bool("resumeState", false, "resume previous state on start master server")
	m.heartbeatInterval = cmdMaster.Flag.Duration("heartbeatInterval", 300*time.Millisecond, "heartbeat interval of master servers, and will be randomly multiplied by [1, 1.25)")
	m.electionTimeout = cmdMaster.Flag.Duration("electionTimeout", 10*time.Second, "election timeout of master servers")
	m.raftHashicorp = cmdMaster.Flag.Bool("raftHashicorp", false, "use hashicorp raft")
	m.raftBootstrap = cmdMaster.Flag.Bool("raftBootstrap", false, "Whether to bootstrap the Raft cluster")
	m.telemetryUrl = cmdMaster.Flag.String("telemetry.url", "https://telemetry.seaweedfs.com/api/collect", "telemetry server URL to send usage statistics")
	m.telemetryEnabled = cmdMaster.Flag.Bool("telemetry", false, "enable telemetry reporting")
	m.debug = cmdMaster.Flag.Bool("debug", false, "serves runtime profiling data via pprof on the port specified by -debug.port")
	m.debugPort = cmdMaster.Flag.Int("debug.port", 6060, "http port for debugging")
}

var cmdMaster = &Command{
	UsageLine: "master -port=9333",
	Short:     "start a master server",
	Long: `start a master server to provide volume=>location mapping service and sequence number of file ids

	The configuration file "security.toml" is read from ".", "$HOME/.seaweedfs/", "/usr/local/etc/seaweedfs/", or "/etc/seaweedfs/", in that order.

	The example security.toml configuration file can be generated by "weed scaffold -config=security"

	For single-master setups, use -peers=none to skip Raft quorum wait and enable instant startup.
	This is ideal for development or standalone deployments.

  `,
}

var (
	masterCpuProfile = cmdMaster.Flag.String("cpuprofile", "", "cpu profile output file")
	masterMemProfile = cmdMaster.Flag.String("memprofile", "", "memory profile output file")
)

func runMaster(cmd *Command, args []string) bool {
	if *m.debug {
		grace.StartDebugServer(*m.debugPort)
	}

	util.LoadSecurityConfiguration()
	util.LoadConfiguration("master", false)

	// bind viper configuration to command line flags
	if v := util.GetViper().GetString("master.mdir"); v != "" {
		*m.metaFolder = v
	}

	grace.SetupProfiling(*masterCpuProfile, *masterMemProfile)

	parent, _ := util.FullPath(*m.metaFolder).DirAndName()
	if util.FileExists(string(parent)) && !util.FileExists(*m.metaFolder) {
		os.MkdirAll(*m.metaFolder, 0755)
	}
	if err := util.TestFolderWritable(util.ResolvePath(*m.metaFolder)); err != nil {
		glog.Fatalf("Check Meta Folder (-mdir) Writable %s : %s", *m.metaFolder, err)
	}

	masterWhiteList := util.StringSplit(*m.whiteList, ",")
	if *m.volumeSizeLimitMB > util.VolumeSizeLimitGB*1000 {
		glog.Fatalf("volumeSizeLimitMB should be smaller than 30000")
	}

	switch {
	case *m.metricsHttpIp != "":
		// noting to do, use m.metricsHttpIp
	case *m.ipBind != "":
		*m.metricsHttpIp = *m.ipBind
	case *m.ip != "":
		*m.metricsHttpIp = *m.ip
	}
	go stats_collect.StartMetricsServer(*m.metricsHttpIp, *m.metricsHttpPort)
	go stats_collect.LoopPushingMetric("masterServer", util.JoinHostPort(*m.ip, *m.port), *m.metricsAddress, *m.metricsIntervalSec)
	startMaster(m, masterWhiteList)
	return true
}

func startMaster(masterOption MasterOptions, masterWhiteList []string) {

	backend.LoadConfiguration(util.GetViper())

	if *masterOption.portGrpc == 0 {
		*masterOption.portGrpc = 10000 + *masterOption.port
	}
	if *masterOption.ipBind == "" {
		*masterOption.ipBind = *masterOption.ip
	}

	myMasterAddress, peers := checkPeers(*masterOption.ip, *masterOption.port, *masterOption.portGrpc, *masterOption.peers)

	masterPeers := make(map[string]pb.ServerAddress)
	for _, peer := range peers {
		masterPeers[string(peer)] = peer
	}

	r := mux.NewRouter()
	ms := weed_server.NewMasterServer(r, masterOption.toMasterOption(masterWhiteList), masterPeers)
	listeningAddress := util.JoinHostPort(*masterOption.ipBind, *masterOption.port)
	glog.V(0).Infof("Start Seaweed Master %s at %s", version.Version(), listeningAddress)
	masterListener, masterLocalListener, e := util.NewIpAndLocalListeners(*masterOption.ipBind, *masterOption.port, 0)
	if e != nil {
		glog.Fatalf("Master startup error: %v", e)
	}

	// start raftServer
	metaDir := path.Join(*masterOption.metaFolder, fmt.Sprintf("m%d", *masterOption.port))

	isSingleMaster := isSingleMasterMode(*masterOption.peers)

	raftServerOption := &weed_server.RaftServerOption{
		GrpcDialOption:    security.LoadClientTLS(util.GetViper(), "grpc.master"),
		Peers:             masterPeers,
		ServerAddr:        myMasterAddress,
		DataDir:           util.ResolvePath(metaDir),
		Topo:              ms.Topo,
		RaftResumeState:   *masterOption.raftResumeState,
		HeartbeatInterval: *masterOption.heartbeatInterval,
		ElectionTimeout:   *masterOption.electionTimeout,
		RaftBootstrap:     *masterOption.raftBootstrap,
	}
	var raftServer *weed_server.RaftServer
	var err error
	if *masterOption.raftHashicorp {
		if raftServer, err = weed_server.NewHashicorpRaftServer(raftServerOption); err != nil {
			glog.Fatalf("NewHashicorpRaftServer: %s", err)
		}
	} else {
		raftServer, err = weed_server.NewRaftServer(raftServerOption)
		if raftServer == nil {
			glog.Fatalf("please verify %s is writable, see https://github.com/seaweedfs/seaweedfs/issues/717: %s", *masterOption.metaFolder, err)
		}
		// For single-master mode, initialize cluster immediately without waiting
		if isSingleMaster {
			glog.V(0).Infof("Single-master mode: initializing cluster immediately")
			raftServer.DoJoinCommand()
		}
	}
	ms.SetRaftServer(raftServer)
	r.HandleFunc("/cluster/status", raftServer.StatusHandler).Methods(http.MethodGet, http.MethodHead)
	r.HandleFunc("/cluster/healthz", raftServer.HealthzHandler).Methods(http.MethodGet, http.MethodHead)
	if *masterOption.raftHashicorp {
		r.HandleFunc("/raft/stats", raftServer.StatsRaftHandler).Methods(http.MethodGet)
	}
	// starting grpc server
	grpcPort := *masterOption.portGrpc
	grpcL, grpcLocalL, err := util.NewIpAndLocalListeners(*masterOption.ipBind, grpcPort, 0)
	if err != nil {
		glog.Fatalf("master failed to listen on grpc port %d: %v", grpcPort, err)
	}
	grpcS := pb.NewGrpcServer(security.LoadServerTLS(util.GetViper(), "grpc.master"))
	master_pb.RegisterSeaweedServer(grpcS, ms)
	if *masterOption.raftHashicorp {
		raftServer.TransportManager.Register(grpcS)
	} else {
		protobuf.RegisterRaftServer(grpcS, raftServer)
	}
	reflection.Register(grpcS)
	glog.V(0).Infof("Start Seaweed Master %s grpc server at %s:%d", version.Version(), *masterOption.ipBind, grpcPort)
	if grpcLocalL != nil {
		go grpcS.Serve(grpcLocalL)
	}
	go grpcS.Serve(grpcL)

	// For multi-master mode with non-Hashicorp raft, wait and check if we should join
	if !*masterOption.raftHashicorp && !isSingleMaster {
		go func() {
			time.Sleep(raftJoinCheckDelay)

			ms.Topo.RaftServerAccessLock.RLock()
			isEmptyMaster := ms.Topo.RaftServer.Leader() == "" && ms.Topo.RaftServer.IsLogEmpty()
			if isEmptyMaster && isTheFirstOne(myMasterAddress, peers) && ms.MasterClient.FindLeaderFromOtherPeers(myMasterAddress) == "" {
				raftServer.DoJoinCommand()
			}
			ms.Topo.RaftServerAccessLock.RUnlock()
		}()
	}

	go ms.MasterClient.KeepConnectedToMaster(context.Background())

	// start http server
	var (
		clientCertFile,
		certFile,
		keyFile string
	)
	useTLS := false
	useMTLS := false

	if viper.GetString("https.master.key") != "" {
		useTLS = true
		certFile = viper.GetString("https.master.cert")
		keyFile = viper.GetString("https.master.key")
	}

	if viper.GetString("https.master.ca") != "" {
		useMTLS = true
		clientCertFile = viper.GetString("https.master.ca")
	}

	if masterLocalListener != nil {
		go newHttpServer(r, nil).Serve(masterLocalListener)
	}

	var tlsConfig *tls.Config
	if useMTLS {
		tlsConfig = security.LoadClientTLSHTTP(clientCertFile)
		security.FixTlsConfig(util.GetViper(), tlsConfig)
	}

	if useTLS {
		go newHttpServer(r, tlsConfig).ServeTLS(masterListener, certFile, keyFile)
	} else {
		go newHttpServer(r, nil).Serve(masterListener)
	}

	grace.OnInterrupt(ms.Shutdown)
	grace.OnInterrupt(grpcS.Stop)
	grace.OnReload(func() {
		if ms.Topo.HashicorpRaft != nil && ms.Topo.HashicorpRaft.State() == hashicorpRaft.Leader {
			ms.Topo.HashicorpRaft.LeadershipTransfer()
		}
	})
	ctx := MiniClusterCtx
	if ctx != nil {
		<-ctx.Done()
		ms.Shutdown()
		grpcS.Stop()
	} else {
		select {}
	}
}

func isSingleMasterMode(peers string) bool {
	p := strings.ToLower(strings.TrimSpace(peers))
	return p == "none"
}

func checkPeers(masterIp string, masterPort int, masterGrpcPort int, peers string) (masterAddress pb.ServerAddress, cleanedPeers []pb.ServerAddress) {
	glog.V(0).Infof("current: %s:%d peers:%s", masterIp, masterPort, peers)
	masterAddress = pb.NewServerAddress(masterIp, masterPort, masterGrpcPort)

	// Handle special case: -peers=none for single-master setup
	if isSingleMasterMode(peers) {
		glog.V(0).Infof("Running in single-master mode (peers=none), no quorum required")
		cleanedPeers = []pb.ServerAddress{masterAddress}
		return
	}

	peers = strings.TrimSpace(peers)
	seenPeers := make(map[string]struct{})
	for _, peer := range pb.ServerAddresses(peers).ToAddresses() {
		normalizedPeer := normalizeMasterPeerAddress(peer, masterAddress)
		key := string(normalizedPeer)
		if _, found := seenPeers[key]; found {
			continue
		}
		seenPeers[key] = struct{}{}
		cleanedPeers = append(cleanedPeers, normalizedPeer)
	}

	hasSelf := false
	for _, peer := range cleanedPeers {
		if peer.ToHttpAddress() == masterAddress.ToHttpAddress() {
			hasSelf = true
			break
		}
	}

	if !hasSelf {
		cleanedPeers = append(cleanedPeers, masterAddress)
	}
	if len(cleanedPeers)%2 == 0 {
		glog.Fatalf("Only odd number of masters are supported: %+v", cleanedPeers)
	}
	return
}

func normalizeMasterPeerAddress(peer pb.ServerAddress, self pb.ServerAddress) pb.ServerAddress {
	if peer.ToHttpAddress() == self.ToHttpAddress() {
		return self
	}

	_, grpcPort, err := net.SplitHostPort(peer.ToGrpcAddress())
	if err != nil {
		return peer
	}
	grpcPortValue, err := strconv.Atoi(grpcPort)
	if err != nil {
		return peer
	}

	return pb.NewServerAddressWithGrpcPort(peer.ToHttpAddress(), grpcPortValue)
}

func isTheFirstOne(self pb.ServerAddress, peers []pb.ServerAddress) bool {
	slices.SortFunc(peers, func(a, b pb.ServerAddress) int {
		return strings.Compare(a.ToHttpAddress(), b.ToHttpAddress())
	})
	if len(peers) <= 0 {
		return true
	}
	return self.ToHttpAddress() == peers[0].ToHttpAddress()
}

func (m *MasterOptions) toMasterOption(whiteList []string) *weed_server.MasterOption {
	masterAddress := pb.NewServerAddress(*m.ip, *m.port, *m.portGrpc)
	return &weed_server.MasterOption{
		Master:                     masterAddress,
		MetaFolder:                 *m.metaFolder,
		VolumeSizeLimitMB:          uint32(*m.volumeSizeLimitMB),
		VolumePreallocate:          *m.volumePreallocate,
		MaxParallelVacuumPerServer: *m.maxParallelVacuumPerServer,
		// PulseSeconds:            *m.pulseSeconds,
		DefaultReplicaPlacement: *m.defaultReplication,
		GarbageThreshold:        *m.garbageThreshold,
		WhiteList:               whiteList,
		DisableHttp:             *m.disableHttp,
		MetricsAddress:          *m.metricsAddress,
		MetricsIntervalSec:      *m.metricsIntervalSec,
		TelemetryUrl:            *m.telemetryUrl,
		TelemetryEnabled:        *m.telemetryEnabled,
	}
}
