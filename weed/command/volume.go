package command

import (
	"fmt"
	"net/http"
	httppprof "net/http/pprof"
	"os"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/storage/types"

	"github.com/spf13/viper"
	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/util/grace"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/server/constants"
	"github.com/seaweedfs/seaweedfs/weed/util/httpdown"

	"google.golang.org/grpc/reflection"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	weed_server "github.com/seaweedfs/seaweedfs/weed/server"
	stats_collect "github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var (
	v VolumeServerOptions
)

type VolumeServerOptions struct {
	port                      *int
	portGrpc                  *int
	publicPort                *int
	folders                   []string
	folderMaxLimits           []int32
	idxFolder                 *string
	ip                        *string
	publicUrl                 *string
	bindIp                    *string
	mastersString             *string
	masters                   []pb.ServerAddress
	idleConnectionTimeout     *int
	dataCenter                *string
	rack                      *string
	whiteList                 []string
	indexType                 *string
	diskType                  *string
	fixJpgOrientation         *bool
	readMode                  *string
	cpuProfile                *string
	memProfile                *string
	compactionMBPerSecond     *int
	fileSizeLimitMB           *int
	concurrentUploadLimitMB   *int
	concurrentDownloadLimitMB *int
	pprof                     *bool
	preStopSeconds            *int
	metricsHttpPort           *int
	metricsHttpIp             *string
	// pulseSeconds          *int
	inflightUploadDataTimeout *time.Duration
	hasSlowRead               *bool
	readBufferSizeMB          *int
	ldbTimeout                *int64
}

func init() {
	cmdVolume.Run = runVolume // break init cycle
	v.port = cmdVolume.Flag.Int("port", 8080, "http listen port")
	v.portGrpc = cmdVolume.Flag.Int("port.grpc", 0, "grpc listen port")
	v.publicPort = cmdVolume.Flag.Int("port.public", 0, "port opened to public")
	v.ip = cmdVolume.Flag.String("ip", util.DetectedHostAddress(), "ip or server name, also used as identifier")
	v.publicUrl = cmdVolume.Flag.String("publicUrl", "", "Publicly accessible address")
	v.bindIp = cmdVolume.Flag.String("ip.bind", "", "ip address to bind to. If empty, default to same as -ip option.")
	v.mastersString = cmdVolume.Flag.String("mserver", "localhost:9333", "comma-separated master servers")
	v.preStopSeconds = cmdVolume.Flag.Int("preStopSeconds", 10, "number of seconds between stop send heartbeats and stop volume server")
	// v.pulseSeconds = cmdVolume.Flag.Int("pulseSeconds", 5, "number of seconds between heartbeats, must be smaller than or equal to the master's setting")
	v.idleConnectionTimeout = cmdVolume.Flag.Int("idleTimeout", 30, "connection idle seconds")
	v.dataCenter = cmdVolume.Flag.String("dataCenter", "", "current volume server's data center name")
	v.rack = cmdVolume.Flag.String("rack", "", "current volume server's rack name")
	v.indexType = cmdVolume.Flag.String("index", "memory", "Choose [memory|leveldb|leveldbMedium|leveldbLarge] mode for memory~performance balance.")
	v.diskType = cmdVolume.Flag.String("disk", "", "[hdd|ssd|<tag>] hard drive or solid state drive or any tag")
	v.fixJpgOrientation = cmdVolume.Flag.Bool("images.fix.orientation", false, "Adjust jpg orientation when uploading.")
	v.readMode = cmdVolume.Flag.String("readMode", "proxy", "[local|proxy|redirect] how to deal with non-local volume: 'not found|proxy to remote node|redirect volume location'.")
	v.cpuProfile = cmdVolume.Flag.String("cpuprofile", "", "cpu profile output file")
	v.memProfile = cmdVolume.Flag.String("memprofile", "", "memory profile output file")
	v.compactionMBPerSecond = cmdVolume.Flag.Int("compactionMBps", 0, "limit background compaction or copying speed in mega bytes per second")
	v.fileSizeLimitMB = cmdVolume.Flag.Int("fileSizeLimitMB", 256, "limit file size to avoid out of memory")
	v.ldbTimeout = cmdVolume.Flag.Int64("index.leveldbTimeout", 0, "alive time for leveldb (default to 0). If leveldb of volume is not accessed in ldbTimeout hours, it will be off loaded to reduce opened files and memory consumption.")
	v.concurrentUploadLimitMB = cmdVolume.Flag.Int("concurrentUploadLimitMB", 256, "limit total concurrent upload size")
	v.concurrentDownloadLimitMB = cmdVolume.Flag.Int("concurrentDownloadLimitMB", 256, "limit total concurrent download size")
	v.pprof = cmdVolume.Flag.Bool("pprof", false, "enable pprof http handlers. precludes --memprofile and --cpuprofile")
	v.metricsHttpPort = cmdVolume.Flag.Int("metricsPort", 0, "Prometheus metrics listen port")
	v.metricsHttpIp = cmdVolume.Flag.String("metricsIp", "", "metrics listen ip. If empty, default to same as -ip.bind option.")
	v.idxFolder = cmdVolume.Flag.String("dir.idx", "", "directory to store .idx files")
	v.inflightUploadDataTimeout = cmdVolume.Flag.Duration("inflightUploadDataTimeout", 60*time.Second, "inflight upload data wait timeout of volume servers")
	v.hasSlowRead = cmdVolume.Flag.Bool("hasSlowRead", true, "<experimental> if true, this prevents slow reads from blocking other requests, but large file read P99 latency will increase.")
	v.readBufferSizeMB = cmdVolume.Flag.Int("readBufferSizeMB", 4, "<experimental> larger values can optimize query performance but will increase some memory usage,Use with hasSlowRead normally.")
}

var cmdVolume = &Command{
	UsageLine: "volume -port=8080 -dir=/tmp -max=5 -ip=server_name -mserver=localhost:9333",
	Short:     "start a volume server",
	Long: `start a volume server to provide storage spaces

  `,
}

var (
	volumeFolders         = cmdVolume.Flag.String("dir", os.TempDir(), "directories to store data files. dir[,dir]...")
	maxVolumeCounts       = cmdVolume.Flag.String("max", "8", "maximum numbers of volumes, count[,count]... If set to zero, the limit will be auto configured as free disk space divided by volume size.")
	volumeWhiteListOption = cmdVolume.Flag.String("whiteList", "", "comma separated Ip addresses having write permission. No limit if empty.")
	minFreeSpacePercent   = cmdVolume.Flag.String("minFreeSpacePercent", "1", "minimum free disk space (default to 1%). Low disk space will mark all volumes as ReadOnly (deprecated, use minFreeSpace instead).")
	minFreeSpace          = cmdVolume.Flag.String("minFreeSpace", "", "min free disk space (value<=100 as percentage like 1, other as human readable bytes, like 10GiB). Low disk space will mark all volumes as ReadOnly.")
)

func runVolume(cmd *Command, args []string) bool {

	util.LoadSecurityConfiguration()

	// If --pprof is set we assume the caller wants to be able to collect
	// cpu and memory profiles via go tool pprof
	if !*v.pprof {
		grace.SetupProfiling(*v.cpuProfile, *v.memProfile)
	}

	switch {
	case *v.metricsHttpIp != "":
		// noting to do, use v.metricsHttpIp
	case *v.bindIp != "":
		*v.metricsHttpIp = *v.bindIp
	case *v.ip != "":
		*v.metricsHttpIp = *v.ip
	}
	go stats_collect.StartMetricsServer(*v.metricsHttpIp, *v.metricsHttpPort)

	minFreeSpaces := util.MustParseMinFreeSpace(*minFreeSpace, *minFreeSpacePercent)
	v.masters = pb.ServerAddresses(*v.mastersString).ToAddresses()
	v.startVolumeServer(*volumeFolders, *maxVolumeCounts, *volumeWhiteListOption, minFreeSpaces)

	return true
}

func (v VolumeServerOptions) startVolumeServer(volumeFolders, maxVolumeCounts, volumeWhiteListOption string, minFreeSpaces []util.MinFreeSpace) {

	// Set multiple folders and each folder's max volume count limit'
	v.folders = strings.Split(volumeFolders, ",")
	for _, folder := range v.folders {
		if err := util.TestFolderWritable(util.ResolvePath(folder)); err != nil {
			glog.Fatalf("Check Data Folder(-dir) Writable %s : %s", folder, err)
		}
	}

	// set max
	maxCountStrings := strings.Split(maxVolumeCounts, ",")
	for _, maxString := range maxCountStrings {
		if max, e := strconv.ParseInt(maxString, 10, 64); e == nil {
			v.folderMaxLimits = append(v.folderMaxLimits, int32(max))
		} else {
			glog.Fatalf("The max specified in -max not a valid number %s", maxString)
		}
	}
	if len(v.folderMaxLimits) == 1 && len(v.folders) > 1 {
		for i := 0; i < len(v.folders)-1; i++ {
			v.folderMaxLimits = append(v.folderMaxLimits, v.folderMaxLimits[0])
		}
	}
	if len(v.folders) != len(v.folderMaxLimits) {
		glog.Fatalf("%d directories by -dir, but only %d max is set by -max", len(v.folders), len(v.folderMaxLimits))
	}

	if len(minFreeSpaces) == 1 && len(v.folders) > 1 {
		for i := 0; i < len(v.folders)-1; i++ {
			minFreeSpaces = append(minFreeSpaces, minFreeSpaces[0])
		}
	}
	if len(v.folders) != len(minFreeSpaces) {
		glog.Fatalf("%d directories by -dir, but only %d minFreeSpacePercent is set by -minFreeSpacePercent", len(v.folders), len(minFreeSpaces))
	}

	// set disk types
	var diskTypes []types.DiskType
	diskTypeStrings := strings.Split(*v.diskType, ",")
	for _, diskTypeString := range diskTypeStrings {
		diskTypes = append(diskTypes, types.ToDiskType(diskTypeString))
	}
	if len(diskTypes) == 1 && len(v.folders) > 1 {
		for i := 0; i < len(v.folders)-1; i++ {
			diskTypes = append(diskTypes, diskTypes[0])
		}
	}
	if len(v.folders) != len(diskTypes) {
		glog.Fatalf("%d directories by -dir, but only %d disk types is set by -disk", len(v.folders), len(diskTypes))
	}

	// security related white list configuration
	v.whiteList = util.StringSplit(volumeWhiteListOption, ",")

	if *v.ip == "" {
		*v.ip = util.DetectedHostAddress()
		glog.V(0).Infof("detected volume server ip address: %v", *v.ip)
	}
	if *v.bindIp == "" {
		*v.bindIp = *v.ip
	}

	if *v.publicPort == 0 {
		*v.publicPort = *v.port
	}
	if *v.portGrpc == 0 {
		*v.portGrpc = 10000 + *v.port
	}
	if *v.publicUrl == "" {
		*v.publicUrl = util.JoinHostPort(*v.ip, *v.publicPort)
	}

	volumeMux := http.NewServeMux()
	publicVolumeMux := volumeMux
	if v.isSeparatedPublicPort() {
		publicVolumeMux = http.NewServeMux()
	}

	if *v.pprof {
		volumeMux.HandleFunc("/debug/pprof/", httppprof.Index)
		volumeMux.HandleFunc("/debug/pprof/cmdline", httppprof.Cmdline)
		volumeMux.HandleFunc("/debug/pprof/profile", httppprof.Profile)
		volumeMux.HandleFunc("/debug/pprof/symbol", httppprof.Symbol)
		volumeMux.HandleFunc("/debug/pprof/trace", httppprof.Trace)
	}

	volumeNeedleMapKind := storage.NeedleMapInMemory
	switch *v.indexType {
	case "leveldb":
		volumeNeedleMapKind = storage.NeedleMapLevelDb
	case "leveldbMedium":
		volumeNeedleMapKind = storage.NeedleMapLevelDbMedium
	case "leveldbLarge":
		volumeNeedleMapKind = storage.NeedleMapLevelDbLarge
	}

	volumeServer := weed_server.NewVolumeServer(volumeMux, publicVolumeMux,
		*v.ip, *v.port, *v.portGrpc, *v.publicUrl,
		v.folders, v.folderMaxLimits, minFreeSpaces, diskTypes,
		*v.idxFolder,
		volumeNeedleMapKind,
		v.masters, constants.VolumePulseSeconds, *v.dataCenter, *v.rack,
		v.whiteList,
		*v.fixJpgOrientation, *v.readMode,
		*v.compactionMBPerSecond,
		*v.fileSizeLimitMB,
		int64(*v.concurrentUploadLimitMB)*1024*1024,
		int64(*v.concurrentDownloadLimitMB)*1024*1024,
		*v.inflightUploadDataTimeout,
		*v.hasSlowRead,
		*v.readBufferSizeMB,
		*v.ldbTimeout,
	)
	// starting grpc server
	grpcS := v.startGrpcService(volumeServer)

	// starting public http server
	var publicHttpDown httpdown.Server
	if v.isSeparatedPublicPort() {
		publicHttpDown = v.startPublicHttpService(publicVolumeMux)
		if nil == publicHttpDown {
			glog.Fatalf("start public http service failed")
		}
	}

	// starting the cluster http server
	clusterHttpServer := v.startClusterHttpService(volumeMux)

	grace.OnReload(volumeServer.LoadNewVolumes)
	grace.OnReload(volumeServer.Reload)

	stopChan := make(chan bool)
	grace.OnInterrupt(func() {
		fmt.Println("volume server has been killed")

		// Stop heartbeats
		if !volumeServer.StopHeartbeat() {
			volumeServer.SetStopping()
			glog.V(0).Infof("stop send heartbeat and wait %d seconds until shutdown ...", *v.preStopSeconds)
			time.Sleep(time.Duration(*v.preStopSeconds) * time.Second)
		}

		shutdown(publicHttpDown, clusterHttpServer, grpcS, volumeServer)
		stopChan <- true
	})

	select {
	case <-stopChan:
	}

}

func shutdown(publicHttpDown httpdown.Server, clusterHttpServer httpdown.Server, grpcS *grpc.Server, volumeServer *weed_server.VolumeServer) {

	// firstly, stop the public http service to prevent from receiving new user request
	if nil != publicHttpDown {
		glog.V(0).Infof("stop public http server ... ")
		if err := publicHttpDown.Stop(); err != nil {
			glog.Warningf("stop the public http server failed, %v", err)
		}
	}

	glog.V(0).Infof("graceful stop cluster http server ... ")
	if err := clusterHttpServer.Stop(); err != nil {
		glog.Warningf("stop the cluster http server failed, %v", err)
	}

	glog.V(0).Infof("graceful stop gRPC ...")
	grpcS.GracefulStop()

	volumeServer.Shutdown()

	pprof.StopCPUProfile()

}

// check whether configure the public port
func (v VolumeServerOptions) isSeparatedPublicPort() bool {
	return *v.publicPort != *v.port
}

func (v VolumeServerOptions) startGrpcService(vs volume_server_pb.VolumeServerServer) *grpc.Server {
	grpcPort := *v.portGrpc
	grpcL, err := util.NewListener(util.JoinHostPort(*v.bindIp, grpcPort), 0)
	if err != nil {
		glog.Fatalf("failed to listen on grpc port %d: %v", grpcPort, err)
	}
	grpcS := pb.NewGrpcServer(security.LoadServerTLS(util.GetViper(), "grpc.volume"))
	volume_server_pb.RegisterVolumeServerServer(grpcS, vs)
	reflection.Register(grpcS)
	go func() {
		if err := grpcS.Serve(grpcL); err != nil {
			glog.Fatalf("start gRPC service failed, %s", err)
		}
	}()
	return grpcS
}

func (v VolumeServerOptions) startPublicHttpService(handler http.Handler) httpdown.Server {
	publicListeningAddress := util.JoinHostPort(*v.bindIp, *v.publicPort)
	glog.V(0).Infoln("Start Seaweed volume server", util.Version(), "public at", publicListeningAddress)
	publicListener, e := util.NewListener(publicListeningAddress, time.Duration(*v.idleConnectionTimeout)*time.Second)
	if e != nil {
		glog.Fatalf("Volume server listener error:%v", e)
	}

	pubHttp := httpdown.HTTP{StopTimeout: 5 * time.Minute, KillTimeout: 5 * time.Minute}
	publicHttpDown := pubHttp.Serve(&http.Server{Handler: handler}, publicListener)
	go func() {
		if err := publicHttpDown.Wait(); err != nil {
			glog.Errorf("public http down wait failed, %v", err)
		}
	}()

	return publicHttpDown
}

func (v VolumeServerOptions) startClusterHttpService(handler http.Handler) httpdown.Server {
	var (
		certFile, keyFile string
	)
	if viper.GetString("https.volume.key") != "" {
		certFile = viper.GetString("https.volume.cert")
		keyFile = viper.GetString("https.volume.key")
	}

	listeningAddress := util.JoinHostPort(*v.bindIp, *v.port)
	glog.V(0).Infof("Start Seaweed volume server %s at %s", util.Version(), listeningAddress)
	listener, e := util.NewListener(listeningAddress, time.Duration(*v.idleConnectionTimeout)*time.Second)
	if e != nil {
		glog.Fatalf("Volume server listener error:%v", e)
	}

	httpDown := httpdown.HTTP{
		KillTimeout: time.Minute,
		StopTimeout: 30 * time.Second,
		CertFile:    certFile,
		KeyFile:     keyFile}
	httpS := &http.Server{Handler: handler}

	if viper.GetString("https.volume.ca") != "" {
		clientCertFile := viper.GetString("https.volume.ca")
		httpS.TLSConfig = security.LoadClientTLSHTTP(clientCertFile)
	}

	clusterHttpServer := httpDown.Serve(httpS, listener)
	go func() {
		if e := clusterHttpServer.Wait(); e != nil {
			glog.Fatalf("Volume server fail to serve: %v", e)
		}
	}()
	return clusterHttpServer
}
