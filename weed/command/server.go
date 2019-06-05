package command

import (
	"fmt"
	"net/http"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/chrislusf/raft/protobuf"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/spf13/viper"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/server"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/gorilla/mux"
	"google.golang.org/grpc/reflection"
)

type ServerOptions struct {
	cpuprofile *string
	v          VolumeServerOptions
}

var (
	serverOptions ServerOptions
	filerOptions  FilerOptions
	s3Options     S3Options
)

func init() {
	cmdServer.Run = runServer // break init cycle
}

var cmdServer = &Command{
	UsageLine: "server -port=8080 -dir=/tmp -volume.max=5 -ip=server_name",
	Short:     "start a master server, a volume server, and optionally a filer and a S3 gateway",
	Long: `start both a volume server to provide storage spaces
  and a master server to provide volume=>location mapping service and sequence number of file ids

  This is provided as a convenient way to start both volume server and master server.
  The servers acts exactly the same as starting them separately.
  So other volume servers can connect to this master server also.

  Optionally, a filer server can be started.
  Also optionally, a S3 gateway can be started.

  `,
}

var (
	serverIp                      = cmdServer.Flag.String("ip", "localhost", "ip or server name")
	serverBindIp                  = cmdServer.Flag.String("ip.bind", "0.0.0.0", "ip address to bind to")
	serverMaxCpu                  = cmdServer.Flag.Int("maxCpu", 0, "maximum number of CPUs. 0 means all available CPUs")
	serverTimeout                 = cmdServer.Flag.Int("idleTimeout", 30, "connection idle seconds")
	serverDataCenter              = cmdServer.Flag.String("dataCenter", "", "current volume server's data center name")
	serverRack                    = cmdServer.Flag.String("rack", "", "current volume server's rack name")
	serverWhiteListOption         = cmdServer.Flag.String("whiteList", "", "comma separated Ip addresses having write permission. No limit if empty.")
	serverDisableHttp             = cmdServer.Flag.Bool("disableHttp", false, "disable http requests, only gRPC operations are allowed.")
	serverPeers                   = cmdServer.Flag.String("master.peers", "", "all master nodes in comma separated ip:masterPort list")
	serverGarbageThreshold        = cmdServer.Flag.Float64("garbageThreshold", 0.3, "threshold to vacuum and reclaim spaces")
	masterPort                    = cmdServer.Flag.Int("master.port", 9333, "master server http listen port")
	masterMetaFolder              = cmdServer.Flag.String("master.dir", "", "data directory to store meta data, default to same as -dir specified")
	masterVolumeSizeLimitMB       = cmdServer.Flag.Uint("master.volumeSizeLimitMB", 30*1000, "Master stops directing writes to oversized volumes.")
	masterVolumePreallocate       = cmdServer.Flag.Bool("master.volumePreallocate", false, "Preallocate disk space for volumes.")
	masterDefaultReplicaPlacement = cmdServer.Flag.String("master.defaultReplicaPlacement", "000", "Default replication type if not specified.")
	volumeDataFolders             = cmdServer.Flag.String("dir", os.TempDir(), "directories to store data files. dir[,dir]...")
	volumeMaxDataVolumeCounts     = cmdServer.Flag.String("volume.max", "7", "maximum numbers of volumes, count[,count]...")
	pulseSeconds                  = cmdServer.Flag.Int("pulseSeconds", 5, "number of seconds between heartbeats")
	isStartingFiler               = cmdServer.Flag.Bool("filer", false, "whether to start filer")
	isStartingS3                  = cmdServer.Flag.Bool("s3", false, "whether to start S3 gateway")

	serverWhiteList []string
)

func init() {
	serverOptions.cpuprofile = cmdServer.Flag.String("cpuprofile", "", "cpu profile output file")
	filerOptions.collection = cmdServer.Flag.String("filer.collection", "", "all data will be stored in this collection")
	filerOptions.port = cmdServer.Flag.Int("filer.port", 8888, "filer server http listen port")
	filerOptions.publicPort = cmdServer.Flag.Int("filer.port.public", 0, "filer server public http listen port")
	filerOptions.defaultReplicaPlacement = cmdServer.Flag.String("filer.defaultReplicaPlacement", "", "Default replication type if not specified during runtime.")
	filerOptions.redirectOnRead = cmdServer.Flag.Bool("filer.redirectOnRead", false, "whether proxy or redirect to volume server during file GET request")
	filerOptions.disableDirListing = cmdServer.Flag.Bool("filer.disableDirListing", false, "turn off directory listing")
	filerOptions.maxMB = cmdServer.Flag.Int("filer.maxMB", 32, "split files larger than the limit")
	filerOptions.dirListingLimit = cmdServer.Flag.Int("filer.dirListLimit", 1000, "limit sub dir listing size")

	serverOptions.v.port = cmdServer.Flag.Int("volume.port", 8080, "volume server http listen port")
	serverOptions.v.publicPort = cmdServer.Flag.Int("volume.port.public", 0, "volume server public port")
	serverOptions.v.indexType = cmdServer.Flag.String("volume.index", "memory", "Choose [memory|leveldb|leveldbMedium|leveldbLarge] mode for memory~performance balance.")
	serverOptions.v.fixJpgOrientation = cmdServer.Flag.Bool("volume.images.fix.orientation", false, "Adjust jpg orientation when uploading.")
	serverOptions.v.readRedirect = cmdServer.Flag.Bool("volume.read.redirect", true, "Redirect moved or non-local volumes.")
	serverOptions.v.compactionMBPerSecond = cmdServer.Flag.Int("volume.compactionMBps", 0, "limit compaction speed in mega bytes per second")
	serverOptions.v.publicUrl = cmdServer.Flag.String("volume.publicUrl", "", "publicly accessible address")

	s3Options.filerBucketsPath = cmdServer.Flag.String("s3.filer.dir.buckets", "/buckets", "folder on filer to store all buckets")
	s3Options.port = cmdServer.Flag.Int("s3.port", 8333, "s3 server http listen port")
	s3Options.domainName = cmdServer.Flag.String("s3.domainName", "", "suffix of the host name, {bucket}.{domainName}")
	s3Options.tlsPrivateKey = cmdServer.Flag.String("s3.key.file", "", "path to the TLS private key file")
	s3Options.tlsCertificate = cmdServer.Flag.String("s3.cert.file", "", "path to the TLS certificate file")

}

func runServer(cmd *Command, args []string) bool {

	util.LoadConfiguration("security", false)
	util.LoadConfiguration("master", false)

	if *serverOptions.cpuprofile != "" {
		f, err := os.Create(*serverOptions.cpuprofile)
		if err != nil {
			glog.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	if *filerOptions.redirectOnRead {
		*isStartingFiler = true
	}

	if *isStartingS3 {
		*isStartingFiler = true
	}

	master := *serverIp + ":" + strconv.Itoa(*masterPort)
	filerOptions.masters = &master
	filerOptions.ip = serverBindIp
	serverOptions.v.ip = serverIp
	serverOptions.v.bindIp = serverBindIp
	serverOptions.v.masters = &master
	serverOptions.v.idleConnectionTimeout = serverTimeout
	serverOptions.v.maxCpu = serverMaxCpu
	serverOptions.v.dataCenter = serverDataCenter
	serverOptions.v.rack = serverRack
	serverOptions.v.pulseSeconds = pulseSeconds

	filerOptions.dataCenter = serverDataCenter
	filerOptions.disableHttp = serverDisableHttp

	filerAddress := fmt.Sprintf("%s:%d", *serverIp, *filerOptions.port)
	s3Options.filer = &filerAddress

	if *filerOptions.defaultReplicaPlacement == "" {
		*filerOptions.defaultReplicaPlacement = *masterDefaultReplicaPlacement
	}

	if *serverMaxCpu < 1 {
		*serverMaxCpu = runtime.NumCPU()
	}
	runtime.GOMAXPROCS(*serverMaxCpu)

	folders := strings.Split(*volumeDataFolders, ",")

	if *masterVolumeSizeLimitMB > util.VolumeSizeLimitGB*1000 {
		glog.Fatalf("masterVolumeSizeLimitMB should be less than 30000")
	}

	if *masterMetaFolder == "" {
		*masterMetaFolder = folders[0]
	}
	if err := util.TestFolderWritable(*masterMetaFolder); err != nil {
		glog.Fatalf("Check Meta Folder (-mdir=\"%s\") Writable: %s", *masterMetaFolder, err)
	}
	filerOptions.defaultLevelDbDirectory = masterMetaFolder

	if *serverWhiteListOption != "" {
		serverWhiteList = strings.Split(*serverWhiteListOption, ",")
	}

	if *isStartingFiler {
		go func() {
			time.Sleep(1 * time.Second)

			filerOptions.startFiler()

		}()
	}

	if *isStartingS3 {
		go func() {
			time.Sleep(2 * time.Second)

			s3Options.startS3Server()

		}()
	}

	var volumeWait sync.WaitGroup

	volumeWait.Add(1)

	go func() {
		r := mux.NewRouter()
		ms := weed_server.NewMasterServer(r, *masterPort, *masterMetaFolder,
			*masterVolumeSizeLimitMB, *masterVolumePreallocate,
			*pulseSeconds, *masterDefaultReplicaPlacement, *serverGarbageThreshold,
			serverWhiteList, *serverDisableHttp,
		)

		glog.V(0).Infof("Start Seaweed Master %s at %s:%d", util.VERSION, *serverIp, *masterPort)
		masterListener, e := util.NewListener(*serverBindIp+":"+strconv.Itoa(*masterPort), 0)
		if e != nil {
			glog.Fatalf("Master startup error: %v", e)
		}

		go func() {
			// start raftServer
			myMasterAddress, peers := checkPeers(*serverIp, *masterPort, *serverPeers)
			raftServer := weed_server.NewRaftServer(security.LoadClientTLS(viper.Sub("grpc"), "master"),
				peers, myMasterAddress, *masterMetaFolder, ms.Topo, *pulseSeconds)
			ms.SetRaftServer(raftServer)
			r.HandleFunc("/cluster/status", raftServer.StatusHandler).Methods("GET")

			// starting grpc server
			grpcPort := *masterPort + 10000
			grpcL, err := util.NewListener(*serverBindIp+":"+strconv.Itoa(grpcPort), 0)
			if err != nil {
				glog.Fatalf("master failed to listen on grpc port %d: %v", grpcPort, err)
			}
			// Create your protocol servers.
			glog.V(1).Infof("grpc config %+v", viper.Sub("grpc"))
			grpcS := util.NewGrpcServer(security.LoadServerTLS(viper.Sub("grpc"), "master"))
			master_pb.RegisterSeaweedServer(grpcS, ms)
			protobuf.RegisterRaftServer(grpcS, raftServer)
			reflection.Register(grpcS)

			glog.V(0).Infof("Start Seaweed Master %s grpc server at %s:%d", util.VERSION, *serverIp, grpcPort)
			grpcS.Serve(grpcL)
		}()

		volumeWait.Done()

		// start http server
		httpS := &http.Server{Handler: r}
		if err := httpS.Serve(masterListener); err != nil {
			glog.Fatalf("master server failed to serve: %v", err)
		}

	}()

	volumeWait.Wait()
	time.Sleep(100 * time.Millisecond)

	serverOptions.v.startVolumeServer(*volumeDataFolders, *volumeMaxDataVolumeCounts, *serverWhiteListOption)

	return true
}
