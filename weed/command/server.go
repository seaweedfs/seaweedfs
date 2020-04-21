package command

import (
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/util"
)

type ServerOptions struct {
	cpuprofile *string
	v          VolumeServerOptions
}

var (
	serverOptions ServerOptions
	masterOptions MasterOptions
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
	serverIp                  = cmdServer.Flag.String("ip", util.DetectedHostAddress(), "ip or server name")
	serverBindIp              = cmdServer.Flag.String("ip.bind", "0.0.0.0", "ip address to bind to")
	serverTimeout             = cmdServer.Flag.Int("idleTimeout", 30, "connection idle seconds")
	serverDataCenter          = cmdServer.Flag.String("dataCenter", "", "current volume server's data center name")
	serverRack                = cmdServer.Flag.String("rack", "", "current volume server's rack name")
	serverWhiteListOption     = cmdServer.Flag.String("whiteList", "", "comma separated Ip addresses having write permission. No limit if empty.")
	serverDisableHttp         = cmdServer.Flag.Bool("disableHttp", false, "disable http requests, only gRPC operations are allowed.")
	volumeDataFolders         = cmdServer.Flag.String("dir", os.TempDir(), "directories to store data files. dir[,dir]...")
	volumeMaxDataVolumeCounts = cmdServer.Flag.String("volume.max", "7", "maximum numbers of volumes, count[,count]... If set to zero on non-windows OS, the limit will be auto configured.")
	pulseSeconds              = cmdServer.Flag.Int("pulseSeconds", 5, "number of seconds between heartbeats")
	isStartingFiler           = cmdServer.Flag.Bool("filer", false, "whether to start filer")
	isStartingS3              = cmdServer.Flag.Bool("s3", false, "whether to start S3 gateway")

	serverWhiteList []string
)

func init() {
	serverOptions.cpuprofile = cmdServer.Flag.String("cpuprofile", "", "cpu profile output file")

	masterOptions.port = cmdServer.Flag.Int("master.port", 9333, "master server http listen port")
	masterOptions.metaFolder = cmdServer.Flag.String("master.dir", "", "data directory to store meta data, default to same as -dir specified")
	masterOptions.peers = cmdServer.Flag.String("master.peers", "", "all master nodes in comma separated ip:masterPort list")
	masterOptions.volumeSizeLimitMB = cmdServer.Flag.Uint("master.volumeSizeLimitMB", 30*1000, "Master stops directing writes to oversized volumes.")
	masterOptions.volumePreallocate = cmdServer.Flag.Bool("master.volumePreallocate", false, "Preallocate disk space for volumes.")
	masterOptions.defaultReplication = cmdServer.Flag.String("master.defaultReplication", "000", "Default replication type if not specified.")
	masterOptions.garbageThreshold = cmdServer.Flag.Float64("garbageThreshold", 0.3, "threshold to vacuum and reclaim spaces")
	masterOptions.metricsAddress = cmdServer.Flag.String("metrics.address", "", "Prometheus gateway address")
	masterOptions.metricsIntervalSec = cmdServer.Flag.Int("metrics.intervalSeconds", 15, "Prometheus push interval in seconds")

	filerOptions.collection = cmdServer.Flag.String("filer.collection", "", "all data will be stored in this collection")
	filerOptions.port = cmdServer.Flag.Int("filer.port", 8888, "filer server http listen port")
	filerOptions.publicPort = cmdServer.Flag.Int("filer.port.public", 0, "filer server public http listen port")
	filerOptions.defaultReplicaPlacement = cmdServer.Flag.String("filer.defaultReplicaPlacement", "", "Default replication type if not specified during runtime.")
	filerOptions.disableDirListing = cmdServer.Flag.Bool("filer.disableDirListing", false, "turn off directory listing")
	filerOptions.maxMB = cmdServer.Flag.Int("filer.maxMB", 32, "split files larger than the limit")
	filerOptions.dirListingLimit = cmdServer.Flag.Int("filer.dirListLimit", 1000, "limit sub dir listing size")
	filerOptions.cipher = cmdServer.Flag.Bool("filer.encryptVolumeData", false, "encrypt data on volume servers")

	serverOptions.v.port = cmdServer.Flag.Int("volume.port", 8080, "volume server http listen port")
	serverOptions.v.publicPort = cmdServer.Flag.Int("volume.port.public", 0, "volume server public port")
	serverOptions.v.indexType = cmdServer.Flag.String("volume.index", "memory", "Choose [memory|leveldb|leveldbMedium|leveldbLarge] mode for memory~performance balance.")
	serverOptions.v.fixJpgOrientation = cmdServer.Flag.Bool("volume.images.fix.orientation", false, "Adjust jpg orientation when uploading.")
	serverOptions.v.readRedirect = cmdServer.Flag.Bool("volume.read.redirect", true, "Redirect moved or non-local volumes.")
	serverOptions.v.compactionMBPerSecond = cmdServer.Flag.Int("volume.compactionMBps", 0, "limit compaction speed in mega bytes per second")
	serverOptions.v.fileSizeLimitMB = cmdServer.Flag.Int("volume.fileSizeLimitMB", 256, "limit file size to avoid out of memory")
	serverOptions.v.publicUrl = cmdServer.Flag.String("volume.publicUrl", "", "publicly accessible address")

	s3Options.port = cmdServer.Flag.Int("s3.port", 8333, "s3 server http listen port")
	s3Options.domainName = cmdServer.Flag.String("s3.domainName", "", "suffix of the host name, {bucket}.{domainName}")
	s3Options.tlsPrivateKey = cmdServer.Flag.String("s3.key.file", "", "path to the TLS private key file")
	s3Options.tlsCertificate = cmdServer.Flag.String("s3.cert.file", "", "path to the TLS certificate file")
	s3Options.config = cmdServer.Flag.String("s3.config", "", "path to the config file")

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

	if *isStartingS3 {
		*isStartingFiler = true
	}

	_, peerList := checkPeers(*serverIp, *masterOptions.port, *masterOptions.peers)
	peers := strings.Join(peerList, ",")
	masterOptions.peers = &peers

	masterOptions.ip = serverIp
	masterOptions.ipBind = serverBindIp
	filerOptions.masters = &peers
	filerOptions.ip = serverIp
	filerOptions.bindIp = serverBindIp
	serverOptions.v.ip = serverIp
	serverOptions.v.bindIp = serverBindIp
	serverOptions.v.masters = &peers
	serverOptions.v.idleConnectionTimeout = serverTimeout
	serverOptions.v.dataCenter = serverDataCenter
	serverOptions.v.rack = serverRack

	serverOptions.v.pulseSeconds = pulseSeconds
	masterOptions.pulseSeconds = pulseSeconds

	masterOptions.whiteList = serverWhiteListOption

	filerOptions.dataCenter = serverDataCenter
	filerOptions.disableHttp = serverDisableHttp
	masterOptions.disableHttp = serverDisableHttp

	filerAddress := fmt.Sprintf("%s:%d", *serverIp, *filerOptions.port)
	s3Options.filer = &filerAddress

	if *filerOptions.defaultReplicaPlacement == "" {
		*filerOptions.defaultReplicaPlacement = *masterOptions.defaultReplication
	}

	runtime.GOMAXPROCS(runtime.NumCPU())

	folders := strings.Split(*volumeDataFolders, ",")

	if *masterOptions.volumeSizeLimitMB > util.VolumeSizeLimitGB*1000 {
		glog.Fatalf("masterVolumeSizeLimitMB should be less than 30000")
	}

	if *masterOptions.metaFolder == "" {
		*masterOptions.metaFolder = folders[0]
	}
	if err := util.TestFolderWritable(*masterOptions.metaFolder); err != nil {
		glog.Fatalf("Check Meta Folder (-mdir=\"%s\") Writable: %s", *masterOptions.metaFolder, err)
	}
	filerOptions.defaultLevelDbDirectory = masterOptions.metaFolder

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

	// start volume server
	{
		go serverOptions.v.startVolumeServer(*volumeDataFolders, *volumeMaxDataVolumeCounts, *serverWhiteListOption)
	}

	startMaster(masterOptions, serverWhiteList)

	return true
}
