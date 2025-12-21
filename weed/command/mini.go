package command

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	iam_pb "github.com/seaweedfs/seaweedfs/weed/pb/iam_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	stats_collect "github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/grace"
	"github.com/seaweedfs/seaweedfs/weed/worker"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"

	// Import task packages to trigger their auto-registration
	_ "github.com/seaweedfs/seaweedfs/weed/worker/tasks/balance"
	_ "github.com/seaweedfs/seaweedfs/weed/worker/tasks/erasure_coding"
	_ "github.com/seaweedfs/seaweedfs/weed/worker/tasks/vacuum"
)

type MiniOptions struct {
	cpuprofile *string
	memprofile *string
	debug      *bool
	debugPort  *int
	v          VolumeServerOptions
}

const (
	miniVolumeMaxDataVolumeCounts = "0" // auto-configured based on free disk space
	miniVolumeMinFreeSpace        = "1" // 1% minimum free space
	miniVolumeMinFreeSpacePercent = "1"
)

var (
	miniOptions       MiniOptions
	miniMasterOptions MasterOptions
	miniFilerOptions  FilerOptions
	miniS3Options     S3Options
	miniWebDavOptions WebDavOption
	miniAdminOptions  AdminOptions
	createdInitialIAM bool // Track if initial IAM config was created from env vars
)

func init() {
	cmdMini.Run = runMini // break init cycle
}

var cmdMini = &Command{
	UsageLine: "mini -dir=/tmp",
	Short:     "start a complete SeaweedFS setup optimized for S3 beginners and small/dev use cases",
	Long: `start a complete SeaweedFS setup with all components optimized for small/dev use cases

This command starts all components in one process (master, volume, filer,
S3 gateway, WebDAV gateway, and Admin UI).

All settings are optimized for small/dev use cases:
- Volume size limit: 128MB (small files)
- Volume max: 0 (auto-configured based on free disk space)
- Pre-stop seconds: 1 (faster shutdown)
- Master peers: none (single master mode)

This is perfect for:
- Development and testing
- Learning SeaweedFS
- Small deployments
- Local S3-compatible storage

Example Usage:
	weed mini                   # Use current directory
	weed mini -dir=/data        # Custom data directory
	weed mini -dir=/data -master.port=9444  # Custom master port

After starting, you can access:
- Master UI:    http://localhost:9333
- Volume Server: http://localhost:9340
- Filer UI:     http://localhost:8888
- S3 Endpoint:  http://localhost:8333
- WebDAV:       http://localhost:7333
- Admin UI:     http://localhost:23646

S3 Access:
The S3 endpoint is available at http://localhost:8333. For client
configuration and IAM setup, see the project documentation or use the
Admin UI (http://localhost:23646) to manage users and policies.

`,
}

var (
	miniIp                          = cmdMini.Flag.String("ip", util.DetectedHostAddress(), "ip or server name, also used as identifier")
	miniBindIp                      = cmdMini.Flag.String("ip.bind", "", "ip address to bind to. If empty, default to same as -ip option.")
	miniTimeout                     = cmdMini.Flag.Int("idleTimeout", 30, "connection idle seconds")
	miniDataCenter                  = cmdMini.Flag.String("dataCenter", "", "current volume server's data center name")
	miniRack                        = cmdMini.Flag.String("rack", "", "current volume server's rack name")
	miniWhiteListOption             = cmdMini.Flag.String("whiteList", "", "comma separated Ip addresses having write permission. No limit if empty.")
	miniDisableHttp                 = cmdMini.Flag.Bool("disableHttp", false, "disable http requests, only gRPC operations are allowed.")
	miniDataFolders                 = cmdMini.Flag.String("dir", ".", "directory to store data files")
	miniMetricsHttpPort             = cmdMini.Flag.Int("metricsPort", 0, "Prometheus metrics listen port")
	miniMetricsHttpIp               = cmdMini.Flag.String("metricsIp", "", "metrics listen ip. If empty, default to same as -ip.bind option.")
	miniS3Config                    = cmdMini.Flag.String("s3.config", "", "path to the S3 config file")
	miniIamConfig                   = cmdMini.Flag.String("s3.iam.config", "", "path to the advanced IAM config file for S3")
	miniS3AllowDeleteBucketNotEmpty = cmdMini.Flag.Bool("s3.allowDeleteBucketNotEmpty", true, "allow recursive deleting all entries along with bucket")
)

func init() {
	miniOptions.cpuprofile = cmdMini.Flag.String("cpuprofile", "", "cpu profile output file")
	miniOptions.memprofile = cmdMini.Flag.String("memprofile", "", "memory profile output file")
	miniOptions.debug = cmdMini.Flag.Bool("debug", false, "serves runtime profiling data, e.g., http://localhost:6060/debug/pprof/goroutine?debug=2")
	miniOptions.debugPort = cmdMini.Flag.Int("debug.port", 6060, "http port for debugging")

	// Master options - optimized for mini
	miniMasterOptions.port = cmdMini.Flag.Int("master.port", 9333, "master server http listen port")
	miniMasterOptions.portGrpc = cmdMini.Flag.Int("master.port.grpc", 0, "master server grpc listen port")
	miniMasterOptions.metaFolder = cmdMini.Flag.String("master.dir", "", "data directory to store meta data, default to same as -dir specified")
	miniMasterOptions.peers = cmdMini.Flag.String("master.peers", "", "all master nodes in comma separated ip:masterPort list (default: none for single master)")
	miniMasterOptions.volumeSizeLimitMB = cmdMini.Flag.Uint("master.volumeSizeLimitMB", 128, "Master stops directing writes to oversized volumes (default: 128MB for mini)")
	miniMasterOptions.volumePreallocate = cmdMini.Flag.Bool("master.volumePreallocate", false, "Preallocate disk space for volumes.")
	miniMasterOptions.maxParallelVacuumPerServer = cmdMini.Flag.Int("master.maxParallelVacuumPerServer", 1, "maximum number of volumes to vacuum in parallel on one volume server")
	miniMasterOptions.defaultReplication = cmdMini.Flag.String("master.defaultReplication", "", "Default replication type if not specified.")
	miniMasterOptions.garbageThreshold = cmdMini.Flag.Float64("master.garbageThreshold", 0.3, "threshold to vacuum and reclaim spaces")
	miniMasterOptions.metricsAddress = cmdMini.Flag.String("master.metrics.address", "", "Prometheus gateway address")
	miniMasterOptions.metricsIntervalSec = cmdMini.Flag.Int("master.metrics.intervalSeconds", 15, "Prometheus push interval in seconds")
	miniMasterOptions.raftResumeState = cmdMini.Flag.Bool("master.resumeState", false, "resume previous state on start master server")
	miniMasterOptions.heartbeatInterval = cmdMini.Flag.Duration("master.heartbeatInterval", 300*time.Millisecond, "heartbeat interval of master servers, and will be randomly multiplied by [1, 1.25)")
	miniMasterOptions.electionTimeout = cmdMini.Flag.Duration("master.electionTimeout", 10*time.Second, "election timeout of master servers")
	miniMasterOptions.raftHashicorp = cmdMini.Flag.Bool("master.raftHashicorp", false, "use hashicorp raft")
	miniMasterOptions.raftBootstrap = cmdMini.Flag.Bool("master.raftBootstrap", false, "whether to bootstrap the Raft cluster")
	miniMasterOptions.telemetryUrl = cmdMini.Flag.String("master.telemetry.url", "https://telemetry.seaweedfs.com/api/collect", "telemetry server URL")
	miniMasterOptions.telemetryEnabled = cmdMini.Flag.Bool("master.telemetry", false, "enable telemetry reporting")

	// Filer options
	miniFilerOptions.filerGroup = cmdMini.Flag.String("filer.filerGroup", "", "share metadata with other filers in the same filerGroup")
	miniFilerOptions.collection = cmdMini.Flag.String("filer.collection", "", "all data will be stored in this collection")
	miniFilerOptions.port = cmdMini.Flag.Int("filer.port", 8888, "filer server http listen port")
	miniFilerOptions.portGrpc = cmdMini.Flag.Int("filer.port.grpc", 0, "filer server grpc listen port")
	miniFilerOptions.publicPort = cmdMini.Flag.Int("filer.port.public", 0, "filer server public http listen port")
	miniFilerOptions.defaultReplicaPlacement = cmdMini.Flag.String("filer.defaultReplicaPlacement", "", "default replication type. If not specified, use master setting.")
	miniFilerOptions.disableDirListing = cmdMini.Flag.Bool("filer.disableDirListing", false, "turn off directory listing")
	miniFilerOptions.maxMB = cmdMini.Flag.Int("filer.maxMB", 4, "split files larger than the limit")
	miniFilerOptions.dirListingLimit = cmdMini.Flag.Int("filer.dirListLimit", 1000, "limit sub dir listing size")
	miniFilerOptions.cipher = cmdMini.Flag.Bool("filer.encryptVolumeData", false, "encrypt data on volume servers")
	miniFilerOptions.saveToFilerLimit = cmdMini.Flag.Int("filer.saveToFilerLimit", 0, "files smaller than this limit will be saved in filer store")
	miniFilerOptions.concurrentUploadLimitMB = cmdMini.Flag.Int("filer.concurrentUploadLimitMB", 0, "limit total concurrent upload size")
	miniFilerOptions.concurrentFileUploadLimit = cmdMini.Flag.Int("filer.concurrentFileUploadLimit", 0, "limit number of concurrent file uploads")
	miniFilerOptions.localSocket = cmdMini.Flag.String("filer.localSocket", "", "default to /tmp/seaweedfs-filer-<port>.sock")
	miniFilerOptions.showUIDirectoryDelete = cmdMini.Flag.Bool("filer.ui.deleteDir", true, "enable filer UI show delete directory button")
	miniFilerOptions.downloadMaxMBps = cmdMini.Flag.Int("filer.downloadMaxMBps", 0, "download max speed for each download request, in MB per second")
	miniFilerOptions.diskType = cmdMini.Flag.String("filer.disk", "", "[hdd|ssd|<tag>] hard drive or solid state drive or any tag")
	miniFilerOptions.allowedOrigins = cmdMini.Flag.String("filer.allowedOrigins", "*", "comma separated list of allowed origins")
	miniFilerOptions.exposeDirectoryData = cmdMini.Flag.Bool("filer.exposeDirectoryData", true, "whether to return directory metadata and content in Filer UI")
	miniFilerOptions.tusBasePath = cmdMini.Flag.String("filer.tusBasePath", "/.tus", "TUS resumable upload endpoint base path")

	// Volume options - optimized for mini
	miniOptions.v.port = cmdMini.Flag.Int("volume.port", 9340, "volume server http listen port")
	miniOptions.v.portGrpc = cmdMini.Flag.Int("volume.port.grpc", 0, "volume server grpc listen port")
	miniOptions.v.publicPort = cmdMini.Flag.Int("volume.port.public", 0, "volume server public port")
	miniOptions.v.id = cmdMini.Flag.String("volume.id", "", "volume server id. If empty, default to ip:port")
	miniOptions.v.publicUrl = cmdMini.Flag.String("volume.publicUrl", "", "publicly accessible address")
	miniOptions.v.indexType = cmdMini.Flag.String("volume.index", "memory", "Choose [memory|leveldb|leveldbMedium|leveldbLarge] mode for memory~performance balance.")
	miniOptions.v.diskType = cmdMini.Flag.String("volume.disk", "", "[hdd|ssd|<tag>] hard drive or solid state drive or any tag")
	miniOptions.v.fixJpgOrientation = cmdMini.Flag.Bool("volume.images.fix.orientation", false, "Adjust jpg orientation when uploading.")
	miniOptions.v.readMode = cmdMini.Flag.String("volume.readMode", "proxy", "[local|proxy|redirect] how to deal with non-local volume: 'not found|read in remote node|redirect volume location'.")
	miniOptions.v.compactionMBPerSecond = cmdMini.Flag.Int("volume.compactionMBps", 0, "limit compaction speed in mega bytes per second")
	miniOptions.v.maintenanceMBPerSecond = cmdMini.Flag.Int("volume.maintenanceMBps", 0, "limit maintenance IO rate in MB/s")
	miniOptions.v.fileSizeLimitMB = cmdMini.Flag.Int("volume.fileSizeLimitMB", 256, "limit file size to avoid out of memory")
	miniOptions.v.ldbTimeout = cmdMini.Flag.Int64("volume.index.leveldbTimeout", 0, "alive time for leveldb")
	miniOptions.v.concurrentUploadLimitMB = cmdMini.Flag.Int("volume.concurrentUploadLimitMB", 0, "limit total concurrent upload size")
	miniOptions.v.concurrentDownloadLimitMB = cmdMini.Flag.Int("volume.concurrentDownloadLimitMB", 0, "limit total concurrent download size")
	miniOptions.v.pprof = cmdMini.Flag.Bool("volume.pprof", false, "enable pprof http handlers")
	miniOptions.v.idxFolder = cmdMini.Flag.String("volume.dir.idx", "", "directory to store .idx files")
	miniOptions.v.inflightUploadDataTimeout = cmdMini.Flag.Duration("volume.inflightUploadDataTimeout", 60*time.Second, "inflight upload data wait timeout")
	miniOptions.v.inflightDownloadDataTimeout = cmdMini.Flag.Duration("volume.inflightDownloadDataTimeout", 60*time.Second, "inflight download data wait timeout")
	miniOptions.v.hasSlowRead = cmdMini.Flag.Bool("volume.hasSlowRead", true, "if true, prevents slow reads from blocking other requests")
	miniOptions.v.readBufferSizeMB = cmdMini.Flag.Int("volume.readBufferSizeMB", 4, "read buffer size in MB")
	miniOptions.v.preStopSeconds = cmdMini.Flag.Int("volume.preStopSeconds", 1, "number of seconds between stop send heartbeats and stop volume server (default: 1 for mini)")

	// S3 options
	miniS3Options.port = cmdMini.Flag.Int("s3.port", 8333, "s3 server http listen port")
	miniS3Options.portHttps = cmdMini.Flag.Int("s3.port.https", 0, "s3 server https listen port")
	miniS3Options.portGrpc = cmdMini.Flag.Int("s3.port.grpc", 0, "s3 server grpc listen port")
	miniS3Options.domainName = cmdMini.Flag.String("s3.domainName", "", "suffix of the host name in comma separated list, {bucket}.{domainName}")
	miniS3Options.allowedOrigins = cmdMini.Flag.String("s3.allowedOrigins", "*", "comma separated list of allowed origins")
	miniS3Options.tlsPrivateKey = cmdMini.Flag.String("s3.key.file", "", "path to the TLS private key file")
	miniS3Options.tlsCertificate = cmdMini.Flag.String("s3.cert.file", "", "path to the TLS certificate file")
	miniS3Options.tlsCACertificate = cmdMini.Flag.String("s3.cacert.file", "", "path to the TLS CA certificate file")
	miniS3Options.tlsVerifyClientCert = cmdMini.Flag.Bool("s3.tlsVerifyClientCert", false, "whether to verify the client's certificate")
	miniS3Options.metricsHttpPort = cmdMini.Flag.Int("s3.metricsPort", 0, "Prometheus metrics listen port")
	miniS3Options.metricsHttpIp = cmdMini.Flag.String("s3.metricsIp", "", "metrics listen ip")
	miniS3Options.localFilerSocket = cmdMini.Flag.String("s3.localFilerSocket", "", "local filer socket path")
	miniS3Options.localSocket = cmdMini.Flag.String("s3.localSocket", "", "default to /tmp/seaweedfs-s3-<port>.sock")
	miniS3Options.idleTimeout = cmdMini.Flag.Int("s3.idleTimeout", 120, "connection idle seconds")
	miniS3Options.concurrentUploadLimitMB = cmdMini.Flag.Int("s3.concurrentUploadLimitMB", 0, "limit total concurrent upload size")
	miniS3Options.concurrentFileUploadLimit = cmdMini.Flag.Int("s3.concurrentFileUploadLimit", 0, "limit number of concurrent file uploads")
	miniS3Options.enableIam = cmdMini.Flag.Bool("s3.iam", true, "enable embedded IAM API on the same port")
	miniS3Options.dataCenter = cmdMini.Flag.String("s3.dataCenter", "", "prefer to read and write to volumes in this data center")
	miniS3Options.config = miniS3Config
	miniS3Options.iamConfig = miniIamConfig
	miniS3Options.auditLogConfig = cmdMini.Flag.String("s3.auditLogConfig", "", "path to the audit log config file")
	miniS3Options.allowDeleteBucketNotEmpty = miniS3AllowDeleteBucketNotEmpty
	miniS3Options.debug = cmdMini.Flag.Bool("s3.debug", false, "serves runtime profiling data via pprof")
	miniS3Options.debugPort = cmdMini.Flag.Int("s3.debug.port", 6060, "http port for debugging")

	// WebDAV options
	miniWebDavOptions.port = cmdMini.Flag.Int("webdav.port", 7333, "webdav server http listen port")
	miniWebDavOptions.collection = cmdMini.Flag.String("webdav.collection", "", "collection to create the files")
	miniWebDavOptions.replication = cmdMini.Flag.String("webdav.replication", "", "replication to create the files")
	miniWebDavOptions.disk = cmdMini.Flag.String("webdav.disk", "", "[hdd|ssd|<tag>] hard drive or solid state drive or any tag")
	miniWebDavOptions.tlsPrivateKey = cmdMini.Flag.String("webdav.key.file", "", "path to the TLS private key file")
	miniWebDavOptions.tlsCertificate = cmdMini.Flag.String("webdav.cert.file", "", "path to the TLS certificate file")
	miniWebDavOptions.cacheDir = cmdMini.Flag.String("webdav.cacheDir", os.TempDir(), "local cache directory for file chunks")
	miniWebDavOptions.cacheSizeMB = cmdMini.Flag.Int64("webdav.cacheCapacityMB", 0, "local cache capacity in MB")
	miniWebDavOptions.maxMB = cmdMini.Flag.Int("webdav.maxMB", 4, "split files larger than the limit")
	miniWebDavOptions.filerRootPath = cmdMini.Flag.String("webdav.filer.path", "/", "use this remote path from filer server")

	// Admin options
	miniAdminOptions.port = cmdMini.Flag.Int("admin.port", 23646, "admin server http listen port")
	miniAdminOptions.grpcPort = cmdMini.Flag.Int("admin.port.grpc", 0, "admin server grpc listen port (default: admin http port + 10000)")
	miniAdminOptions.master = cmdMini.Flag.String("admin.master", "", "master server address (automatically set)")
	miniAdminOptions.dataDir = cmdMini.Flag.String("admin.dataDir", "", "directory to store admin configuration and data files")
	miniAdminOptions.adminUser = cmdMini.Flag.String("admin.user", "admin", "admin interface username")
	miniAdminOptions.adminPassword = cmdMini.Flag.String("admin.password", "", "admin interface password (if empty, auth is disabled)")
}

func runMini(cmd *Command, args []string) bool {

	if *miniOptions.debug {
		grace.StartDebugServer(*miniOptions.debugPort)
	}

	util.LoadSecurityConfiguration()
	util.LoadConfiguration("master", false)

	grace.SetupProfiling(*miniOptions.cpuprofile, *miniOptions.memprofile)

	// Set master.peers to "none" if not specified (single master mode)
	if *miniMasterOptions.peers == "" {
		*miniMasterOptions.peers = "none"
	}

	// Validate and complete the peer list
	_, peerList := checkPeers(*miniIp, *miniMasterOptions.port, *miniMasterOptions.portGrpc, *miniMasterOptions.peers)
	actualPeersForComponents := strings.Join(pb.ToAddressStrings(peerList), ",")

	if *miniBindIp == "" {
		miniBindIp = miniIp
	}

	if *miniMetricsHttpIp == "" {
		*miniMetricsHttpIp = *miniBindIp
	}

	// ip address
	miniMasterOptions.ip = miniIp
	miniMasterOptions.ipBind = miniBindIp
	miniFilerOptions.masters = pb.ServerAddresses(actualPeersForComponents).ToServiceDiscovery()
	miniFilerOptions.ip = miniIp
	miniFilerOptions.bindIp = miniBindIp
	miniS3Options.bindIp = miniBindIp
	miniWebDavOptions.ipBind = miniBindIp
	miniOptions.v.ip = miniIp
	miniOptions.v.bindIp = miniBindIp
	miniOptions.v.masters = pb.ServerAddresses(actualPeersForComponents).ToAddresses()
	miniOptions.v.idleConnectionTimeout = miniTimeout
	miniOptions.v.dataCenter = miniDataCenter
	miniOptions.v.rack = miniRack

	miniMasterOptions.whiteList = miniWhiteListOption

	miniFilerOptions.dataCenter = miniDataCenter
	miniFilerOptions.rack = miniRack
	miniS3Options.dataCenter = miniDataCenter
	miniFilerOptions.disableHttp = miniDisableHttp
	miniMasterOptions.disableHttp = miniDisableHttp

	filerAddress := string(pb.NewServerAddress(*miniIp, *miniFilerOptions.port, *miniFilerOptions.portGrpc))
	miniS3Options.filer = &filerAddress
	miniWebDavOptions.filer = &filerAddress

	go stats_collect.StartMetricsServer(*miniMetricsHttpIp, *miniMetricsHttpPort)

	if *miniMasterOptions.volumeSizeLimitMB > util.VolumeSizeLimitGB*1000 {
		glog.Fatalf("masterVolumeSizeLimitMB should be less than 30000")
	}

	if *miniMasterOptions.metaFolder == "" {
		*miniMasterOptions.metaFolder = *miniDataFolders
	}
	if err := util.TestFolderWritable(util.ResolvePath(*miniMasterOptions.metaFolder)); err != nil {
		glog.Fatalf("Check Meta Folder (-dir=\"%s\") Writable: %s", *miniMasterOptions.metaFolder, err)
	}
	miniFilerOptions.defaultLevelDbDirectory = miniMasterOptions.metaFolder

	miniWhiteList := util.StringSplit(*miniWhiteListOption, ",")

	// Start all services with proper dependency coordination
	// This channel will be closed when all services are fully ready
	allServicesReady := make(chan struct{})
	err := startMiniServices(miniWhiteList, allServicesReady)
	if err != nil {
		glog.Fatalf("Failed to start services: %v", err)
	}

	// Wait for all services to be fully running before printing welcome message
	<-allServicesReady

	// Print welcome message after all services are running
	printWelcomeMessage()

	select {}
}

// startMiniServices starts all mini services with proper dependency coordination
func startMiniServices(miniWhiteList []string, allServicesReady chan struct{}) error {
	// Start Master server (no dependencies)
	go startMiniService("Master", func() {
		startMaster(miniMasterOptions, miniWhiteList)
	}, *miniMasterOptions.port)

	// Wait for master to be ready
	if err := waitForServiceReady("Master", *miniMasterOptions.port); err != nil {
		glog.Warningf("Master readiness check failed: %v", err)
	}

	// Start Volume server (depends on master)
	go startMiniService("Volume", func() {
		minFreeSpaces := util.MustParseMinFreeSpace(miniVolumeMinFreeSpace, miniVolumeMinFreeSpacePercent)
		miniOptions.v.startVolumeServer(*miniDataFolders, miniVolumeMaxDataVolumeCounts, *miniWhiteListOption, minFreeSpaces)
	}, *miniOptions.v.port)

	// Wait for volume to be ready
	if err := waitForServiceReady("Volume", *miniOptions.v.port); err != nil {
		glog.Warningf("Volume readiness check failed: %v", err)
	}

	// Start Filer (depends on master and volume)
	go startMiniService("Filer", func() {
		miniFilerOptions.startFiler()
	}, *miniFilerOptions.port)

	// Wait for filer to be ready
	if err := waitForServiceReady("Filer", *miniFilerOptions.port); err != nil {
		glog.Warningf("Filer readiness check failed: %v", err)
	}

	// Start S3 and WebDAV in parallel (both depend on filer)
	go startMiniService("S3", func() {
		startS3Service()
	}, *miniS3Options.port)

	go startMiniService("WebDAV", func() {
		miniWebDavOptions.startWebDav()
	}, *miniWebDavOptions.port)

	// Wait for both S3 and WebDAV to be ready
	if err := waitForServiceReady("S3", *miniS3Options.port); err != nil {
		glog.Warningf("S3 readiness check failed: %v", err)
	}
	if err := waitForServiceReady("WebDAV", *miniWebDavOptions.port); err != nil {
		glog.Warningf("WebDAV readiness check failed: %v", err)
	}

	// Start Admin with worker (depends on master, filer, S3, WebDAV)
	go startMiniAdminWithWorker(allServicesReady)

	return nil
}

// startMiniService starts a service in a goroutine with logging
func startMiniService(name string, fn func(), port int) {
	glog.Infof("%s service starting...", name)
	fn()
}

// waitForServiceReady pings the service HTTP endpoint to check if it's ready to accept connections
func waitForServiceReady(name string, port int) error {
	address := fmt.Sprintf("http://%s:%d", *miniIp, port)
	maxAttempts := 30 // 30 * 200ms = 6 seconds max wait
	attempt := 0
	client := &http.Client{
		Timeout: 200 * time.Millisecond,
	}

	for attempt < maxAttempts {
		resp, err := client.Get(address)
		if err == nil {
			resp.Body.Close()
			glog.V(1).Infof("%s service is ready at %s", name, address)
			return nil
		}
		attempt++
		time.Sleep(200 * time.Millisecond)
	}

	return fmt.Errorf("%s service did not become ready at %s after %d attempts", name, address, maxAttempts)
}

// startS3Service initializes and starts the S3 server
func startS3Service() {
	// Use existing AWS env vars if present (no new env vars).
	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")

	if accessKey != "" && secretKey != "" {
		user := "mini"
		iamCfg := &iam_pb.S3ApiConfiguration{}
		ident := &iam_pb.Identity{Name: user}
		ident.Credentials = append(ident.Credentials, &iam_pb.Credential{AccessKey: accessKey, SecretKey: secretKey})
		iamCfg.Identities = append(iamCfg.Identities, ident)

		iamPath := filepath.Join(*miniDataFolders, "iam_config.json")

		// Check if IAM config file already exists
		if _, err := os.Stat(iamPath); err == nil {
			// File exists, skip writing to preserve existing configuration
			glog.V(1).Infof("IAM config file already exists at %s, preserving existing configuration", iamPath)
		} else if os.IsNotExist(err) {
			// File does not exist, create and write new configuration
			f, err := os.OpenFile(iamPath, os.O_CREATE|os.O_WRONLY, 0600)
			if err != nil {
				glog.Errorf("failed to create IAM config file %s: %v", iamPath, err)
			} else {
				defer func() {
					if err := f.Close(); err != nil {
						glog.Errorf("failed to close IAM config file %s: %v", iamPath, err)
					}
				}()
				if err := filer.ProtoToText(f, iamCfg); err != nil {
					glog.Errorf("failed to write IAM config to %s: %v", iamPath, err)
				} else {
					*miniIamConfig = iamPath
					createdInitialIAM = true // Mark that we created initial IAM config
					glog.V(1).Infof("Created initial IAM config at %s", iamPath)
				}
			}
		} else {
			// Error checking file existence
			glog.Errorf("failed to check IAM config file existence at %s: %v", iamPath, err)
		}
	}

	miniS3Options.config = miniS3Config
	miniS3Options.iamConfig = miniIamConfig
	miniS3Options.allowDeleteBucketNotEmpty = miniS3AllowDeleteBucketNotEmpty
	miniS3Options.localFilerSocket = miniFilerOptions.localSocket
	miniS3Options.startS3Server()
}

// startMiniAdminWithWorker starts the admin server with one worker
func startMiniAdminWithWorker(allServicesReady chan struct{}) {
	defer close(allServicesReady) // Ensure channel is always closed on all paths

	ctx := context.Background()

	// Prepare master address
	masterAddr := fmt.Sprintf("%s:%d", *miniIp, *miniMasterOptions.port)

	// Set admin options
	*miniAdminOptions.master = masterAddr
	if *miniAdminOptions.grpcPort == 0 {
		grpcPort := *miniAdminOptions.port + 10000
		miniAdminOptions.grpcPort = &grpcPort
	}

	// Create data directory if specified
	if *miniAdminOptions.dataDir == "" {
		// Use a subdirectory in the main data folder
		adminDataDir := filepath.Join(*miniDataFolders, "admin")
		miniAdminOptions.dataDir = &adminDataDir
	}

	// Start admin server in background
	adminServerDone := make(chan struct{})
	go func() {
		if err := startAdminServer(ctx, miniAdminOptions); err != nil {
			glog.Errorf("Admin server error: %v", err)
		}
		close(adminServerDone)
	}()

	// Wait for admin server's HTTP port to be ready before launching worker
	adminAddr := fmt.Sprintf("http://%s:%d", *miniIp, *miniAdminOptions.port)
	glog.V(1).Infof("Waiting for admin server to be ready at %s...", adminAddr)
	if err := waitForAdminServerReady(adminAddr); err != nil {
		glog.Warningf("Admin server readiness check failed: %v", err)
	}

	// Start worker after admin server is ready
	startMiniWorker(allServicesReady)
}

// waitForAdminServerReady pings the admin server HTTP endpoint to check if it's ready
func waitForAdminServerReady(adminAddr string) error {
	maxAttempts := 40 // 40 * 500ms = 20 seconds max wait
	attempt := 0
	client := &http.Client{
		Timeout: 500 * time.Millisecond,
	}

	for attempt < maxAttempts {
		resp, err := client.Get(adminAddr)
		if err == nil {
			resp.Body.Close()
			glog.V(1).Infof("Admin server is ready at %s", adminAddr)
			return nil
		}
		attempt++
		time.Sleep(500 * time.Millisecond)
	}

	return fmt.Errorf("admin server did not become ready at %s after %d attempts", adminAddr, maxAttempts)
}

// startMiniWorker starts a single worker for the admin server
func startMiniWorker(allServicesReady chan struct{}) {
	glog.Infof("Starting maintenance worker for admin server")

	adminAddr := fmt.Sprintf("%s:%d", *miniIp, *miniAdminOptions.port)
	capabilities := "vacuum,ec,balance"

	// Use worker directory under main data folder
	workerDir := filepath.Join(*miniDataFolders, "worker")
	if err := os.MkdirAll(workerDir, 0755); err != nil {
		glog.Errorf("Failed to create worker directory: %v", err)
		return
	}

	glog.Infof("Worker connecting to admin server: %s", adminAddr)
	glog.Infof("Worker capabilities: %s", capabilities)
	glog.Infof("Worker directory: %s", workerDir)

	// Parse capabilities
	capabilitiesParsed := parseCapabilities(capabilities)
	if len(capabilitiesParsed) == 0 {
		glog.Errorf("No valid capabilities for worker")
		return
	}

	// Create task directories
	for _, capability := range capabilitiesParsed {
		taskDir := filepath.Join(workerDir, string(capability))
		if err := os.MkdirAll(taskDir, 0755); err != nil {
			glog.Errorf("Failed to create task directory %s: %v", taskDir, err)
			return
		}
	}

	// Load security configuration for gRPC communication
	util.LoadConfiguration("security", false)

	// Create gRPC dial option using TLS configuration
	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.worker")

	// Create worker configuration
	config := &types.WorkerConfig{
		AdminServer:         adminAddr,
		Capabilities:        capabilitiesParsed,
		MaxConcurrent:       2,
		HeartbeatInterval:   30 * time.Second,
		TaskRequestInterval: 5 * time.Second,
		BaseWorkingDir:      workerDir,
		GrpcDialOption:      grpcDialOption,
	}

	// Create worker instance
	workerInstance, err := worker.NewWorker(config)
	if err != nil {
		glog.Errorf("Failed to create worker: %v", err)
		return
	}

	// Create admin client
	adminClient, err := worker.CreateAdminClient(adminAddr, workerInstance.ID(), grpcDialOption)
	if err != nil {
		glog.Errorf("Failed to create admin client: %v", err)
		return
	}

	// Set admin client
	workerInstance.SetAdminClient(adminClient)

	// Start the worker
	err = workerInstance.Start()
	if err != nil {
		glog.Errorf("Failed to start worker: %v", err)
		return
	}

	glog.Infof("Maintenance worker %s started successfully", workerInstance.ID())
}

// printWelcomeMessage prints the welcome message after all services are running
func printWelcomeMessage() {
	fmt.Println("")
	fmt.Println("╔═══════════════════════════════════════════════════════════════════════════════╗")
	fmt.Println("║                      SeaweedFS Mini - All-in-One Mode                         ║")
	fmt.Println("╚═══════════════════════════════════════════════════════════════════════════════╝")
	fmt.Println("")
	fmt.Println("  All components are running and ready to use:")
	fmt.Println("")
	fmt.Printf("    Master UI:      http://%s:%d\n", *miniIp, *miniMasterOptions.port)
	fmt.Printf("    Filer UI:       http://%s:%d\n", *miniIp, *miniFilerOptions.port)
	fmt.Printf("    S3 Endpoint:    http://%s:%d\n", *miniIp, *miniS3Options.port)
	fmt.Printf("    WebDAV:         http://%s:%d\n", *miniIp, *miniWebDavOptions.port)
	fmt.Printf("    Admin UI:       http://%s:%d\n", *miniIp, *miniAdminOptions.port)
	fmt.Printf("    Volume Server:  http://%s:%d\n", *miniIp, *miniOptions.v.port)
	fmt.Println("")
	fmt.Println("  Optimized Settings:")
	fmt.Println("    • Volume size limit: 128MB")
	fmt.Println("    • Volume max: auto (based on free disk space)")
	fmt.Println("    • Pre-stop seconds: 1 (faster shutdown)")
	fmt.Println("    • Master peers: none (single master mode)")
	fmt.Println("    • Admin UI for management and maintenance tasks")
	fmt.Println("")
	fmt.Println("  Data Directory: " + *miniDataFolders)
	fmt.Println("")
	fmt.Println("  Press Ctrl+C to stop all components")
	fmt.Println("")

	// If we created initial IAM config from AWS env vars, inform the user;
	// otherwise instruct user to create credentials via Admin UI.
	if createdInitialIAM {
		fmt.Println("  Initial S3 credentials created:")
		fmt.Printf("    user: mini\n")
		fmt.Println("    Note: credentials have been written to the IAM configuration file.")
		fmt.Println("")
	} else {
		fmt.Println("  To create S3 credentials, you have two options:")
		fmt.Println("")
		fmt.Println("  Option 1: Use environment variables (recommended for quick setup)")
		fmt.Println("    export AWS_ACCESS_KEY_ID=your-access-key")
		fmt.Println("    export AWS_SECRET_ACCESS_KEY=your-secret-key")
		fmt.Println("    weed mini -dir=/data")
		fmt.Println("    This will create initial credentials for the 'mini' user.")
		fmt.Println("")
		fmt.Println("  Option 2: Use the Admin UI")
		fmt.Printf("    Open: http://%s:%d\n", *miniIp, *miniAdminOptions.port)
		fmt.Println("    Add a new identity to create S3 credentials.")
		fmt.Println("")
	}
}
