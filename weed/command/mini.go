package command

import (
	"context"
	"fmt"
	"math/bits"
	"net"
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
	flag "github.com/seaweedfs/seaweedfs/weed/util/fla9"
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
	bytesPerMB                    = 1024 * 1024 // Bytes per MB
	miniVolumeMaxDataVolumeCounts = "0"         // auto-configured based on free disk space
	miniVolumeMinFreeSpace        = "1"         // 1% minimum free space
	minVolumeSizeMB               = 64          // Minimum volume size in MB
	defaultMiniVolumeSizeMB       = 128         // Default volume size for mini mode
	maxVolumeSizeMB               = 1024        // Maximum volume size in MB (1GB)
	GrpcPortOffset                = 10000       // Offset used to calculate gRPC port from HTTP port
)

var (
	miniOptions       MiniOptions
	miniMasterOptions MasterOptions
	miniFilerOptions  FilerOptions
	miniS3Options     S3Options
	miniWebDavOptions WebDavOption
	miniAdminOptions  AdminOptions
	createdInitialIAM bool // Track if initial IAM config was created from env vars
	// Track which port flags were explicitly passed on CLI before config file is applied
	explicitPortFlags map[string]bool
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
- Volume size limit: auto configured based on disk space (64MB-1024MB)
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

// getBindIp determines the bind IP address based on miniIp and miniBindIp flags
// Returns miniBindIp if set (non-empty), otherwise returns miniIp
func getBindIp() string {
	if *miniBindIp != "" {
		return *miniBindIp
	}
	return *miniIp
}

// initMiniCommonFlags initializes common mini flags
func initMiniCommonFlags() {
	miniOptions.cpuprofile = cmdMini.Flag.String("cpuprofile", "", "cpu profile output file")
	miniOptions.memprofile = cmdMini.Flag.String("memprofile", "", "memory profile output file")
	miniOptions.debug = cmdMini.Flag.Bool("debug", false, "serves runtime profiling data, e.g., http://localhost:6060/debug/pprof/goroutine?debug=2")
	miniOptions.debugPort = cmdMini.Flag.Int("debug.port", 6060, "http port for debugging")
}

// initMiniMasterFlags initializes Master server flag options
func initMiniMasterFlags() {
	miniMasterOptions.port = cmdMini.Flag.Int("master.port", 9333, "master server http listen port")
	miniMasterOptions.portGrpc = cmdMini.Flag.Int("master.port.grpc", 0, "master server grpc listen port")
	miniMasterOptions.metaFolder = cmdMini.Flag.String("master.dir", "", "data directory to store meta data, default to same as -dir specified")
	miniMasterOptions.peers = cmdMini.Flag.String("master.peers", "", "all master nodes in comma separated ip:masterPort list (default: none for single master)")
	miniMasterOptions.volumeSizeLimitMB = cmdMini.Flag.Uint("master.volumeSizeLimitMB", defaultMiniVolumeSizeMB, "Master stops directing writes to oversized volumes (default: 128MB for mini)")
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
}

// initMiniFilerFlags initializes Filer server flag options
func initMiniFilerFlags() {
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
}

// initMiniVolumeFlags initializes Volume server flag options
func initMiniVolumeFlags() {
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
}

// initMiniS3Flags initializes S3 server flag options
func initMiniS3Flags() {
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
	miniS3Options.cipher = cmdMini.Flag.Bool("s3.encryptVolumeData", false, "encrypt data on volume servers for S3 uploads")
	miniS3Options.config = miniS3Config
	miniS3Options.iamConfig = miniIamConfig
	miniS3Options.auditLogConfig = cmdMini.Flag.String("s3.auditLogConfig", "", "path to the audit log config file")
	miniS3Options.allowDeleteBucketNotEmpty = miniS3AllowDeleteBucketNotEmpty
	// In mini mode, S3 uses the shared debug server started at line 681, not its own separate debug server
	miniS3Options.debug = new(bool) // explicitly false
	miniS3Options.debugPort = cmdMini.Flag.Int("s3.debug.port", 6060, "http port for debugging (unused in mini mode)")
}

// initMiniWebDAVFlags initializes WebDAV server flag options
func initMiniWebDAVFlags() {
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
}

// initMiniAdminFlags initializes Admin server flag options
func initMiniAdminFlags() {
	miniAdminOptions.port = cmdMini.Flag.Int("admin.port", 23646, "admin server http listen port")
	miniAdminOptions.grpcPort = cmdMini.Flag.Int("admin.port.grpc", 0, "admin server grpc listen port (default: admin http port + GrpcPortOffset)")
	miniAdminOptions.master = cmdMini.Flag.String("admin.master", "", "master server address (automatically set)")
	miniAdminOptions.dataDir = cmdMini.Flag.String("admin.dataDir", "", "directory to store admin configuration and data files")
	miniAdminOptions.adminUser = cmdMini.Flag.String("admin.user", "admin", "admin interface username")
	miniAdminOptions.adminPassword = cmdMini.Flag.String("admin.password", "", "admin interface password (if empty, auth is disabled)")
	miniAdminOptions.readOnlyUser = cmdMini.Flag.String("admin.readOnlyUser", "", "read-only user username (optional, for view-only access)")
	miniAdminOptions.readOnlyPassword = cmdMini.Flag.String("admin.readOnlyPassword", "", "read-only user password (optional, for view-only access; requires admin.password to be set)")
}

func init() {
	// Initialize common flags
	initMiniCommonFlags()

	// Initialize component-specific flags
	initMiniMasterFlags()
	initMiniFilerFlags()
	initMiniVolumeFlags()
	initMiniS3Flags()
	initMiniWebDAVFlags()
	initMiniAdminFlags()
}

// calculateOptimalVolumeSizeMB calculates optimal volume size based on total disk capacity.
//
// Algorithm:
// 1. Read total disk capacity using the OS-independent stats.NewDiskStatus()
// 2. Convert capacity from bytes to MB, then divide by 100
// 3. Round up to nearest power of 2 (64MB, 128MB, 256MB, 512MB, 1024MB, etc.)
// 4. Clamp the result to range [64MB, 1024MB]
//
// Examples (GB→MB conversion, divide by 100, round to next power-of-2, clamp [64,1024]):
// - 10GB disk   → 10240MB / 100 = 102.4MB → rounds to 128MB
// - 100GB disk  → 102400MB / 100 = 1024MB → rounds to 1024MB
// - 500GB disk  → 512000MB / 100 = 5120MB → rounds to 8192MB → capped to 1024MB
// - 1TB disk    → 1048576MB / 100 = 10485.76MB → capped to 1024MB (maximum)
// - 6.4TB disk  → 6553600MB / 100 = 65536MB → capped to 1024MB (maximum)
// - 12.8TB disk → 13107200MB / 100 = 131072MB → capped to 1024MB (maximum)
func calculateOptimalVolumeSizeMB(dataFolder string) uint {
	// Get disk status for the data folder using OS-independent function
	diskStatus := stats_collect.NewDiskStatus(dataFolder)
	if diskStatus == nil || diskStatus.All == 0 {
		glog.Warningf("Could not determine disk size, using default %dMB", defaultMiniVolumeSizeMB)
		return defaultMiniVolumeSizeMB
	}

	// Calculate optimal size: total disk capacity / 100 for stability
	// Using total capacity (All) instead of free space ensures consistent volume size
	// regardless of current disk usage. diskStatus.All is in bytes, convert to MB
	totalCapacityMB := diskStatus.All / bytesPerMB
	initialOptimalMB := uint(totalCapacityMB / 100)
	optimalMB := initialOptimalMB

	// Round up to nearest power of 2: 64MB, 128MB, 256MB, 512MB, etc.
	// Minimum is 64MB, maximum is 1024MB (1GB)
	if optimalMB == 0 {
		// If the computed optimal size is 0, start from the minimum volume size
		optimalMB = minVolumeSizeMB
	} else {
		// Round up to the nearest power of 2
		optimalMB = 1 << bits.Len(optimalMB-1)
	}

	// Apply the minimum and maximum constraints
	if optimalMB < minVolumeSizeMB {
		optimalMB = minVolumeSizeMB
	} else if optimalMB > maxVolumeSizeMB {
		optimalMB = maxVolumeSizeMB
	}

	glog.Infof("Optimal volume size: %dMB (total disk capacity: %dMB, capacity/100 before rounding: %dMB, rounded to nearest power of 2, clamped to [%d,%d]MB)",
		optimalMB, totalCapacityMB, initialOptimalMB, minVolumeSizeMB, maxVolumeSizeMB)

	return optimalMB
}

// isFlagPassed checks if a specific flag was passed on the command line
func isFlagPassed(name string) bool {
	found := false
	cmdMini.Flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}

// isPortOpenOnIP checks if a port is available for binding on a specific IP address
func isPortOpenOnIP(ip string, port int) bool {
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", ip, port))
	if err != nil {
		return false
	}
	listener.Close()
	return true
}

// isPortAvailable checks if a port is available on any interface
// This is more comprehensive than checking a single IP
func isPortAvailable(port int) bool {
	// Try to listen on all interfaces (0.0.0.0)
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return false
	}
	listener.Close()
	return true
}

// findAvailablePortOnIP finds the next available port on a specific IP starting from the given port
// It skips any ports that are in the reservedPorts map (for gRPC port collision avoidance)
// It returns the first available port found within maxAttempts, or 0 if none found
func findAvailablePortOnIP(ip string, startPort int, maxAttempts int, reservedPorts map[int]bool) int {
	for i := 0; i < maxAttempts; i++ {
		port := startPort + i
		// Skip ports reserved for gRPC calculation
		if reservedPorts[port] {
			continue
		}
		// Check on both the specific IP and on all interfaces for maximum reliability
		if isPortOpenOnIP(ip, port) && isPortAvailable(port) {
			return port
		}
	}
	// If no port found, return 0 to indicate failure
	return 0
}

// ensurePortAvailableOnIP ensures a port pointer points to an available port on a specific IP
// If the port is not available, it finds the next available port and updates the pointer
// The reservedPorts map contains ports that should not be allocated (for gRPC collision avoidance)
func ensurePortAvailableOnIP(portPtr *int, serviceName string, ip string, reservedPorts map[int]bool, flagName string) error {
	if portPtr == nil {
		return nil
	}

	original := *portPtr

	// Check if this port was explicitly specified by the user (from CLI, before config file was applied)
	isExplicitPort := explicitPortFlags[flagName]

	// Skip if this port is reserved for gRPC calculation
	if reservedPorts[original] {
		if isExplicitPort {
			return fmt.Errorf("port %d for %s (specified by flag %s) is reserved for gRPC calculation and cannot be used", original, serviceName, flagName)
		}
		glog.Warningf("Port %d for %s is reserved for gRPC calculation, finding alternative...", original, serviceName)
		newPort := findAvailablePortOnIP(ip, original+1, 100, reservedPorts)
		if newPort == 0 {
			glog.Errorf("Could not find available port for %s starting from %d, will use original %d and fail on binding", serviceName, original+1, original)
		} else {
			glog.Infof("Port %d for %s is available, using it instead of %d", newPort, serviceName, original)
			*portPtr = newPort
		}
		return nil
	}

	// Check on both the specific IP and on all interfaces (0.0.0.0) for maximum reliability
	if !isPortOpenOnIP(ip, original) || !isPortAvailable(original) {
		// If explicitly specified, fail immediately with the originally requested port
		if isExplicitPort {
			return fmt.Errorf("port %d for %s (specified by flag %s) is not available on %s and cannot be used", original, serviceName, flagName, ip)
		}
		// For default ports, try to find an alternative
		glog.Warningf("Port %d for %s is not available on %s, finding alternative port...", original, serviceName, ip)
		newPort := findAvailablePortOnIP(ip, original+1, 100, reservedPorts)
		if newPort == 0 {
			glog.Errorf("Could not find available port for %s starting from %d, will use original %d and fail on binding", serviceName, original+1, original)
		} else {
			glog.Infof("Port %d for %s is available, using it instead of %d", newPort, serviceName, original)
			*portPtr = newPort
		}
	} else {
		glog.V(1).Infof("Port %d for %s is available on %s", original, serviceName, ip)
	}
	return nil
}

// ensureAllPortsAvailableOnIP ensures all mini service ports are available on a specific IP
// Returns an error if an explicitly specified port is unavailable.
// This should be called before starting any services
func ensureAllPortsAvailableOnIP(bindIp string) error {
	portConfigs := []struct {
		port     *int
		name     string
		flagName string
		grpcPtr  *int
	}{
		{miniMasterOptions.port, "Master", "master.port", miniMasterOptions.portGrpc},
		{miniFilerOptions.port, "Filer", "filer.port", miniFilerOptions.portGrpc},
		{miniOptions.v.port, "Volume", "volume.port", miniOptions.v.portGrpc},
		{miniS3Options.port, "S3", "s3.port", miniS3Options.portGrpc},
		{miniWebDavOptions.port, "WebDAV", "webdav.port", nil},
		{miniAdminOptions.port, "Admin", "admin.port", miniAdminOptions.grpcPort},
	}

	// First, reserve all gRPC ports that will be calculated to prevent HTTP port allocation from using them
	// This prevents collisions like: HTTP port moves to X, then gRPC port is calculated as Y where Y == X
	reservedPorts := make(map[int]bool)
	for _, config := range portConfigs {
		if config.grpcPtr != nil && *config.grpcPtr == 0 {
			// This gRPC port will be calculated as httpPort + GrpcPortOffset
			calculatedGrpcPort := *config.port + GrpcPortOffset
			reservedPorts[calculatedGrpcPort] = true
		}
	}

	// Check all HTTP ports sequentially to avoid race conditions
	// Each port check and allocation must complete before the next one starts
	// to prevent multiple goroutines from claiming the same available port
	// Also avoid allocating ports that are reserved for gRPC calculation
	for _, config := range portConfigs {
		original := *config.port
		if err := ensurePortAvailableOnIP(config.port, config.name, bindIp, reservedPorts, config.flagName); err != nil {
			return err
		}
		// If port was changed, update the reserved gRPC ports mapping
		if *config.port != original && config.grpcPtr != nil && *config.grpcPtr == 0 {
			delete(reservedPorts, original+GrpcPortOffset)
			reservedPorts[*config.port+GrpcPortOffset] = true
		}
	}

	// Initialize all gRPC ports before services start
	// This ensures they won't be recalculated and cause conflicts
	// All gRPC port handling (calculation, validation, and assignment) is performed exclusively in initializeGrpcPortsOnIP
	initializeGrpcPortsOnIP(bindIp)

	// Log the final port configuration
	glog.Infof("Final port configuration - Master: %d, Filer: %d, Volume: %d, S3: %d, WebDAV: %d, Admin: %d",
		*miniMasterOptions.port, *miniFilerOptions.port, *miniOptions.v.port,
		*miniS3Options.port, *miniWebDavOptions.port, *miniAdminOptions.port)

	// Log gRPC ports too (now finalized)
	glog.Infof("gRPC port configuration - Master: %d, Filer: %d, Volume: %d, S3: %d, Admin: %d",
		*miniMasterOptions.portGrpc, *miniFilerOptions.portGrpc, *miniOptions.v.portGrpc,
		*miniS3Options.portGrpc, *miniAdminOptions.grpcPort)

	return nil
}

// initializeGrpcPortsOnIP initializes all gRPC ports based on their HTTP ports on a specific IP
// If a gRPC port is 0, it will be set to httpPort + GrpcPortOffset
// This must be called after HTTP ports are finalized and before services start
func initializeGrpcPortsOnIP(bindIp string) {
	// Track gRPC ports allocated during this function to prevent collisions between services
	// when multiple services need fallback port allocation
	allocatedGrpcPorts := make(map[int]bool)

	grpcConfigs := []struct {
		httpPort *int
		grpcPort *int
		name     string
	}{
		{miniMasterOptions.port, miniMasterOptions.portGrpc, "Master"},
		{miniFilerOptions.port, miniFilerOptions.portGrpc, "Filer"},
		{miniOptions.v.port, miniOptions.v.portGrpc, "Volume"},
		{miniS3Options.port, miniS3Options.portGrpc, "S3"},
		{miniAdminOptions.port, miniAdminOptions.grpcPort, "Admin"},
	}

	for _, config := range grpcConfigs {
		if config.grpcPort == nil {
			continue
		}

		// If gRPC port is 0, calculate it from HTTP port
		if *config.grpcPort == 0 {
			*config.grpcPort = *config.httpPort + GrpcPortOffset
		}

		// Verify the gRPC port is available (whether calculated or explicitly set)
		// Check on both specific IP and all interfaces, and check against already allocated ports
		if !isPortOpenOnIP(bindIp, *config.grpcPort) || !isPortAvailable(*config.grpcPort) || allocatedGrpcPorts[*config.grpcPort] {
			glog.Warningf("gRPC port %d for %s is not available, finding alternative...", *config.grpcPort, config.name)
			originalPort := *config.grpcPort
			newPort := findAvailablePortOnIP(bindIp, originalPort+1, 100, allocatedGrpcPorts)
			if newPort == 0 {
				glog.Errorf("Could not find available gRPC port for %s starting from %d, will use %d and fail on binding", config.name, originalPort+1, originalPort)
			} else {
				glog.Infof("gRPC port %d for %s is available, using it instead of %d", newPort, config.name, originalPort)
				*config.grpcPort = newPort
			}
		}
		allocatedGrpcPorts[*config.grpcPort] = true
		glog.V(1).Infof("%s gRPC port set to %d", config.name, *config.grpcPort)
	}
}

// loadMiniConfigurationFile reads the mini.options file and returns parsed options
// File format: one option per line, without leading dash (e.g., "ip=127.0.0.1")
func loadMiniConfigurationFile(dataFolder string) (map[string]string, error) {
	configFile := filepath.Join(util.ResolvePath(util.StringSplit(dataFolder, ",")[0]), "mini.options")

	options := make(map[string]string)

	// Check if file exists
	data, err := os.ReadFile(configFile)
	if err != nil {
		if os.IsNotExist(err) {
			// File doesn't exist - this is OK, return empty options
			return options, nil
		}
		glog.Warningf("Failed to read configuration file %s: %v", configFile, err)
		return options, err
	}

	// Parse the file line by line
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)

		// Skip empty lines and comments
		if len(line) == 0 || strings.HasPrefix(line, "#") {
			continue
		}

		// Remove leading dash if present
		if strings.HasPrefix(line, "-") {
			line = line[1:]
		}

		// Parse key=value
		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			key := strings.TrimSpace(parts[0])
			value := strings.TrimSpace(parts[1])
			// Remove quotes if present
			if (strings.HasPrefix(value, "\"") && strings.HasSuffix(value, "\"")) ||
				(strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'")) {
				value = value[1 : len(value)-1]
			}
			options[key] = value
		}
	}

	glog.Infof("Loaded %d options from configuration file %s", len(options), configFile)
	return options, nil
}

// applyConfigFileOptions sets command-line flags from loaded configuration file
func applyConfigFileOptions(options map[string]string) {
	for key, value := range options {
		// Skip port flags that were explicitly passed on CLI
		if explicitPortFlags[key] {
			glog.V(2).Infof("Skipping config file option %s=%s (explicitly specified on command line)", key, value)
			continue
		}
		// Set the flag value if it hasn't been explicitly set on command line
		flag := cmdMini.Flag.Lookup(key)
		if flag != nil {
			// Only set if not already set (by command line)
			if flag.Value.String() == flag.DefValue {
				flag.Value.Set(value)
				glog.V(2).Infof("Applied config file option: %s=%s", key, value)
			}
		}
	}
}

// saveMiniConfiguration saves the current mini configuration to a file
// The file format uses option=value format without leading dashes
func saveMiniConfiguration(dataFolder string) error {
	configDir := util.ResolvePath(util.StringSplit(dataFolder, ",")[0])
	if err := os.MkdirAll(configDir, 0755); err != nil {
		glog.Warningf("Failed to create config directory %s: %v", configDir, err)
		return err
	}

	configFile := filepath.Join(configDir, "mini.options")

	var sb strings.Builder
	sb.WriteString("#!/bin/bash\n")
	sb.WriteString("# Mini server configuration\n")
	sb.WriteString("# Format: option=value (no leading dash)\n")
	sb.WriteString("# This file is loaded on startup if it exists\n\n")

	// Collect all flags that were explicitly passed (except "dir")
	cmdMini.Flag.Visit(func(f *flag.Flag) {
		// Skip the "dir" option - it's environment-specific
		if f.Name == "dir" {
			return
		}
		value := f.Value.String()
		// Quote the value if it contains spaces
		if strings.Contains(value, " ") {
			sb.WriteString(fmt.Sprintf("%s=\"%s\"\n", f.Name, value))
		} else {
			sb.WriteString(fmt.Sprintf("%s=%s\n", f.Name, value))
		}
	})

	// Add auto-calculated volume size if it was computed
	if !isFlagPassed("master.volumeSizeLimitMB") && miniMasterOptions.volumeSizeLimitMB != nil {
		sb.WriteString(fmt.Sprintf("\n# Auto-calculated volume size based on total disk capacity\n"))
		sb.WriteString(fmt.Sprintf("# Delete this line to force recalculation on next startup\n"))
		sb.WriteString(fmt.Sprintf("master.volumeSizeLimitMB=%d\n", *miniMasterOptions.volumeSizeLimitMB))
	}

	if err := os.WriteFile(configFile, []byte(sb.String()), 0644); err != nil {
		glog.Warningf("Failed to save configuration to %s: %v", configFile, err)
		return err
	}

	glog.Infof("Mini configuration saved to %s", configFile)
	return nil
}

func runMini(cmd *Command, args []string) bool {

	// Capture which port flags were explicitly passed on CLI BEFORE config file is applied
	// This is necessary to distinguish user-specified ports from defaults or config file options
	explicitPortFlags = make(map[string]bool)
	portFlagNames := []string{"master.port", "filer.port", "volume.port", "s3.port", "webdav.port", "admin.port"}
	for _, flagName := range portFlagNames {
		explicitPortFlags[flagName] = isFlagPassed(flagName)
	}

	// Load configuration from file if it exists
	configOptions, err := loadMiniConfigurationFile(*miniDataFolders)
	if err != nil {
		glog.Warningf("Error loading configuration file: %v", err)
	}
	// Apply loaded options to flags (CLI flags will override these)
	applyConfigFileOptions(configOptions)

	if *miniOptions.debug {
		grace.StartDebugServer(*miniOptions.debugPort)
	}

	util.LoadSecurityConfiguration()
	util.LoadConfiguration("master", false)

	grace.SetupProfiling(*miniOptions.cpuprofile, *miniOptions.memprofile)

	// Determine bind IP
	bindIp := getBindIp()

	// Ensure all ports are available, find alternatives if needed
	if err := ensureAllPortsAvailableOnIP(bindIp); err != nil {
		glog.Errorf("Port allocation failed: %v", err)
		os.Exit(1)
	}

	// Set master.peers to "none" if not specified (single master mode)
	if *miniMasterOptions.peers == "" {
		*miniMasterOptions.peers = "none"
	}

	// Validate and complete the peer list
	_, peerList := checkPeers(*miniIp, *miniMasterOptions.port, *miniMasterOptions.portGrpc, *miniMasterOptions.peers)
	actualPeersForComponents := strings.Join(pb.ToAddressStrings(peerList), ",")

	if *miniBindIp == "" {
		*miniBindIp = *miniIp
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

	// Calculate and set optimal volume size limit based on available disk space
	// Only auto-calculate if user didn't explicitly specify a value via -master.volumeSizeLimitMB
	if !isFlagPassed("master.volumeSizeLimitMB") {
		// User didn't override, use auto-calculated value
		// The -dir flag can accept comma-separated directories; use the first one for disk space calculation
		resolvedDataFolder := util.ResolvePath(util.StringSplit(*miniDataFolders, ",")[0])
		optimalVolumeSizeMB := calculateOptimalVolumeSizeMB(resolvedDataFolder)
		miniMasterOptions.volumeSizeLimitMB = &optimalVolumeSizeMB
		glog.Infof("Mini started with auto-calculated optimal volume size limit: %dMB", optimalVolumeSizeMB)
	} else {
		// User specified a custom value
		glog.Infof("Mini started with user-specified volume size limit: %dMB", *miniMasterOptions.volumeSizeLimitMB)
	}

	miniWhiteList := util.StringSplit(*miniWhiteListOption, ",")

	// Start all services with proper dependency coordination
	// This channel will be closed when all services are fully ready
	allServicesReady := make(chan struct{})
	startMiniServices(miniWhiteList, allServicesReady)

	// Wait for all services to be fully running before printing welcome message
	<-allServicesReady

	// Print welcome message after all services are running
	printWelcomeMessage()

	// Save configuration to file for persistence and documentation
	saveMiniConfiguration(*miniDataFolders)

	select {}
}

// startMiniServices starts all mini services with proper dependency coordination
func startMiniServices(miniWhiteList []string, allServicesReady chan struct{}) {
	// Determine bind IP for health checks
	bindIp := getBindIp()

	// Start Master server (no dependencies)
	go startMiniService("Master", func() {
		startMaster(miniMasterOptions, miniWhiteList)
	}, *miniMasterOptions.port)

	// Wait for master to be ready
	waitForServiceReady("Master", *miniMasterOptions.port, bindIp)

	// Start Volume server (depends on master)
	go startMiniService("Volume", func() {
		minFreeSpaces := util.MustParseMinFreeSpace(miniVolumeMinFreeSpace, "")
		miniOptions.v.startVolumeServer(*miniDataFolders, miniVolumeMaxDataVolumeCounts, *miniWhiteListOption, minFreeSpaces)
	}, *miniOptions.v.port)

	// Wait for volume to be ready
	waitForServiceReady("Volume", *miniOptions.v.port, bindIp)

	// Start Filer (depends on master and volume)
	go startMiniService("Filer", func() {
		miniFilerOptions.startFiler()
	}, *miniFilerOptions.port)

	// Wait for filer to be ready
	waitForServiceReady("Filer", *miniFilerOptions.port, bindIp)

	// Start S3 and WebDAV in parallel (both depend on filer)
	go startMiniService("S3", func() {
		startS3Service()
	}, *miniS3Options.port)

	go startMiniService("WebDAV", func() {
		miniWebDavOptions.startWebDav()
	}, *miniWebDavOptions.port)

	// Wait for both S3 and WebDAV to be ready
	waitForServiceReady("S3", *miniS3Options.port, bindIp)
	waitForServiceReady("WebDAV", *miniWebDavOptions.port, bindIp)

	// Start Admin with worker (depends on master, filer, S3, WebDAV)
	go startMiniAdminWithWorker(allServicesReady)
}

// startMiniService starts a service in a goroutine with logging
func startMiniService(name string, fn func(), port int) {
	glog.Infof("%s service starting...", name)
	fn()
}

// waitForServiceReady pings the service HTTP endpoint to check if it's ready to accept connections
func waitForServiceReady(name string, port int, bindIp string) {
	address := fmt.Sprintf("http://%s:%d", bindIp, port)
	maxAttempts := 30 // 30 * 200ms = 6 seconds max wait
	attempt := 0
	client := &http.Client{
		Timeout: 200 * time.Millisecond,
	}

	for attempt < maxAttempts {
		resp, err := client.Get(address)
		if err == nil {
			resp.Body.Close()
			glog.Infof("%s service is ready at %s", name, address)
			return
		}
		attempt++
		time.Sleep(200 * time.Millisecond)
	}

	// Service failed to become ready, log warning but don't fail startup
	// (services may still work even if health check endpoint isn't responding immediately)
	glog.Warningf("Health check for %s failed (service may still be functional, retries may succeed)", name)
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
		ident.Actions = append(ident.Actions, "Admin")
		iamCfg.Identities = append(iamCfg.Identities, ident)

		iamPath := filepath.Join(*miniDataFolders, "iam_config.json")

		// Check if IAM config file already exists
		if _, err := os.Stat(iamPath); err == nil {
			// File exists, skip writing to preserve existing configuration
			glog.V(1).Infof("IAM config file already exists at %s, preserving existing configuration", iamPath)
			*miniIamConfig = iamPath
		} else if os.IsNotExist(err) {
			// File does not exist, create and write new configuration
			f, err := os.OpenFile(iamPath, os.O_CREATE|os.O_WRONLY, 0600)
			if err != nil {
				glog.Fatalf("failed to create IAM config file %s: %v", iamPath, err)
			}
			defer f.Close()
			if err := filer.ProtoToText(f, iamCfg); err != nil {
				glog.Fatalf("failed to write IAM config to %s: %v", iamPath, err)
			}
			*miniIamConfig = iamPath
			createdInitialIAM = true // Mark that we created initial IAM config
			glog.V(1).Infof("Created initial IAM config at %s", iamPath)
		} else {
			// Error checking file existence
			glog.Fatalf("failed to check IAM config file existence at %s: %v", iamPath, err)
		}
	}

	miniS3Options.localFilerSocket = miniFilerOptions.localSocket
	miniS3Options.startS3Server()
}

// startMiniAdminWithWorker starts the admin server with one worker
func startMiniAdminWithWorker(allServicesReady chan struct{}) {
	defer close(allServicesReady) // Ensure channel is always closed on all paths

	ctx := context.Background()

	// Determine bind IP for health checks
	bindIp := getBindIp()

	// Prepare master address
	masterAddr := fmt.Sprintf("%s:%d", *miniIp, *miniMasterOptions.port)

	// Set admin options
	*miniAdminOptions.master = masterAddr

	// Security validation: prevent empty username when password is set
	if *miniAdminOptions.adminPassword != "" && *miniAdminOptions.adminUser == "" {
		glog.Fatalf("Error: -admin.user cannot be empty when -admin.password is set")
	}
	if *miniAdminOptions.readOnlyPassword != "" && *miniAdminOptions.readOnlyUser == "" {
		glog.Fatalf("Error: -admin.readOnlyUser is required when -admin.readOnlyPassword is set")
	}
	// Security validation: prevent username conflicts between admin and read-only users
	if *miniAdminOptions.adminUser != "" && *miniAdminOptions.readOnlyUser != "" &&
		*miniAdminOptions.adminUser == *miniAdminOptions.readOnlyUser {
		glog.Fatalf("Error: -admin.user and -admin.readOnlyUser must be different when both are configured")
	}
	// Security validation: admin password is required for read-only user
	if *miniAdminOptions.readOnlyPassword != "" && *miniAdminOptions.adminPassword == "" {
		glog.Fatalf("Error: -admin.password must be set when -admin.readOnlyPassword is configured")
	}

	// gRPC port should have been initialized by ensureAllPortsAvailableOnIP in runMini
	// If it's still 0, that indicates a problem with the port initialization sequence
	if *miniAdminOptions.grpcPort == 0 {
		glog.Fatalf("Admin gRPC port was not initialized before startAdminServer. This indicates a problem with the port initialization sequence.")
	}

	// Create data directory if specified
	if *miniAdminOptions.dataDir == "" {
		// Use a subdirectory in the main data folder
		*miniAdminOptions.dataDir = filepath.Join(*miniDataFolders, "admin")
	}

	// Start admin server in background
	go func() {
		if err := startAdminServer(ctx, miniAdminOptions); err != nil {
			glog.Errorf("Admin server error: %v", err)
		}
	}()

	// Wait for admin server's HTTP port to be ready before launching worker
	adminAddr := fmt.Sprintf("http://%s:%d", bindIp, *miniAdminOptions.port)
	glog.V(1).Infof("Waiting for admin server to be ready at %s...", adminAddr)
	if err := waitForAdminServerReady(adminAddr); err != nil {
		glog.Fatalf("Admin server readiness check failed: %v", err)
	}

	// Start worker after admin server is ready
	startMiniWorker()

	// Wait for worker to be ready by polling its gRPC port
	workerGrpcAddr := fmt.Sprintf("%s:%d", bindIp, *miniAdminOptions.grpcPort)
	waitForWorkerReady(workerGrpcAddr)
}

// waitForAdminServerReady pings the admin server HTTP endpoint to check if it's ready
func waitForAdminServerReady(adminAddr string) error {
	healthAddr := fmt.Sprintf("%s/health", adminAddr)
	maxAttempts := 60 // 60 * 500ms = 30 seconds max wait
	attempt := 0
	client := &http.Client{
		Timeout: 1 * time.Second,
	}

	for attempt < maxAttempts {
		resp, err := client.Get(healthAddr)
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

// waitForWorkerReady polls the worker's gRPC port to ensure the worker has fully initialized
func waitForWorkerReady(workerGrpcAddr string) {
	maxAttempts := 30 // 30 * 200ms = 6 seconds max wait
	attempt := 0

	// Worker gRPC server doesn't have an HTTP endpoint, so we'll use a simple TCP connection attempt
	// as a synchronization point to ensure the worker has started listening
	for attempt < maxAttempts {
		conn, err := net.DialTimeout("tcp", workerGrpcAddr, 200*time.Millisecond)
		if err == nil {
			conn.Close()
			glog.V(1).Infof("Worker is ready at %s", workerGrpcAddr)
			return
		}
		attempt++
		time.Sleep(200 * time.Millisecond)
	}

	// Worker readiness check failed, but log as warning since worker may still be functional
	glog.Warningf("Worker readiness check timed out at %s (worker may still be functional)", workerGrpcAddr)
}

// startMiniWorker starts a single worker for the admin server
func startMiniWorker() {
	glog.Infof("Starting maintenance worker for admin server")

	adminAddr := fmt.Sprintf("%s:%d", *miniIp, *miniAdminOptions.port)
	capabilities := "vacuum,ec,balance"

	// Use worker directory under main data folder
	workerDir := filepath.Join(*miniDataFolders, "worker")
	if err := os.MkdirAll(workerDir, 0755); err != nil {
		glog.Fatalf("Failed to create worker directory: %v", err)
	}

	glog.Infof("Worker connecting to admin server: %s", adminAddr)
	glog.Infof("Worker capabilities: %s", capabilities)
	glog.Infof("Worker directory: %s", workerDir)

	// Parse capabilities
	capabilitiesParsed := parseCapabilities(capabilities)
	if len(capabilitiesParsed) == 0 {
		glog.Fatalf("No valid capabilities for worker")
	}

	// Create task directories
	for _, capability := range capabilitiesParsed {
		taskDir := filepath.Join(workerDir, string(capability))
		if err := os.MkdirAll(taskDir, 0755); err != nil {
			glog.Fatalf("Failed to create task directory %s: %v", taskDir, err)
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
		glog.Fatalf("Failed to create worker: %v", err)
	}

	// Create admin client
	adminClient, err := worker.CreateAdminClient(adminAddr, workerInstance.ID(), grpcDialOption)
	if err != nil {
		glog.Fatalf("Failed to create admin client: %v", err)
	}

	// Set admin client
	workerInstance.SetAdminClient(adminClient)

	// Metrics server is already started in the main init function above, so no need to start it again here

	// Start the worker
	err = workerInstance.Start()
	if err != nil {
		glog.Fatalf("Failed to start worker: %v", err)
	}

	glog.Infof("Maintenance worker %s started successfully", workerInstance.ID())
}

const welcomeMessageTemplate = `
╔═══════════════════════════════════════════════════════════════════════════════╗
║                      SeaweedFS Mini - All-in-One Mode                         ║
╚═══════════════════════════════════════════════════════════════════════════════╝

  All components are running and ready to use:

    Master UI:      http://%s:%d
    Filer UI:       http://%s:%d
    S3 Endpoint:    http://%s:%d
    WebDAV:         http://%s:%d
    Admin UI:       http://%s:%d
    Volume Server:  http://%s:%d

  Optimized Settings:
    • Volume size limit: %dMB
    • Volume max: auto (based on free disk space)
    • Pre-stop seconds: 1 (faster shutdown)
    • Master peers: none (single master mode)
    • Admin UI for management and maintenance tasks

  Data Directory: %s

  Press Ctrl+C to stop all components
`

const credentialsInstructionTemplate = `
  To create S3 credentials, you have two options:

  Option 1: Use environment variables (recommended for quick setup)
    export AWS_ACCESS_KEY_ID=your-access-key
    export AWS_SECRET_ACCESS_KEY=your-secret-key
    weed mini -dir=/data
    This will create initial credentials for the 'mini' user.

  Option 2: Use the Admin UI
    Open: http://%s:%d
    Add a new identity to create S3 credentials.
`

const credentialsCreatedMessage = `
  Initial S3 credentials created:
    user: mini
    Note: credentials have been written to the IAM configuration file.
`

// printWelcomeMessage prints the welcome message after all services are running
func printWelcomeMessage() {
	fmt.Printf(welcomeMessageTemplate,
		*miniIp, *miniMasterOptions.port,
		*miniIp, *miniFilerOptions.port,
		*miniIp, *miniS3Options.port,
		*miniIp, *miniWebDavOptions.port,
		*miniIp, *miniAdminOptions.port,
		*miniIp, *miniOptions.v.port,
		*miniMasterOptions.volumeSizeLimitMB,
		*miniDataFolders,
	)

	if createdInitialIAM {
		fmt.Print(credentialsCreatedMessage)
	} else {
		fmt.Printf(credentialsInstructionTemplate, *miniIp, *miniAdminOptions.port)
	}
	fmt.Println("")
}
