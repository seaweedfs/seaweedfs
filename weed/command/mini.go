package command

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/bits"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	pluginworker "github.com/seaweedfs/seaweedfs/weed/plugin/worker"
	"github.com/seaweedfs/seaweedfs/weed/s3api"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3bucket"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"github.com/seaweedfs/seaweedfs/weed/security"
	stats_collect "github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"
	flag "github.com/seaweedfs/seaweedfs/weed/util/fla9"
	"github.com/seaweedfs/seaweedfs/weed/util/grace"
	"github.com/seaweedfs/seaweedfs/weed/util/version"
	"github.com/seaweedfs/seaweedfs/weed/worker"
	"github.com/seaweedfs/seaweedfs/weed/worker/tasks/vacuum"
	"github.com/seaweedfs/seaweedfs/weed/worker/types"
	"golang.org/x/term"

	// Import task packages to trigger their auto-registration
	_ "github.com/seaweedfs/seaweedfs/weed/worker/tasks/balance"
	_ "github.com/seaweedfs/seaweedfs/weed/worker/tasks/erasure_coding"
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
	defaultMiniPluginJobTypes     = "all"
)

var (
	miniOptions       MiniOptions
	miniMasterOptions MasterOptions
	miniFilerOptions  FilerOptions
	miniS3Options     S3Options
	miniWebDavOptions WebDavOption
	miniAdminOptions  AdminOptions
	// Track which port flags were explicitly passed on CLI before config file is applied
	explicitPortFlags map[string]bool
	miniEnableWebDAV  *bool
	miniEnableS3      *bool
	miniEnableAdminUI *bool
	miniS3IamReadOnly *bool
	// MiniClusterCtx is the context for the mini cluster. If set, the mini cluster will stop when the context is cancelled.
	MiniClusterCtx context.Context

	miniProgressBoard *miniProgress
)

// miniProgress renders a docker-compose-style multi-line status table for
// mini's startup and shutdown phases. On a TTY each service has a single
// in-place row that updates from pending -> starting -> ready (or stopping
// -> stopped). When stdout isn't a TTY it falls back to one printed line per
// state change so log capture stays readable. All access is mutex-guarded so
// concurrent goroutines (S3 and WebDAV start in parallel) don't tear updates.
type miniProgress struct {
	mu       sync.Mutex
	order    []string
	states   map[string]string
	starts   map[string]time.Time
	elapsed  map[string]time.Duration
	isTTY    bool
	rendered bool
}

const miniProgressNameWidth = 12

func newMiniProgress(services []string) *miniProgress {
	p := &miniProgress{
		order:   append([]string(nil), services...),
		states:  make(map[string]string, len(services)),
		starts:  make(map[string]time.Time, len(services)),
		elapsed: make(map[string]time.Duration, len(services)),
		isTTY:   term.IsTerminal(int(os.Stdout.Fd())),
	}
	for _, name := range services {
		p.states[name] = "pending"
	}
	if p.isTTY {
		// Initial render so each subsequent update can move the cursor up by
		// len(order) rows and overwrite in place.
		p.renderLocked()
	}
	return p
}

func (p *miniProgress) update(name, state string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, known := p.states[name]; !known {
		return
	}
	switch state {
	case "starting", "stopping":
		p.starts[name] = time.Now()
	case "ready", "stopped":
		if started, ok := p.starts[name]; ok {
			p.elapsed[name] = time.Since(started)
		}
	}
	p.states[name] = state
	if p.isTTY {
		p.renderLocked()
		return
	}
	// Non-TTY: print exactly one line for this transition. Pending lines aren't
	// announced (they'd be every service on init); starting/ready/etc. are.
	fmt.Println(p.formatRow(name))
}

func (p *miniProgress) starting(name string) { p.update(name, "starting") }
func (p *miniProgress) ready(name string)    { p.update(name, "ready") }
func (p *miniProgress) failed(name string)   { p.update(name, "failed") }
func (p *miniProgress) stopped(name string)  { p.update(name, "stopped") }

// renderLocked redraws every row in the board. Caller must hold p.mu and
// p.isTTY must be true. Uses CSI sequences: ESC[<N>A moves cursor up N lines,
// ESC[2K clears the line — leaving the cursor parked one line below the last
// row so the next print (welcome banner, shutdown messages) flows naturally.
func (p *miniProgress) renderLocked() {
	if p.rendered {
		fmt.Printf("\033[%dA", len(p.order))
	}
	for _, name := range p.order {
		fmt.Print("\r\033[2K")
		fmt.Println(p.formatRow(name))
	}
	p.rendered = true
}

func (p *miniProgress) formatRow(name string) string {
	state := p.states[name]
	switch state {
	case "ready", "stopped":
		return fmt.Sprintf("  %-*s %-9s %.1fs", miniProgressNameWidth, name, state, p.elapsed[name].Seconds())
	default:
		return fmt.Sprintf("  %-*s %s", miniProgressNameWidth, name, state)
	}
}

// reset clears the rendered state so the board can be redrawn as a fresh
// table with all rows in initialState — used to switch from the startup
// phase to the shutdown phase so shutdown rows don't try to overwrite the
// already-printed startup rows.
func (p *miniProgress) reset(services []string, initialState string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.order = append([]string(nil), services...)
	p.states = make(map[string]string, len(services))
	p.starts = make(map[string]time.Time, len(services))
	p.elapsed = make(map[string]time.Duration, len(services))
	now := time.Now()
	for _, name := range services {
		p.states[name] = initialState
		p.starts[name] = now
	}
	p.rendered = false
	if p.isTTY {
		p.renderLocked()
	}
}

// reportMiniStopped marks a service as fully stopped on the progress board.
// Safe to call when the board is nil (mini not running) — used from defers
// in service goroutines so a normal exit (Ctrl+C, ctx cancel) flips the row.
func reportMiniStopped(name string) {
	if miniProgressBoard != nil {
		miniProgressBoard.stopped(name)
	}
}

// miniStartupServices lists components in the order they appear in the
// startup progress board. Matches the welcome banner order so visual
// continuity carries from startup through to the running summary.
func miniStartupServices() []string {
	services := []string{"Master", "Volume", "Filer"}
	if miniEnableWebDAV != nil && *miniEnableWebDAV {
		services = append(services, "WebDAV")
	}
	if miniEnableS3 != nil && *miniEnableS3 {
		services = append(services, "S3")
		if miniS3Options.portIceberg != nil && *miniS3Options.portIceberg > 0 {
			services = append(services, "Iceberg")
		}
	}
	services = append(services, "Admin")
	return services
}

// miniClientsState orchestrates graceful shutdown of admin/s3/webdav/worker on
// weed mini Ctrl+C, BEFORE filer/volume/master tear down. It is rebuilt on
// each runMini invocation so in-process test reruns see fresh state.
//
// The ctx chains from MiniClusterCtx so cancelling MiniClusterCtx (how tests
// tear down the cluster) also triggers the client-shutdown path.
//
// Shutdown has two phases:
//  1. preCancelFns run synchronously in registration order (worker disconnect,
//     etc.) so downstream servers don't block on handlers that are about to
//     close anyway.
//  2. ctx is cancelled and the shutdown hook waits on wg for admin/s3/webdav
//     goroutines (registered via onMiniClientsShutdown / trackMiniClient) to
//     drain.
type miniClientsState struct {
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	preCancelMu  sync.Mutex
	preCancelFns []func()
}

var miniClients *miniClientsState

// resetMiniClients installs a fresh client-shutdown state chained from
// MiniClusterCtx. Called once at the top of runMini; any goroutines from a
// prior invocation keep their old state via closure and are unaffected.
func resetMiniClients() {
	parent := context.Background()
	if MiniClusterCtx != nil {
		parent = MiniClusterCtx
	}
	s := &miniClientsState{}
	s.ctx, s.cancel = context.WithCancel(parent)
	miniClients = s
}

// trackMiniClient registers an externally-managed goroutine (one that
// observes miniClientsCtx() itself) so the interrupt hook waits for it.
// The caller invokes the returned done func when the goroutine exits.
func trackMiniClient() (done func()) {
	s := miniClients
	if s == nil {
		return func() {}
	}
	s.wg.Add(1)
	return s.wg.Done
}

// beforeMiniClientsShutdown registers fn to run synchronously BEFORE the
// clients ctx is cancelled. Use for cleanup that must complete before
// downstream servers (e.g., the admin worker-gRPC) start waiting on clients.
func beforeMiniClientsShutdown(fn func()) {
	s := miniClients
	if s == nil {
		return
	}
	s.preCancelMu.Lock()
	defer s.preCancelMu.Unlock()
	s.preCancelFns = append(s.preCancelFns, fn)
}

// miniClientsCtx returns the shutdown context for mini clients, or
// context.Background() if not running inside weed mini.
func miniClientsCtx() context.Context {
	s := miniClients
	if s == nil {
		return context.Background()
	}
	return s.ctx
}

// triggerMiniClientsShutdown runs preCancel fns synchronously, cancels the
// clients ctx, and waits up to timeout for tracked goroutines to finish.
// Called from the OnInterrupt hook.
func triggerMiniClientsShutdown(timeout time.Duration) {
	s := miniClients
	if s == nil {
		return
	}
	if miniProgressBoard != nil {
		// Switch the progress board from startup mode to shutdown mode. All
		// services start as "stopping"; each goroutine's defer reportMiniStopped
		// flips its own row to "stopped" when it actually drains.
		fmt.Println("\n  Shutting down SeaweedFS Mini ...")
		miniProgressBoard.reset(miniStartupServices(), "stopping")
	}
	glog.V(0).Infof("Shutting down admin/s3/webdav ...")
	s.preCancelMu.Lock()
	fns := s.preCancelFns
	s.preCancelFns = nil
	s.preCancelMu.Unlock()
	for _, fn := range fns {
		fn()
	}
	s.cancel()
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		glog.V(0).Infof("admin/s3/webdav shut down")
	case <-time.After(timeout):
		glog.V(0).Infof("timed out waiting for admin/s3/webdav to shut down")
	}
}

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
	weed mini -dir=/data -bucket=my-bucket             # Pre-create an S3 bucket on startup
	weed mini -dir=/data -bucket=bucket1,bucket2       # Pre-create multiple S3 buckets
	weed mini -dir=/data -tableBucket=iceberg-tables   # Pre-create an S3 Tables bucket

After starting, you can access:
- Master UI:       http://localhost:9333
- Volume Server:   http://localhost:9340
- Filer UI:        http://localhost:8888
- S3 Endpoint:     http://localhost:8333
- Iceberg Catalog: http://localhost:8181
- WebDAV:          http://localhost:7333
- Admin UI:        http://localhost:23646

S3 Access:
The S3 endpoint is available at http://localhost:8333. For client
configuration and IAM setup, see the project documentation or use the
Admin UI (http://localhost:23646) to manage users and policies.

`,
}

var (
	miniIp                          = cmdMini.Flag.String("ip", util.DetectedHostAddress(), "ip or server name, also used as identifier")
	miniBindIp                      = cmdMini.Flag.String("ip.bind", "0.0.0.0", "ip address to bind to. If empty, default to same as -ip option.")
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
	miniBucket                      = cmdMini.Flag.String("bucket", "", "comma-separated S3 bucket names to create on startup if they do not already exist; leave empty to skip. Falls back to S3_BUCKET env var.")
	miniTableBucket                 = cmdMini.Flag.String("tableBucket", "", "comma-separated S3 Tables bucket names to create on startup if they do not already exist; leave empty to skip. Falls back to S3_TABLE_BUCKET env var.")
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
	miniEnableWebDAV = cmdMini.Flag.Bool("webdav", true, "enable WebDAV server")
	miniEnableS3 = cmdMini.Flag.Bool("s3", true, "enable S3 server")
	miniEnableAdminUI = cmdMini.Flag.Bool("admin.ui", true, "enable Admin UI")
	miniS3IamReadOnly = cmdMini.Flag.Bool("s3.iam.readOnly", true, "disable IAM write operations on this server")
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
	miniMasterOptions.raftResumeState = cmdMini.Flag.Bool("master.resumeState", true, "resume previous state on start master server")
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
	miniOptions.v.tags = cmdMini.Flag.String("volume.tags", "", "comma-separated tag groups per data dir; each group uses ':' (e.g. fast:ssd,archive)")
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
	miniOptions.v.allowUntrustedRemoteEndpoints = cmdMini.Flag.Bool("volume.allowUntrustedRemoteEndpoints", false, "if true, FetchAndWriteNeedle accepts arbitrary remote S3 endpoints including loopback / link-local hosts. Default rejects internal / metadata endpoints.")
	miniOptions.v.preStopSeconds = cmdMini.Flag.Int("volume.preStopSeconds", 1, "number of seconds between stop send heartbeats and stop volume server (default: 1 for mini)")
	miniOptions.v.setDiskIOProbeDefaults()
}

// initMiniS3Flags initializes S3 server flag options
func initMiniS3Flags() {
	miniS3Options.port = cmdMini.Flag.Int("s3.port", 8333, "s3 server http listen port")
	miniS3Options.portHttps = cmdMini.Flag.Int("s3.port.https", 0, "s3 server https listen port")
	miniS3Options.portGrpc = cmdMini.Flag.Int("s3.port.grpc", 0, "s3 server grpc listen port")
	miniS3Options.portIceberg = cmdMini.Flag.Int("s3.port.iceberg", 8181, "Iceberg REST Catalog server listen port (0 to disable)")
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
	miniS3Options.iamReadOnly = miniS3IamReadOnly
	miniS3Options.dataCenter = cmdMini.Flag.String("s3.dataCenter", "", "prefer to read and write to volumes in this data center")
	miniS3Options.cipher = cmdMini.Flag.Bool("s3.encryptVolumeData", false, "encrypt data on volume servers for S3 uploads")
	miniS3Options.config = miniS3Config
	miniS3Options.iamConfig = miniIamConfig
	miniS3Options.auditLogConfig = cmdMini.Flag.String("s3.auditLogConfig", "", "path to the audit log config file")
	miniS3Options.allowDeleteBucketNotEmpty = miniS3AllowDeleteBucketNotEmpty
	miniS3Options.externalUrl = cmdMini.Flag.String("s3.externalUrl", "", "the external URL clients use to connect (e.g. https://api.example.com:9000). Used for S3 signature verification behind a reverse proxy. Falls back to S3_EXTERNAL_URL env var.")
	miniS3Options.defaultFileMode = cmdMini.Flag.String("s3.defaultFileMode", "", "default file mode for S3 uploaded objects, e.g. 0660, 0644, 0666")
	miniS3Options.cacheSizeMB = cmdMini.Flag.Int64("s3.cacheCapacityMB", 0, "in-memory chunk cache capacity in MB for S3 GETs shared across requests (0 disables)")
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
	miniAdminOptions.urlPrefix = cmdMini.Flag.String("admin.urlPrefix", "", "URL path prefix when running the admin UI behind a reverse proxy under a subdirectory (e.g. /seaweedfs)")
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

	glog.V(1).Infof("Optimal volume size: %dMB (total disk capacity: %dMB, capacity/100 before rounding: %dMB, rounded to nearest power of 2, clamped to [%d,%d]MB)",
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

// quietMiniLogs routes info-level glog output to the log file only, leaving
// stderr for warnings and errors. The welcome banner (printWelcomeMessage)
// uses fmt.Print so it is unaffected. If the user explicitly set any glog
// flag, leave their choice alone — they want control of the output.
func quietMiniLogs() {
	userOverride := false
	flag.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "v", "logtostderr", "alsologtostderr", "stderrthreshold":
			userOverride = true
		}
	})
	if userOverride {
		return
	}
	_ = flag.Set("alsologtostderr", "false")
	_ = flag.Set("stderrthreshold", "WARNING")
}

// ensureMiniDevSSES3Keys silences two startup messages that would otherwise
// dominate mini's output even with quietMiniLogs():
//
//  1. the SSE-S3 "KEK will be stored in plaintext" warning from s3_sse_s3.go
//     (cleared by providing s3.sse.kek.passphrase), and
//  2. the IAM "no signing key found for STS service" error from s3api_server.go
//     (cleared by populating the SSE-S3 master KEK so the IAM loader's fallback
//     at s3api_server.go:1044 can derive an STS signing key from it).
//
// Values are exported as WEED_* env vars rather than written via viper.Set:
// viper treats dots as a path delimiter, so Set("s3.sse.kek.passphrase", ...)
// and Set("s3.sse.kek", ...) overwrite each other's subtree. Env vars are flat
// and viper picks them up through AutomaticEnv()+SetEnvPrefix("weed").
//
// Both secrets are random and persisted under the data folder on first run, so
// later runs reuse the same key — necessary because s3.sse.kek is checked for
// equality against any existing KEK file on the filer. If the operator already
// configured one or both via security.toml or WEED_ env vars, their values win.
func ensureMiniDevSSES3Keys(dataFolder string) {
	topDir := util.StringSplit(dataFolder, ",")[0]
	if topDir == "" {
		return
	}
	if err := os.MkdirAll(topDir, 0755); err != nil {
		glog.Warningf("mini: failed to create data folder for dev SSE-S3 keys: %v", err)
		return
	}

	// (1) KEK passphrase — wraps the KEK on the filer at rest, also gates the
	//     plaintext-KEK warning at s3_sse_s3.go:862. Any non-empty string works.
	if util.GetViper().GetString("s3.sse.kek.passphrase") == "" && os.Getenv("WEED_S3_SSE_KEK_PASSPHRASE") == "" {
		if pp := loadOrCreateMiniHexSecret(filepath.Join(topDir, ".mini_kek_passphrase"), 32); pp != "" {
			_ = os.Setenv("WEED_S3_SSE_KEK_PASSPHRASE", pp)
		}
	}

	// (2) KEK — must be exactly 32 bytes hex-encoded. Setting this populates
	//     SSES3KeyManager.superKey so GetMasterKey() returns a derived STS
	//     signing key instead of nil, satisfying the IAM fallback.
	hasOperatorKEK := util.GetViper().GetString("s3.sse.kek") != "" || util.GetViper().GetString("s3.sse.key") != "" ||
		os.Getenv("WEED_S3_SSE_KEK") != "" || os.Getenv("WEED_S3_SSE_KEY") != ""
	if !hasOperatorKEK {
		if kek := loadOrCreateMiniHexSecret(filepath.Join(topDir, ".mini_sse_kek"), 32); kek != "" {
			_ = os.Setenv("WEED_S3_SSE_KEK", kek)
		}
	}
}

// loadOrCreateMiniHexSecret loads a hex-encoded secret from path or generates
// nBytes of randomness on first call, persists it (0600), and returns the hex
// string. Returns "" if reading fails, generation fails, OR persistence fails
// — refusing to return an in-memory-only secret is deliberate: SSE-S3 writes
// data encrypted under this key, and a later restart that can't read the
// persisted secret would generate a fresh one and orphan whatever was written.
// Better to leave SSE-S3 disabled this run than to silently lose data later.
func loadOrCreateMiniHexSecret(path string, nBytes int) string {
	if data, err := os.ReadFile(path); err == nil {
		if s := strings.TrimSpace(string(data)); s != "" {
			return s
		}
	}
	buf := make([]byte, nBytes)
	if _, err := rand.Read(buf); err != nil {
		glog.Warningf("mini: failed to generate dev secret for %s: %v", path, err)
		return ""
	}
	s := hex.EncodeToString(buf)
	if err := os.WriteFile(path, []byte(s), 0600); err != nil {
		glog.Warningf("mini: failed to persist dev secret to %s: %v (skipping; SSE-S3/IAM stay disabled this run to avoid orphaning data on next restart)", path, err)
		return ""
	}
	return s
}

// ensureMiniVolumeGrowthDefaults keeps a small mini cluster usable with many
// S3 buckets. Mini auto-sizes its data disk into a handful of large (up to
// 1 GiB) volume slots, but the master pre-grows copy_1 (default 7) volumes for
// every new collection. Under a filer group each bucket is its own collection,
// so the first couple of buckets' writes claim every slot and later buckets
// can no longer grow a volume — their PutObjects fail with
// "assign volume: ... no free volumes". Grow one volume at a time so the slots
// stretch across many collections. Anything the operator already set via
// master.toml or a WEED_ env var wins.
func ensureMiniVolumeGrowthDefaults() {
	v := util.GetViper()
	for _, key := range []string{
		"master.volume_growth.copy_1",
		"master.volume_growth.copy_2",
		"master.volume_growth.copy_3",
		"master.volume_growth.copy_other",
	} {
		envKey := "WEED_" + strings.ToUpper(strings.ReplaceAll(key, ".", "_"))
		if v.IsSet(key) || os.Getenv(envKey) != "" {
			continue
		}
		v.Set(key, 1)
	}
}

// isPortOpenOnIP checks if a port is available for binding on a specific IP address
func isPortOpenOnIP(ip string, port int) bool {
	if port <= 0 || port > 65535 {
		return false
	}
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
	if port <= 0 || port > 65535 {
		return false
	}
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
		if port > 65535 {
			// Wrap around to a lower range if we exceed 65535
			port = 10000 + (port % 65535)
		}
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
	// Check if this port was explicitly specified by the user (from CLI, before config file was applied)
	isExplicitPort := explicitPortFlags[flagName]

	if *portPtr == 0 {
		return nil
	}

	original := *portPtr

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
	}
	if *miniEnableS3 {
		portConfigs = append(portConfigs, struct {
			port     *int
			name     string
			flagName string
			grpcPtr  *int
		}{miniS3Options.port, "S3", "s3.port", miniS3Options.portGrpc})
		if miniS3Options.portIceberg != nil && *miniS3Options.portIceberg > 0 {
			portConfigs = append(portConfigs, struct {
				port     *int
				name     string
				flagName string
				grpcPtr  *int
			}{miniS3Options.portIceberg, "Iceberg", "s3.port.iceberg", nil})
		}
	}
	portConfigs = append(portConfigs, struct {
		port     *int
		name     string
		flagName string
		grpcPtr  *int
	}{miniWebDavOptions.port, "WebDAV", "webdav.port", nil},
		struct {
			port     *int
			name     string
			flagName string
			grpcPtr  *int
		}{miniAdminOptions.port, "Admin", "admin.port", miniAdminOptions.grpcPort})

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
	icebergPortStr := "disabled"
	if miniS3Options.portIceberg != nil && *miniS3Options.portIceberg > 0 {
		icebergPortStr = fmt.Sprintf("%d", *miniS3Options.portIceberg)
	}
	glog.V(1).Infof("Final port configuration - Master: %d, Filer: %d, Volume: %d, S3: %d, Iceberg: %s, WebDAV: %d, Admin: %d",
		*miniMasterOptions.port, *miniFilerOptions.port, *miniOptions.v.port,
		*miniS3Options.port, icebergPortStr, *miniWebDavOptions.port, *miniAdminOptions.port)

	// Log gRPC ports too (now finalized)
	glog.V(1).Infof("gRPC port configuration - Master: %d, Filer: %d, Volume: %d, S3: %d, Admin: %d",
		*miniMasterOptions.portGrpc, *miniFilerOptions.portGrpc, *miniOptions.v.portGrpc,
		*miniS3Options.portGrpc, *miniAdminOptions.grpcPort)

	return nil
}

// initializeGrpcPortsOnIP initializes all gRPC ports based on their HTTP ports on a specific IP
// If a gRPC port is 0, it will be set to httpPort + GrpcPortOffset
// This must be called after HTTP ports are finalized and before services start
func initializeGrpcPortsOnIP(bindIp string) {
	// Track all ports allocated (both HTTP and gRPC) to prevent collisions.
	// We must reserve HTTP ports so that gRPC fallback allocation never picks
	// a port already assigned to an HTTP service (which hasn't bound yet).
	allocatedPorts := make(map[int]bool)

	// Reserve all HTTP ports first
	allocatedPorts[*miniMasterOptions.port] = true
	allocatedPorts[*miniFilerOptions.port] = true
	allocatedPorts[*miniOptions.v.port] = true
	allocatedPorts[*miniWebDavOptions.port] = true
	allocatedPorts[*miniAdminOptions.port] = true
	if *miniEnableS3 {
		allocatedPorts[*miniS3Options.port] = true
		if miniS3Options.portIceberg != nil && *miniS3Options.portIceberg > 0 {
			allocatedPorts[*miniS3Options.portIceberg] = true
		}
	}

	grpcConfigs := []struct {
		httpPort *int
		grpcPort *int
		name     string
	}{
		{miniMasterOptions.port, miniMasterOptions.portGrpc, "Master"},
		{miniFilerOptions.port, miniFilerOptions.portGrpc, "Filer"},
		{miniOptions.v.port, miniOptions.v.portGrpc, "Volume"},
	}
	if *miniEnableS3 {
		grpcConfigs = append(grpcConfigs, struct {
			httpPort *int
			grpcPort *int
			name     string
		}{miniS3Options.port, miniS3Options.portGrpc, "S3"})
	}
	grpcConfigs = append(grpcConfigs, struct {
		httpPort *int
		grpcPort *int
		name     string
	}{miniAdminOptions.port, miniAdminOptions.grpcPort, "Admin"})

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
		if !isPortOpenOnIP(bindIp, *config.grpcPort) || !isPortAvailable(*config.grpcPort) || allocatedPorts[*config.grpcPort] {
			glog.Warningf("gRPC port %d for %s is not available, finding alternative...", *config.grpcPort, config.name)
			originalPort := *config.grpcPort
			newPort := findAvailablePortOnIP(bindIp, originalPort+1, 100, allocatedPorts)
			if newPort == 0 {
				glog.Errorf("Could not find available gRPC port for %s starting from %d, will use %d and fail on binding", config.name, originalPort+1, originalPort)
			} else {
				glog.Infof("gRPC port %d for %s is available, using it instead of %d", newPort, config.name, originalPort)
				*config.grpcPort = newPort
			}
		}
		allocatedPorts[*config.grpcPort] = true
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

	glog.V(1).Infof("Loaded %d options from configuration file %s", len(options), configFile)
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
				if err := flag.Value.Set(value); err != nil {
					glog.Warningf("Failed to apply config file option: %s=%s: %v", key, value, err)
				} else {
					glog.V(2).Infof("Applied config file option: %s=%s", key, value)
				}
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

	glog.V(1).Infof("Mini configuration saved to %s", configFile)
	return nil
}

func runMini(cmd *Command, args []string) bool {
	*miniDataFolders = util.ResolveCommaSeparatedPaths(*miniDataFolders)

	// Mini is meant to be quiet by default: only the welcome banner and any
	// warnings/errors should reach stderr. Skip if the user passed any glog
	// flag (-v / -logtostderr / -alsologtostderr / -stderrthreshold) so they
	// can opt back into verbose output.
	quietMiniLogs()

	// Capture which port flags were explicitly passed on CLI BEFORE config file is applied
	// This is necessary to distinguish user-specified ports from defaults or config file options
	explicitPortFlags = make(map[string]bool)
	portFlagNames := []string{"master.port", "filer.port", "volume.port", "s3.port", "s3.port.iceberg", "webdav.port", "admin.port", "s3.iam.readOnly"}
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
	util.LoadConfiguration("volume", false)
	util.LoadConfiguration("admin", false)
	miniOptions.v.applyDiskIOProbeConfig()

	ensureMiniVolumeGrowthDefaults()

	// applyConfigFileOptions above may have overwritten -dir from the
	// mini.options file, so re-resolve it here alongside the other paths.
	*miniDataFolders = util.ResolveCommaSeparatedPaths(*miniDataFolders)

	// Seed stable dev SSE-S3 keys before any service starts, so the SSE-S3
	// plaintext-KEK warning and the IAM "no signing key" error don't fire.
	// Does nothing for keys the operator has already configured.
	ensureMiniDevSSES3Keys(*miniDataFolders)

	*miniOptions.cpuprofile = util.ResolvePath(*miniOptions.cpuprofile)
	*miniOptions.memprofile = util.ResolvePath(*miniOptions.memprofile)
	*miniS3Config = util.ResolvePath(*miniS3Config)
	*miniIamConfig = util.ResolvePath(*miniIamConfig)
	*miniMasterOptions.metaFolder = util.ResolvePath(*miniMasterOptions.metaFolder)
	*miniAdminOptions.dataDir = util.ResolvePath(*miniAdminOptions.dataDir)
	miniS3Options.resolvePaths()
	miniWebDavOptions.resolvePaths()
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

	// Share the S3 static identity config file with the filer so its
	// credential manager can also serve static users.
	miniFilerOptions.s3ConfigFile = miniS3Config

	filerAddress := string(pb.NewServerAddress(*miniIp, *miniFilerOptions.port, *miniFilerOptions.portGrpc))
	miniS3Options.filer = &filerAddress
	miniWebDavOptions.filer = &filerAddress

	// Register Unix socket paths for gRPC services so local inter-service
	// communication goes through Unix sockets instead of TCP.
	pb.RegisterLocalGrpcSocket(*miniIp, *miniMasterOptions.portGrpc, fmt.Sprintf("/tmp/seaweedfs-master-grpc-%d.sock", *miniMasterOptions.portGrpc))
	pb.RegisterLocalGrpcSocket(*miniIp, *miniOptions.v.portGrpc, fmt.Sprintf("/tmp/seaweedfs-volume-grpc-%d.sock", *miniOptions.v.portGrpc))
	pb.RegisterLocalGrpcSocket(*miniIp, *miniFilerOptions.portGrpc, fmt.Sprintf("/tmp/seaweedfs-filer-grpc-%d.sock", *miniFilerOptions.portGrpc))
	if *miniS3Options.portGrpc > 0 {
		pb.RegisterLocalGrpcSocket(*miniIp, *miniS3Options.portGrpc, fmt.Sprintf("/tmp/seaweedfs-s3-grpc-%d.sock", *miniS3Options.portGrpc))
	}
	pb.RegisterLocalGrpcSocket(*miniIp, *miniAdminOptions.grpcPort, fmt.Sprintf("/tmp/seaweedfs-admin-grpc-%d.sock", *miniAdminOptions.grpcPort))

	go stats_collect.StartMetricsServer(*miniMetricsHttpIp, *miniMetricsHttpPort)

	if *miniMasterOptions.volumeSizeLimitMB > util.VolumeSizeLimitGB*1000 {
		glog.Fatalf("masterVolumeSizeLimitMB should be less than 30000")
	}

	if *miniMasterOptions.metaFolder == "" {
		// -dir may be comma-separated (dir[,dir]...); the master expects a
		// single directory, so default to the first entry. Both miniDataFolders
		// and miniMasterOptions.metaFolder were already tilde-resolved at the
		// top of runMini.
		*miniMasterOptions.metaFolder = util.StringSplit(*miniDataFolders, ",")[0]
	}
	if err := util.TestFolderWritable(*miniMasterOptions.metaFolder); err != nil {
		glog.Fatalf("Check Meta Folder (-dir=\"%s\") Writable: %s", *miniMasterOptions.metaFolder, err)
	}
	miniFilerOptions.defaultLevelDbDirectory = miniMasterOptions.metaFolder

	// Calculate and set optimal volume size limit based on available disk space
	// Only auto-calculate if user didn't explicitly specify a value via -master.volumeSizeLimitMB
	if !isFlagPassed("master.volumeSizeLimitMB") {
		// User didn't override, use auto-calculated value
		// The -dir flag can accept comma-separated directories; use the first one for disk space calculation.
		// miniDataFolders was already tilde-resolved at the top of runMini.
		optimalVolumeSizeMB := calculateOptimalVolumeSizeMB(util.StringSplit(*miniDataFolders, ",")[0])
		miniMasterOptions.volumeSizeLimitMB = &optimalVolumeSizeMB
		glog.V(1).Infof("Mini started with auto-calculated optimal volume size limit: %dMB", optimalVolumeSizeMB)
	} else {
		// User specified a custom value
		glog.V(1).Infof("Mini started with user-specified volume size limit: %dMB", *miniMasterOptions.volumeSizeLimitMB)
	}

	miniWhiteList := util.StringSplit(*miniWhiteListOption, ",")

	// Install a fresh clients-shutdown context (chained from MiniClusterCtx)
	// before any service starts.
	resetMiniClients()

	// Master/volume/filer observe MiniClusterCtx so tests that cancel it
	// tear those services down too. On Ctrl+C they rely on their own
	// OnInterrupt hooks (see grace's LIFO ordering).
	miniMasterOptions.shutdownCtx = MiniClusterCtx
	miniOptions.v.shutdownCtx = MiniClusterCtx
	miniFilerOptions.shutdownCtx = MiniClusterCtx
	// Mini is a small/dev setup with short-lived RPCs; cap the filer's
	// gRPC graceful-stop at 1s so Ctrl+C returns quickly instead of sitting
	// on the default 10s waiting for background subscription streams.
	miniFilerOptions.gracefulStopTimeout = 1 * time.Second

	// Start all services with proper dependency coordination
	// This channel will be closed when all services are fully ready
	fmt.Println("\n  Starting SeaweedFS Mini ...")
	miniProgressBoard = newMiniProgress(miniStartupServices())
	allServicesReady := make(chan struct{})
	startMiniServices(miniWhiteList, allServicesReady)

	// Wait for all services to be fully running before printing welcome message
	<-allServicesReady

	// Register the clients-shutdown interrupt hook AFTER all services have
	// registered theirs. Under grace's LIFO firing, this hook runs FIRST on
	// Ctrl+C so admin/s3/webdav drain before filer/volume/master tear down.
	grace.OnInterrupt(func() {
		triggerMiniClientsShutdown(10 * time.Second)
	})

	// Create the requested bucket(s) (if any) before announcing readiness.
	bucketSpec := *miniBucket
	if bucketSpec == "" {
		bucketSpec = os.Getenv("S3_BUCKET")
	}
	if err := ensureMiniBuckets(bucketSpec); err != nil {
		glog.Warningf("failed to ensure buckets %q: %v", bucketSpec, err)
	}
	tableBucketSpec := *miniTableBucket
	if tableBucketSpec == "" {
		tableBucketSpec = os.Getenv("S3_TABLE_BUCKET")
	}
	if err := ensureMiniTableBuckets(tableBucketSpec); err != nil {
		glog.Warningf("failed to ensure table buckets %q: %v", tableBucketSpec, err)
	}

	// Print welcome message after all services are running
	printWelcomeMessage()

	// Save configuration to file for persistence and documentation
	if err := saveMiniConfiguration(*miniDataFolders); err != nil {
		glog.Warningf("failed to save mini configuration in %s: %v", *miniDataFolders, err)
	}

	if MiniClusterCtx != nil {
		<-MiniClusterCtx.Done()
	} else {
		select {}
	}
	return true
}

// startMiniServices starts all mini services with proper dependency coordination
func startMiniServices(miniWhiteList []string, allServicesReady chan struct{}) {
	// Determine bind IP for health checks
	bindIp := getBindIp()

	// Start Master server (no dependencies)
	go func() {
		defer reportMiniStopped("Master")
		startMiniService("Master", func() {
			startMaster(miniMasterOptions, miniWhiteList)
		}, *miniMasterOptions.port)
	}()

	// Wait for master to be ready
	waitForServiceReady("Master", *miniMasterOptions.port, bindIp)

	// Start Volume server (depends on master)
	go func() {
		defer reportMiniStopped("Volume")
		startMiniService("Volume", func() {
			minFreeSpaces := util.MustParseMinFreeSpace(miniVolumeMinFreeSpace, "")
			miniOptions.v.startVolumeServer(*miniDataFolders, miniVolumeMaxDataVolumeCounts, *miniWhiteListOption, minFreeSpaces)
		}, *miniOptions.v.port)
	}()

	// Wait for volume to be ready
	waitForServiceReady("Volume", *miniOptions.v.port, bindIp)

	// Start Filer (depends on master and volume)
	go func() {
		defer reportMiniStopped("Filer")
		startMiniService("Filer", func() {
			miniFilerOptions.startFiler()
		}, *miniFilerOptions.port)
	}()

	// Wait for filer to be ready
	waitForServiceReady("Filer", *miniFilerOptions.port, bindIp)

	// Start S3 and WebDAV in parallel (both depend on filer). Each observes
	// miniClientsCtx so it shuts down first on Ctrl+C, tracked via
	// trackMiniClient so runMini's interrupt hook can wait for them.
	if *miniEnableS3 {
		miniS3Options.shutdownCtx = miniClientsCtx()
		done := trackMiniClient()
		go func() {
			defer done()
			defer reportMiniStopped("S3")
			// Iceberg lives inside the S3 server; report it stopped alongside.
			if miniS3Options.portIceberg != nil && *miniS3Options.portIceberg > 0 {
				defer reportMiniStopped("Iceberg")
			}
			startMiniService("S3", startS3Service, *miniS3Options.port)
		}()
	}

	if *miniEnableWebDAV {
		miniWebDavOptions.shutdownCtx = miniClientsCtx()
		done := trackMiniClient()
		go func() {
			defer done()
			defer reportMiniStopped("WebDAV")
			startMiniService("WebDAV", func() {
				miniWebDavOptions.startWebDav()
			}, *miniWebDavOptions.port)
		}()
	}

	// Wait for services to be ready
	if *miniEnableS3 {
		waitForServiceReady("S3", *miniS3Options.port, bindIp)
		if miniS3Options.portIceberg != nil && *miniS3Options.portIceberg > 0 {
			// Iceberg is started inside the S3 server, not via startMiniService,
			// so announce it here for symmetry with the other progress lines.
			if miniProgressBoard != nil {
				miniProgressBoard.starting("Iceberg")
			}
			waitForServiceReady("Iceberg", *miniS3Options.portIceberg, bindIp)
		}
	}
	if *miniEnableWebDAV {
		waitForServiceReady("WebDAV", *miniWebDavOptions.port, bindIp)
	}

	// Start Admin with worker (depends on master, filer, S3, WebDAV)
	go startMiniAdminWithWorker(allServicesReady)
}

// startMiniService starts a service in a goroutine and reports its start to
// the progress board.
func startMiniService(name string, fn func(), port int) {
	if miniProgressBoard != nil {
		miniProgressBoard.starting(name)
	}
	fn()
}

// waitForServiceReady pings the service HTTP endpoint to check if it's ready
// to accept connections, and reports the transition to the progress board.
func waitForServiceReady(name string, port int, bindIp string) {
	address := fmt.Sprintf("http://%s:%d", bindIp, port)
	healthAddr := getHealthCheckAddr(address)
	maxAttempts := 30 // 30 * 200ms = 6 seconds max wait
	attempt := 0
	client := &http.Client{
		Timeout: 200 * time.Millisecond,
	}

	for attempt < maxAttempts {
		resp, err := client.Get(healthAddr)
		if err == nil {
			resp.Body.Close()
			if miniProgressBoard != nil {
				miniProgressBoard.ready(name)
			}
			return
		}
		attempt++
		time.Sleep(200 * time.Millisecond)
	}

	// Service failed to become ready, log warning but don't fail startup
	// (services may still work even if health check endpoint isn't responding immediately)
	if miniProgressBoard != nil {
		miniProgressBoard.failed(name)
	}
	glog.Warningf("Health check for %s failed (service may still be functional, retries may succeed)", name)
}

// startS3Service initializes and starts the S3 server
func startS3Service() {
	miniS3Options.localFilerSocket = miniFilerOptions.localSocket
	miniS3Options.startS3Server()
}

// applyMiniAdminCredentialFallback fills the admin credential flags from
// security.toml [admin] / WEED_ADMIN_* env vars when they were not set on the
// command line, mirroring the standalone `weed admin` command. CLI flags take
// precedence. Note the read-only viper keys (admin.readonly.*) differ from the
// mini flag names (admin.readOnly*).
func applyMiniAdminCredentialFallback(options *AdminOptions) {
	applyViperFallback(cmdMini, options.adminUser, "admin.user", "admin.user")
	applyViperFallback(cmdMini, options.adminPassword, "admin.password", "admin.password")
	applyViperFallback(cmdMini, options.readOnlyUser, "admin.readOnlyUser", "admin.readonly.user")
	applyViperFallback(cmdMini, options.readOnlyPassword, "admin.readOnlyPassword", "admin.readonly.password")
}

// startMiniAdminWithWorker starts the admin server with one worker
func startMiniAdminWithWorker(allServicesReady chan struct{}) {
	defer close(allServicesReady) // Ensure channel is always closed on all paths

	// Admin shuts down when mini clients shutdown is triggered.
	ctx := miniClientsCtx()

	// Determine bind IP for health checks
	bindIp := getBindIp()

	// Prepare master address with gRPC port
	masterAddr := string(pb.NewServerAddress(*miniIp, *miniMasterOptions.port, *miniMasterOptions.portGrpc))

	// Set admin options
	*miniAdminOptions.master = masterAddr

	// Resolve admin credentials from security.toml [admin] / WEED_ADMIN_* env
	// vars, matching the standalone `weed admin` command.
	applyMiniAdminCredentialFallback(&miniAdminOptions)

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

	// Normalize URL prefix the same way `weed admin` does.
	urlPrefix := strings.TrimRight(*miniAdminOptions.urlPrefix, "/")
	if urlPrefix != "" && !strings.HasPrefix(urlPrefix, "/") {
		urlPrefix = "/" + urlPrefix
	}

	// Start admin server in background. trackMiniClient lets the Ctrl+C
	// handler wait for startAdminServer's graceful shutdown before filer/
	// volume/master tear down.
	if miniProgressBoard != nil {
		miniProgressBoard.starting("Admin")
	}
	done := trackMiniClient()
	go func() {
		defer done()
		defer reportMiniStopped("Admin")
		var icebergPort int
		if miniS3Options.portIceberg != nil {
			icebergPort = *miniS3Options.portIceberg
		}
		if err := startAdminServer(ctx, miniAdminOptions, *miniEnableAdminUI, icebergPort, urlPrefix); err != nil {
			glog.Errorf("Admin server error: %v", err)
		}
	}()

	// Wait for admin server's HTTP port to be ready before launching worker
	adminAddr := fmt.Sprintf("http://%s:%d", bindIp, *miniAdminOptions.port)
	if err := waitForAdminServerReady(ctx, adminAddr); err != nil {
		// If the parent context was cancelled (e.g. a previous in-process
		// mini run is being torn down), bail out gracefully instead of
		// fataling — the test harness uses `cmd.Run` directly so a Fatalf
		// would terminate the entire test binary.
		if ctx.Err() != nil {
			glog.Warningf("Admin server readiness wait aborted: %v", err)
			return
		}
		glog.Fatalf("Admin server readiness check failed: %v", err)
	}

	// Start consolidated worker runtime (both standard and plugin runtimes)
	workerDir := filepath.Join(*miniDataFolders, "worker")
	if err := os.MkdirAll(workerDir, 0755); err != nil {
		glog.Fatalf("Failed to create unified worker directory: %v", err)
	}

	glog.V(1).Infof("Starting consolidated maintenance worker system (directory: %s)", workerDir)
	startMiniWorker(workerDir)
	startMiniPluginWorker(ctx, workerDir)

	// Wait for worker to be ready by polling its gRPC port
	workerGrpcAddr := fmt.Sprintf("%s:%d", bindIp, *miniAdminOptions.grpcPort)
	waitForWorkerReady(workerGrpcAddr)
	if miniProgressBoard != nil {
		miniProgressBoard.ready("Admin")
	}
}

// waitForAdminServerReady pings the admin server HTTP endpoint to check if it's ready.
// Returns ctx.Err() (typically context.Canceled) if the parent context is
// cancelled mid-poll so a torn-down mini doesn't keep spinning for the full
// timeout — relevant when tests embed mini in-process and reuse the same
// goroutine pool across subtests.
func waitForAdminServerReady(ctx context.Context, adminAddr string) error {
	healthAddr := getHealthCheckAddr(fmt.Sprintf("%s/health", adminAddr))
	// 240 * 500ms = 120 seconds max wait. The previous 30-second ceiling was
	// too tight on busy CI runners where master + filer + volume + admin all
	// initialise on a shared worker — the S3 Policy Shell Integration Tests
	// flaked regularly even though the admin server still came up within a
	// minute or two. Two minutes leaves headroom without being absurd locally.
	maxAttempts := 240
	attempt := 0
	client := &http.Client{
		Timeout: 1 * time.Second,
	}

	for attempt < maxAttempts {
		if err := ctx.Err(); err != nil {
			return err
		}
		resp, err := client.Get(healthAddr)
		if err == nil {
			resp.Body.Close()
			glog.V(1).Infof("Admin server is ready at %s", adminAddr)
			return nil
		}
		attempt++
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}

	return fmt.Errorf("admin server did not become ready at %s after %d attempts", adminAddr, maxAttempts)
}
func getHealthCheckAddr(addr string) string {
	if strings.Contains(addr, "://0.0.0.0:") {
		return strings.Replace(addr, "://0.0.0.0:", "://127.0.0.1:", 1)
	}
	return addr
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
func startMiniWorker(workerDir string) {
	glog.V(1).Infof("Initializing standard worker runtime")

	adminAddr := fmt.Sprintf("%s:%d", *miniIp, *miniAdminOptions.port)
	capabilities := "vacuum,ec,balance"

	// Use common worker directory

	glog.V(1).Infof("Worker connecting to admin server: %s", adminAddr)
	glog.V(1).Infof("Worker capabilities: %s", capabilities)
	glog.V(1).Infof("Worker directory: %s", workerDir)

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

	// Stop the worker BEFORE the clients ctx is cancelled. Otherwise admin's
	// internal worker gRPC GracefulStop (called during admin.Shutdown) would
	// wait for the worker stream to close, blocking the whole mini shutdown
	// by ~10s and cascading into filer's own gRPC graceful stop timeout.
	beforeMiniClientsShutdown(func() {
		workerInstance.Stop()
	})
	err = workerInstance.Start()
	if err != nil {
		glog.Fatalf("Failed to start worker: %v", err)
	}

	glog.V(1).Infof("Maintenance worker %s started successfully", workerInstance.ID())
}

func startMiniPluginWorker(ctx context.Context, workerDir string) {
	glog.V(1).Infof("Starting plugin worker for admin server")

	adminAddr := fmt.Sprintf("%s:%d", *miniIp, *miniAdminOptions.port)
	resolvedAdminAddr := resolvePluginWorkerAdminServer(adminAddr)
	if resolvedAdminAddr != adminAddr {
		glog.V(1).Infof("Resolved mini plugin worker admin endpoint: %s -> %s", adminAddr, resolvedAdminAddr)
	}

	// Use common worker directory

	util.LoadConfiguration("security", false)
	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.worker")

	handlers, err := buildPluginWorkerHandlers(defaultMiniPluginJobTypes, grpcDialOption, int(vacuum.DefaultMaxExecutionConcurrency), workerDir)
	if err != nil {
		glog.Fatalf("Failed to build mini plugin worker handlers: %v", err)
	}

	workerID, err := resolvePluginWorkerID("", workerDir)
	if err != nil {
		glog.Fatalf("Failed to resolve mini plugin worker ID: %v", err)
	}

	pluginRuntime, err := pluginworker.NewWorker(pluginworker.WorkerOptions{
		AdminServer:             resolvedAdminAddr,
		WorkerID:                workerID,
		WorkerVersion:           version.Version(),
		WorkerAddress:           *miniIp,
		HeartbeatInterval:       15 * time.Second,
		ReconnectDelay:          5 * time.Second,
		MaxDetectionConcurrency: 1,
		MaxExecutionConcurrency: int(vacuum.DefaultMaxExecutionConcurrency),
		GrpcDialOption:          grpcDialOption,
		Handlers:                handlers,
	})
	if err != nil {
		glog.Fatalf("Failed to create mini plugin worker: %v", err)
	}

	go func() {
		runCtx := ctx
		if runCtx == nil {
			runCtx = context.Background()
		}
		if runErr := pluginRuntime.Run(runCtx); runErr != nil && runCtx.Err() == nil {
			glog.Errorf("Mini plugin worker stopped with error: %v", runErr)
		}
	}()

	glog.V(1).Infof("Plugin worker %s started successfully with job types: %s", workerID, defaultMiniPluginJobTypes)
}

const credentialsInstructionTemplate = `
  To create S3 credentials, you have two options:

  Option 1: Use environment variables (recommended for quick setup)
    export AWS_ACCESS_KEY_ID=your-access-key
    export AWS_SECRET_ACCESS_KEY=your-secret-key
    export S3_BUCKET=my-bucket
    weed mini -dir=/data
    Creates initial credentials for the 'mini' user and pre-creates the bucket.

  Option 2: Use the Admin UI
    Open: http://%s:%d
    Add a new identity to create S3 credentials.
`

// printWelcomeMessage prints the welcome message after all services are running
func printWelcomeMessage() {
	var sb strings.Builder

	sb.WriteString("╔═══════════════════════════════════════════════════════════════════════════════╗\n")
	sb.WriteString("║                      SeaweedFS Mini - All-in-One Mode                         ║\n")
	sb.WriteString("╚═══════════════════════════════════════════════════════════════════════════════╝\n\n")
	sb.WriteString("  All enabled components are running and ready to use:\n\n")
	fmt.Fprintf(&sb, "    Master UI:       http://%s:%d\n", *miniIp, *miniMasterOptions.port)
	fmt.Fprintf(&sb, "    Volume Server:   http://%s:%d\n", *miniIp, *miniOptions.v.port)
	fmt.Fprintf(&sb, "    Filer UI:        http://%s:%d\n", *miniIp, *miniFilerOptions.port)
	if *miniEnableWebDAV {
		fmt.Fprintf(&sb, "    WebDAV:          http://%s:%d\n", *miniIp, *miniWebDavOptions.port)
	}
	if *miniEnableS3 {
		fmt.Fprintf(&sb, "    S3 Endpoint:     http://%s:%d\n", *miniIp, *miniS3Options.port)
		if miniS3Options.portIceberg != nil && *miniS3Options.portIceberg > 0 {
			fmt.Fprintf(&sb, "    Iceberg Catalog: http://%s:%d\n", *miniIp, *miniS3Options.portIceberg)
		}
	}
	if *miniEnableAdminUI {
		fmt.Fprintf(&sb, "    Admin UI:        http://%s:%d\n", *miniIp, *miniAdminOptions.port)
	}

	fmt.Fprintf(&sb, "\n  Data Directory:   %s\n", *miniDataFolders)
	firstDir := util.StringSplit(*miniDataFolders, ",")[0]
	if ds := stats_collect.NewDiskStatus(firstDir); ds != nil && ds.All > 0 {
		fmt.Fprintf(&sb, "  Free Space:       %s\n", util.BytesToHumanReadable(ds.Free))
	}
	if miniMasterOptions.volumeSizeLimitMB != nil {
		fmt.Fprintf(&sb, "  Volume Size:      %s\n", util.BytesToHumanReadable(uint64(*miniMasterOptions.volumeSizeLimitMB)*bytesPerMB))
	}
	if max, free, ok := miniVolumeCounts(); ok {
		fmt.Fprintf(&sb, "  Volume Count:     %d\n", max)
		fmt.Fprintf(&sb, "  Free Volumes:     %d\n", free)
	}
	sb.WriteString("\n  Press Ctrl+C to stop all components")

	switch {
	case s3api.HasAnyIdentity():
		// S3 identities already exist (loaded from filer on a previous mini
		// run, configured via env vars, static config file, etc.) — no need
		// to show setup hints.
	case *miniEnableAdminUI:
		fmt.Fprintf(&sb, credentialsInstructionTemplate, *miniIp, *miniAdminOptions.port)
	default:
		sb.WriteString("\n  To create S3 credentials, use environment variables:\n\n")
		sb.WriteString("    export AWS_ACCESS_KEY_ID=your-access-key\n")
		sb.WriteString("    export AWS_SECRET_ACCESS_KEY=your-secret-key\n")
		sb.WriteString("    export S3_BUCKET=my-bucket\n")
		sb.WriteString("    weed mini -dir=/data\n")
		sb.WriteString("    Creates initial credentials for the 'mini' user and pre-creates the bucket.\n")
	}

	fmt.Print(sb.String())
	fmt.Println("")
}

// miniVolumeCounts asks the local master for the total volume slots the data
// directory supports (Topology.Max) and how many are still free (Topology.Free).
// Best-effort: any error returns ok=false so the welcome banner simply omits
// the lines.
func miniVolumeCounts() (max, free int64, ok bool) {
	url := getHealthCheckAddr(fmt.Sprintf("http://%s:%d/dir/status", *miniIp, *miniMasterOptions.port))
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return 0, 0, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, 0, false
	}
	var status struct {
		Topology struct {
			Max  int64
			Free int64
		}
	}
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return 0, 0, false
	}
	return status.Topology.Max, status.Topology.Free, true
}

// ensureMiniBuckets creates each named bucket on the embedded filer if it does
// not already exist. bucketSpec is a comma-separated list (whitespace around
// each name is trimmed); empty entries and an empty spec are no-ops so callers
// who do not pass -bucket pay nothing. Per-bucket failures are logged and the
// loop continues so a single bad name does not block creating the rest.
func ensureMiniBuckets(bucketSpec string) error {
	names := parseBucketList(bucketSpec)
	if len(names) == 0 {
		return nil
	}

	filerAddress := pb.NewServerAddress(*miniIp, *miniFilerOptions.port, *miniFilerOptions.portGrpc)
	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.client")

	const bucketsPath = "/buckets"
	// Derive from miniClientsCtx so Ctrl+C cancels the bucket RPCs, and bound
	// with a short timeout (per bucket) so a stalled filer cannot block the
	// welcome message indefinitely.
	return pb.WithGrpcFilerClient(false, 0, filerAddress, grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		for _, name := range names {
			if err := s3bucket.VerifyS3BucketName(name); err != nil {
				glog.Warningf("invalid bucket name %q: %v", name, err)
				continue
			}
			ctx, cancel := context.WithTimeout(miniClientsCtx(), 5*time.Second)
			_, err := filer_pb.LookupEntry(ctx, client, &filer_pb.LookupDirectoryEntryRequest{
				Directory: bucketsPath,
				Name:      name,
			})
			if err == nil {
				glog.V(0).Infof("bucket %s already exists", name)
				cancel()
				continue
			}
			if !errors.Is(err, filer_pb.ErrNotFound) {
				glog.Warningf("lookup bucket %s: %v", name, err)
				cancel()
				continue
			}
			if err := filer_pb.DoMkdir(ctx, client, bucketsPath, name, nil); err != nil {
				glog.Warningf("create bucket %s: %v", name, err)
				cancel()
				continue
			}
			cancel()
			glog.V(0).Infof("created bucket %s", name)
		}
		return nil
	})
}

// ensureMiniTableBuckets creates each named S3 Tables bucket on the embedded
// filer if it does not already exist. bucketSpec is comma-separated; whitespace
// is trimmed and duplicates are dropped. Per-bucket failures are logged so one
// bad name does not block the rest. Buckets are owned by s3tables.DefaultAccountID
// since mini does not yet model multi-account ownership.
func ensureMiniTableBuckets(bucketSpec string) error {
	names := parseBucketList(bucketSpec)
	if len(names) == 0 {
		return nil
	}

	filerAddress := pb.NewServerAddress(*miniIp, *miniFilerOptions.port, *miniFilerOptions.portGrpc)
	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.client")

	return pb.WithGrpcFilerClient(false, 0, filerAddress, grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		manager := s3tables.NewManager()
		mgrClient := s3tables.NewManagerClient(client)
		for _, name := range names {
			ctx, cancel := context.WithTimeout(miniClientsCtx(), 5*time.Second)
			req := &s3tables.CreateTableBucketRequest{Name: name}
			var resp s3tables.CreateTableBucketResponse
			err := manager.Execute(ctx, mgrClient, "CreateTableBucket", req, &resp, s3tables.DefaultAccountID)
			cancel()
			if err == nil {
				glog.V(0).Infof("created table bucket %s", name)
				continue
			}
			var s3Err *s3tables.S3TablesError
			if errors.As(err, &s3Err) && s3Err.Type == s3tables.ErrCodeBucketAlreadyExists {
				glog.V(0).Infof("table bucket %s already exists", name)
				continue
			}
			glog.Warningf("create table bucket %s: %v", name, err)
		}
		return nil
	})
}

// parseBucketList splits a comma-separated bucket spec into a deduplicated list
// of trimmed, non-empty names, preserving the order they were given.
func parseBucketList(spec string) []string {
	if spec == "" {
		return nil
	}
	seen := make(map[string]bool)
	var names []string
	for _, raw := range strings.Split(spec, ",") {
		name := strings.TrimSpace(raw)
		if name == "" || seen[name] {
			continue
		}
		seen[name] = true
		names = append(names, name)
	}
	return names
}
