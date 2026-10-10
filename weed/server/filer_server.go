package weed_server

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/credential"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"golang.org/x/sync/singleflight"

	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/util/grace"

	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/arangodb"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/cassandra"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/cassandra2"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/elastic/v7"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/etcd"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/foundationdb"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/hbase"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/leveldb"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/leveldb2"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/leveldb3"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/mongodb"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/mysql"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/mysql2"
	"github.com/seaweedfs/seaweedfs/weed/filer/posixlock"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/postgres"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/postgres2"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/redis"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/redis2"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/redis3"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/sqlite"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/tarantool"
	_ "github.com/seaweedfs/seaweedfs/weed/filer/ydb"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/notification"
	_ "github.com/seaweedfs/seaweedfs/weed/notification/aws_sqs"
	_ "github.com/seaweedfs/seaweedfs/weed/notification/gocdk_pub_sub"
	_ "github.com/seaweedfs/seaweedfs/weed/notification/google_pub_sub"
	_ "github.com/seaweedfs/seaweedfs/weed/notification/kafka"
	_ "github.com/seaweedfs/seaweedfs/weed/notification/log"
	_ "github.com/seaweedfs/seaweedfs/weed/notification/webhook"
	"github.com/seaweedfs/seaweedfs/weed/security"
)

type FilerOption struct {
	Masters                   *pb.ServerDiscovery
	FilerGroup                string
	Collection                string
	DefaultReplication        string
	DisableDirListing         bool
	MaxMB                     int
	DirListingLimit           int
	DataCenter                string
	Rack                      string
	DataNode                  string
	DefaultLevelDbDir         string
	DisableHttp               bool
	Host                      pb.ServerAddress
	recursiveDelete           bool
	Cipher                    bool
	SaveToFilerLimit          int64
	ConcurrentUploadLimit     int64
	ConcurrentFileUploadLimit int64
	ShowUIDirectoryDelete     bool
	DownloadMaxBytesPs        int64
	DiskType                  string
	AllowedOrigins            []string
	ExposeDirectoryData       bool
	TusBasePath               string
	TusMaxSize                int64
	TusSessionExpiry          time.Duration
	S3ConfigFile              string // optional path to static S3 identity config file
	CredentialManager         *credential.CredentialManager
	// AnnounceCh, when set, must be closed before the filer registers on the
	// master; it joins the filer list only once the gRPC port is serving.
	AnnounceCh chan struct{}
	// AllowUntrustedRemoteEndpoints lets a read of a remote-only entry dial a
	// mounted endpoint that resolves to a loopback / private / metadata host.
	AllowUntrustedRemoteEndpoints bool
	// RemoteCacheEvictThreshold is the disk usage fraction at which the filer
	// evicts remote-mounted cached chunks; 0 disables eviction.
	RemoteCacheEvictThreshold float64
}

type FilerServer struct {
	inFlightDataSize int64
	inFlightUploads  int64

	inFlightDataLimitCond *sync.Cond

	filer_pb.UnimplementedSeaweedFilerServer
	option         *FilerOption
	filer          *filer.Filer
	filerGuard     *security.Guard
	volumeGuard    *security.Guard
	grpcDialOption grpc.DialOption

	// metrics read from the master
	metricsAddress     string
	metricsIntervalSec int

	// track known metadata listeners
	knownListenersLock sync.Mutex
	knownListeners     map[int32]int32
	// live metadata subscribers (FUSE mounts, S3, peer filers, ...) keyed by
	// clientId, guarded by knownListenersLock. Exposed via ListMetadataSubscribers.
	subscribers map[int32]*metadataSubscriber

	// deduplicates concurrent remote object caching operations
	remoteCacheGroup singleflight.Group

	// serializes remote-cache eviction passes; lastVacuum rate-limits the
	// compaction trigger that reclaims evicted chunks.
	remoteCacheEvictMu       sync.Mutex
	remoteCacheLastVacuum    atomic.Pointer[time.Time]
	remoteCacheEvictCtx      context.Context
	remoteCacheEvictCancel   context.CancelFunc
	remoteCachePendingVidsMu sync.Mutex
	remoteCachePendingVids   map[uint32]int

	recentCopyRequestsMu sync.Mutex
	recentCopyRequests   map[string]recentCopyRequest

	// credential manager for IAM operations
	CredentialManager *credential.CredentialManager

	// mountPeerRegistry backs the MountRegister / MountList RPCs for peer
	// chunk sharing (tier 1). Always populated.
	mountPeerRegistry *filer.MountPeerRegistry

	// tusActiveUploads marks TUS sessions with a mutating request in flight, so
	// a concurrent PATCH or DELETE is refused instead of recording duplicate
	// chunks behind the first request's back.
	tusActiveUploads sync.Map

	// ringPeerIPs caches resolved ring member addresses per ring version so
	// verifying a forwarded request's peer does not pay a DNS lookup per hop.
	ringPeerIPs      atomic.Pointer[ringPeerIPs]
	ringResolveGroup singleflight.Group

	// masterCtx drives KeepConnectedToMaster; cancelling it ends the stream
	// so the master drops this filer from its list before gRPC stops.
	masterCtx    context.Context
	masterCancel context.CancelFunc

	// entryLockTable serializes mutations to the same entry path on this filer.
	// CreateEntry takes it today; UpdateEntry and DeleteEntry are intended to take
	// it too as their callers route a key's writes to this node, making it the
	// local serialization point for read-modify-write operations that replaces
	// the distributed lock for that key. Idle keys are evicted automatically, so
	// the table stays bounded.
	entryLockTable *util.LockTable[util.FullPath]

	// posixLocks is the in-memory authority for cross-mount POSIX advisory locks
	// on inodes this filer owns (per the route-by-key ring). Lock state is kept
	// here rather than in replicated metadata: it is transient coordination, so
	// keeping it off the meta-log avoids churn.
	posixLocks *posixlock.Manager
	// posixLockSweeperStop stops the lease-reaping sweeper goroutine on Shutdown.
	posixLockSweeperStop chan struct{}
	// posixLockReadyAt is the unix-nanos when this filer began serving POSIX
	// locks. For posixLockWarmup after it, the owner defers would-be grants while
	// mounts re-assert, so a (re)started owner does not double-grant from empty
	// state. Atomic so the handler reads it without locking; 0 means "not warming
	// up" (e.g. in tests).
	posixLockReadyAt atomic.Int64
}

func NewFilerServer(defaultMux, readonlyMux *http.ServeMux, option *FilerOption) (fs *FilerServer, err error) {

	v := util.GetViper()
	signingKey := v.GetString("jwt.filer_signing.key")
	v.SetDefault("jwt.filer_signing.expires_after_seconds", 10)
	expiresAfterSec := v.GetInt("jwt.filer_signing.expires_after_seconds")

	readSigningKey := v.GetString("jwt.filer_signing.read.key")
	v.SetDefault("jwt.filer_signing.read.expires_after_seconds", 60)
	readExpiresAfterSec := v.GetInt("jwt.filer_signing.read.expires_after_seconds")

	volumeSigningKey := v.GetString("jwt.signing.key")
	v.SetDefault("jwt.signing.expires_after_seconds", 10)
	volumeExpiresAfterSec := v.GetInt("jwt.signing.expires_after_seconds")

	volumeReadSigningKey := v.GetString("jwt.signing.read.key")
	v.SetDefault("jwt.signing.read.expires_after_seconds", 60)
	volumeReadExpiresAfterSec := v.GetInt("jwt.signing.read.expires_after_seconds")

	v.SetDefault("cors.allowed_origins.values", "*")

	allowedOrigins := v.GetString("cors.allowed_origins.values")
	domains := strings.Split(allowedOrigins, ",")
	option.AllowedOrigins = domains

	// -exposeDirectoryData and filer.expose_directory_metadata both default to
	// on, and either one turning it off has to hold: this is what keeps the
	// directory listing off a filer whose reads are otherwise unauthenticated.
	v.SetDefault("filer.expose_directory_metadata.enabled", true)
	option.ExposeDirectoryData = option.ExposeDirectoryData && v.GetBool("filer.expose_directory_metadata.enabled")

	fs = &FilerServer{
		option:                option,
		grpcDialOption:        security.LoadClientTLS(util.GetViper(), "grpc.filer"),
		knownListeners:        make(map[int32]int32),
		subscribers:           make(map[int32]*metadataSubscriber),
		inFlightDataLimitCond: sync.NewCond(new(sync.Mutex)),
		recentCopyRequests:    make(map[string]recentCopyRequest),
		CredentialManager:     option.CredentialManager,
		entryLockTable:        util.NewLockTable[util.FullPath](),
		posixLocks:            posixlock.NewManager(),
	}
	fs.startPosixLockSweeper()
	fs.mountPeerRegistry = filer.NewMountPeerRegistry()
	go fs.runMountPeerRegistrySweeper()
	fs.remoteCacheEvictCtx, fs.remoteCacheEvictCancel = context.WithCancel(context.Background())

	option.Masters.RefreshBySrvIfAvailable()
	if len(option.Masters.GetInstances()) == 0 {
		glog.Fatal("master list is required!")
	}

	if !util.LoadConfiguration("filer", false) {
		v.SetDefault("leveldb2.enabled", true)
		v.SetDefault("leveldb2.dir", option.DefaultLevelDbDir)
		_, err := os.Stat(option.DefaultLevelDbDir)
		if os.IsNotExist(err) {
			os.MkdirAll(option.DefaultLevelDbDir, 0755)
		}
		glog.V(0).Infof("default to create filer store dir in %s", option.DefaultLevelDbDir)
	} else {
		glog.Warningf("skipping default store dir in %s", option.DefaultLevelDbDir)
	}
	util.LoadConfiguration("notification", false)

	v.SetDefault("filer.options.max_file_name_length", 255)
	maxFilenameLength := v.GetUint32("filer.options.max_file_name_length")
	glog.V(0).Infof("max_file_name_length %d", maxFilenameLength)
	fs.filer = filer.NewFiler(*option.Masters, fs.grpcDialOption, option.Host, option.FilerGroup, option.Collection, option.DefaultReplication, option.DataCenter, maxFilenameLength, nil)
	fs.filer.Cipher = option.Cipher
	fs.filer.DefaultDiskType = option.DiskType
	fs.filer.BuildGuardedRemoteClient = BuildGuardedRemoteStorageClient
	fs.filer.AllowUntrustedRemoteEndpoints = option.AllowUntrustedRemoteEndpoints
	fs.filer.RemoteStorage.SetConfValidator(func(ctx context.Context, conf *remote_pb.RemoteConf) error {
		return ValidateRemoteConfForLoad(ctx, conf, option.AllowUntrustedRemoteEndpoints)
	})
	go fs.runRemoteCacheEviction()
	// we do not support IP whitelist right now https://github.com/seaweedfs/seaweedfs/issues/7094
	if v.GetString("guard.white_list") != "" {
		glog.Warningf("filer: guard.white_list is configured but the IP whitelist feature is currently disabled. See https://github.com/seaweedfs/seaweedfs/issues/7094")
	}
	fs.filerGuard = security.NewGuard([]string{}, signingKey, expiresAfterSec, readSigningKey, readExpiresAfterSec)
	fs.volumeGuard = security.NewGuard([]string{}, volumeSigningKey, volumeExpiresAfterSec, volumeReadSigningKey, volumeReadExpiresAfterSec)

	fs.checkWithMaster()

	go stats.LoopPushingMetric("filer", string(fs.option.Host), fs.metricsAddress, fs.metricsIntervalSec)
	if option.AnnounceCh != nil {
		fs.filer.MasterClient.SetAnnounceCh(option.AnnounceCh)
	}
	fs.masterCtx, fs.masterCancel = context.WithCancel(context.Background())
	go fs.filer.MasterClient.KeepConnectedToMaster(fs.masterCtx)

	fs.option.recursiveDelete = v.GetBool("filer.options.recursive_delete")
	v.SetDefault("filer.options.buckets_folder", "/buckets")
	fs.filer.DirBucketsPath = v.GetString("filer.options.buckets_folder")
	// TODO deprecated, will be removed after 2020-12-31
	// replaced by https://github.com/seaweedfs/seaweedfs/wiki/Path-Specific-Configuration
	// fs.filer.FsyncBuckets = v.GetStringSlice("filer.options.buckets_fsync")
	isFresh := fs.filer.LoadConfiguration(v)

	notification.LoadConfiguration(v, "notification.")

	handleStaticResources(defaultMux)
	if !option.DisableHttp {
		defaultMux.HandleFunc("/healthz", requestIDMiddleware(fs.filerHealthzHandler))
		defaultMux.HandleFunc("/readyz", requestIDMiddleware(fs.filerHealthzHandler))
		// TUS resumable upload protocol handler
		if option.TusBasePath != "" {
			// Normalize TusPath to always have a leading slash and no trailing slash
			if !strings.HasPrefix(option.TusBasePath, "/") {
				option.TusBasePath = "/" + option.TusBasePath
			}
			option.TusBasePath = strings.TrimRight(option.TusBasePath, "/")

			// Disallow using "/" as TUS base to avoid hijacking all filer routes
			if option.TusBasePath == "" {
				glog.Warningf("Invalid TUS base path; TUS disabled (must not be root '/')")
			} else {
				if option.TusMaxSize <= 0 {
					option.TusMaxSize = TusDefaultMaxSize
				}
				if option.TusSessionExpiry <= 0 {
					option.TusSessionExpiry = TusDefaultSessionExpiry
				}
				handlePath := option.TusBasePath + "/"
				defaultMux.HandleFunc(handlePath, fs.filerGuard.WhiteList(requestIDMiddleware(fs.tusHandler)))
				// Start background cleanup of expired TUS sessions (every hour)
				fs.StartTusSessionCleanup(1 * time.Hour)
			}
		}
		defaultMux.HandleFunc("/", fs.filerGuard.WhiteList(requestIDMiddleware(fs.filerHandler)))
	}
	if defaultMux != readonlyMux {
		handleStaticResources(readonlyMux)
		readonlyMux.HandleFunc("/healthz", requestIDMiddleware(fs.filerHealthzHandler))
		readonlyMux.HandleFunc("/readyz", requestIDMiddleware(fs.filerHealthzHandler))
		readonlyMux.HandleFunc("/", fs.filerGuard.WhiteList(requestIDMiddleware(fs.readonlyFilerHandler)))
	}

	// A failed peer scan is not an empty cluster: a fresh filer that skipped
	// bootstrap here would serve without the existing files, so retry until a
	// master answers. This runs before the announce gate opens, so the filer
	// is not yet advertised.
	var existingNodes []*master_pb.ClusterNodeUpdate
	for {
		var listErr error
		existingNodes, listErr = fs.filer.ListExistingPeerUpdates(context.Background())
		if listErr == nil {
			break
		}
		glog.Warningf("%s cannot list existing peers: %v; retrying", option.Host, listErr)
		time.Sleep(2 * time.Second)
	}
	startFromTime := time.Now().Add(-filer.LogFlushInterval)
	if isFresh {
		glog.V(0).Infof("%s bootstrap from peers %+v", option.Host, existingNodes)
		if err := fs.filer.MaybeBootstrapFromOnePeer(option.Host, existingNodes, startFromTime); err != nil {
			glog.Fatalf("%s bootstrap from %+v: %v", option.Host, existingNodes, err)
		}
	}
	v.SetDefault("filer.options.s3.empty_folder_cleanup_delay", "2m")
	if d, err := time.ParseDuration(v.GetString("filer.options.s3.empty_folder_cleanup_delay")); err == nil {
		fs.filer.EmptyFolderCleanupDelay = d
	}
	fs.filer.AggregateFromPeers(option.Host, existingNodes, startFromTime)

	fs.filer.LoadFilerConf()

	fs.filer.LoadRemoteStorageConfAndMapping()

	fs.filer.RebuildRemoteDeletionTombstones(context.Background())

	grace.OnReload(fs.Reload)

	fs.SetupDlmReplication()
	fs.filer.Dlm.LockRing.SetTakeSnapshotCallback(fs.OnDlmChangeSnapshot)

	if fs.CredentialManager != nil {
		fs.CredentialManager.SetFilerAddressFunc(func() pb.ServerAddress {
			return fs.option.Host
		}, fs.grpcDialOption)
		fs.CredentialManager.SetMasterClient(fs.filer.MasterClient, fs.grpcDialOption)
	}

	return fs, nil
}

func (fs *FilerServer) checkWithMaster() {

	isConnected := false
	for !isConnected {
		fs.option.Masters.RefreshBySrvIfAvailable()
		for _, master := range fs.option.Masters.GetInstances() {
			readErr := operation.WithMasterServerClient(context.Background(), false, master, fs.grpcDialOption, func(masterClient master_pb.SeaweedClient) error {
				resp, err := masterClient.GetMasterConfiguration(context.Background(), &master_pb.GetMasterConfigurationRequest{})
				if err != nil {
					return fmt.Errorf("get master %s configuration: %v", master, err)
				}
				fs.metricsAddress, fs.metricsIntervalSec = resp.MetricsAddress, int(resp.MetricsIntervalSeconds)
				return nil
			})
			if readErr == nil {
				isConnected = true
			} else {
				time.Sleep(7 * time.Second)
			}
		}
	}
}

// Shutdown gracefully shuts down the filer server by waiting for in-flight uploads to complete.
// This prevents data corruption when the process receives SIGTERM during active uploads.
func (fs *FilerServer) Shutdown() {
	glog.V(0).Infof("Shutting down filer")
	if fs.posixLockSweeperStop != nil {
		close(fs.posixLockSweeperStop)
	}
	if fs.remoteCacheEvictCancel != nil {
		fs.remoteCacheEvictCancel()
	}
	fs.filer.Shutdown()
	// LeaveLockRing already ended the master stream in the normal path; this
	// covers callers that skip it, and runs after the filer's own shutdown so
	// the final metadata-log flush can still reach the master.
	if fs.masterCancel != nil {
		fs.masterCancel()
	}
}

// priorOwnerWindowSkew covers peers and gateways that start their prior-owner
// window after this filer, since each times it from its own ring update.
const priorOwnerWindowSkew = time.Second

// leaveLockRingBudget bounds the leave: installing the ring update runs lock
// transfers under the ring lock, which can outlast the removal timeout.
const leaveLockRingBudget = 10 * time.Second

// LeaveLockRing hands this filer's lock-ring keys to its peers while its own
// servers still accept the lock transfers and prior-owner writes that follow.
func (fs *FilerServer) LeaveLockRing() {
	fs.leaveLockRingWithin(3*cluster.LockRingStabilizationInterval, leaveLockRingBudget)
}

func (fs *FilerServer) leaveLockRingWithin(removalTimeout, budget time.Duration) {
	left := make(chan struct{})
	go func() {
		defer close(left)
		fs.leaveLockRing(removalTimeout)
	}()
	select {
	case <-left:
	case <-time.After(budget):
		glog.Warningf("LockRing: %s did not finish leaving within %v, shutting down anyway", fs.option.Host, budget)
	}
	// End the master stream so the master drops this filer from the cluster
	// list before gRPC stops accepting connections.
	if fs.masterCancel != nil {
		fs.masterCancel()
	}
	fs.waitForClusterRemoval()
}

// masterRemovalWaitBudget bounds how long shutdown waits for masters to
// drop this filer before gRPC drains anyway.
const masterRemovalWaitBudget = 5 * time.Second

// waitForClusterRemoval polls the configured masters until none still lists
// this filer. The master removes membership when the KeepConnected handler
// unwinds, which is asynchronous to cancel, so new requests could otherwise
// route to this filer after its gRPC listener has already closed.
func (fs *FilerServer) waitForClusterRemoval() {
	deadline := time.Now().Add(masterRemovalWaitBudget)
	for {
		listed := false
		for _, master := range fs.filer.MasterClient.ListMasters() {
			queryCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			nodes := cluster.ListExistingPeerUpdates(queryCtx, master, fs.grpcDialOption, fs.filer.MasterClient.FilerGroup, cluster.FilerType)
			cancel()
			for _, node := range nodes {
				if node.Address == string(fs.option.Host) {
					listed = true
				}
			}
		}
		if !listed || time.Now().After(deadline) {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (fs *FilerServer) leaveLockRing(removalTimeout time.Duration) {
	if fs.filer.Dlm == nil {
		return
	}
	ring := fs.filer.Dlm.LockRing
	self := fs.option.Host
	members := ring.GetSnapshot()
	// A lone filer has no peer to take its keys: leaving would strand lock
	// requests arriving during the drain without an owner.
	if len(members) < 2 || !slices.Contains(members, self) {
		return
	}
	// A failed send still leaves: the broken stream's close drops this filer
	// from the ring and the reconnect registers without it.
	if err := fs.filer.MasterClient.LeaveLockRing(); err != nil {
		glog.Warningf("LockRing: %s leave request failed, waiting for reconnect: %v", self, err)
	}
	deadline := time.Now().Add(removalTimeout)
	for slices.Contains(ring.GetSnapshot(), self) {
		if time.Now().After(deadline) {
			glog.Warningf("LockRing: %s still in the ring after %v, shutting down anyway", self, removalTimeout)
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	until := ring.PriorOwnerWindowEnd().Add(priorOwnerWindowSkew)
	glog.V(0).Infof("LockRing: %s left, serving prior-owner requests until %v", self, until)
	time.Sleep(time.Until(until))
}

func (fs *FilerServer) Reload() {
	glog.V(0).Infoln("Reload filer server...")

	util.LoadConfiguration("security", false)
	v := util.GetViper()
	fs.filerGuard.UpdateSigningKeys(
		v.GetString("jwt.filer_signing.key"),
		v.GetInt("jwt.filer_signing.expires_after_seconds"),
		v.GetString("jwt.filer_signing.read.key"),
		v.GetInt("jwt.filer_signing.read.expires_after_seconds"),
	)
	fs.volumeGuard.UpdateSigningKeys(
		v.GetString("jwt.signing.key"),
		v.GetInt("jwt.signing.expires_after_seconds"),
		v.GetString("jwt.signing.read.key"),
		v.GetInt("jwt.signing.read.expires_after_seconds"),
	)
	util_http.ReloadJwtSigningReadConfig()
}
