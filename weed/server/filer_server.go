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
	// master; it joins the filer list only once the gRPC port is serving and
	// the metadata replay has caught up with the peers found at startup.
	AnnounceCh chan struct{}
	// SkipEmptyMembershipConfirm announces after the startup peer scan
	// without re-polling to disprove an empty result: the embedded filer of
	// a single-master weed mini is the only member its dev cluster can have
	// yet, so the confirm window is dead time while routed traffic waits on
	// the filer's master listing. A late external filer still joins and
	// replays through the normal path.
	SkipEmptyMembershipConfirm bool
	// ClusterFreshHint, when non-nil, reports that the co-located master
	// minted its TopologyId this boot — its persisted state was empty, so the
	// empty-scan confirm only needs to cover a reconnecting peer's keepalive
	// retries rather than the full settling window. Restarted masters
	// recovering a prior id keep the full window.
	ClusterFreshHint func() bool
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
	option               *FilerOption
	filer                *filer.Filer
	filerGuard           *security.Guard
	volumeGuard          *security.Guard
	grpcDialOption       grpc.DialOption
	grpcDialOptionMaster grpc.DialOption
	grpcDialOptionVolume grpc.DialOption
	grpcDialOptionS3     grpc.DialOption

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

	// outcomes of recently applied object transactions keyed by request_id, so
	// a failover replay of the same transaction replays its outcome instead of
	// re-applying its mutations over a newer entry
	recentObjectTxnsMu      sync.Mutex
	recentObjectTxns        map[string]recentObjectTxn
	recentObjectTxnsSweepAt time.Time

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

	// startupPeers are the filers discovered during NewFilerServer, before
	// this filer announced itself; AnnounceOnceCaughtUp waits for their
	// replayed backlogs before the filer joins the master's list.
	startupPeers []pb.ServerAddress

	// peerBootstrapPending marks a fresh store whose startup peer scan came
	// back empty: an empty answer during a master restart can just mean the
	// existing filers have not reconnected yet, so AnnounceOnceCaughtUp
	// re-checks membership before registering.
	peerBootstrapPending bool

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

	// subscriptionsStopped is cancelled by StopSubscriptions when the filer
	// starts shutting down; every metadata subscription derives its context
	// from it as well as from its stream.
	subscriptionsStopped context.Context
	stopSubscriptions    context.CancelFunc
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
		grpcDialOptionMaster:  security.LoadClientTLSForTarget(util.GetViper(), "grpc.filer", "grpc.master"),
		grpcDialOptionVolume:  security.LoadClientTLSForTarget(util.GetViper(), "grpc.filer", "grpc.volume"),
		grpcDialOptionS3:      security.LoadClientTLSForTarget(util.GetViper(), "grpc.filer", "grpc.s3"),
		knownListeners:        make(map[int32]int32),
		subscribers:           make(map[int32]*metadataSubscriber),
		inFlightDataLimitCond: sync.NewCond(new(sync.Mutex)),
		recentCopyRequests:    make(map[string]recentCopyRequest),
		recentObjectTxns:      make(map[string]recentObjectTxn),
		CredentialManager:     option.CredentialManager,
		entryLockTable:        util.NewLockTable[util.FullPath](),
		posixLocks:            posixlock.NewManager(),
	}
	fs.subscriptionsStopped, fs.stopSubscriptions = context.WithCancel(context.Background())
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
	fs.filer = filer.NewFiler(*option.Masters, &filer.DialOptions{
		Filer:  fs.grpcDialOption,
		Master: fs.masterGrpcDialOption(),
		Volume: fs.volumeGrpcDialOption(),
	}, option.Host, option.FilerGroup, option.Collection, option.DefaultReplication, option.DataCenter, maxFilenameLength, nil)
	// Peer metadata replay takes the same per-path locks, so a replayed change
	// cannot split a local mutation's read-modify-write.
	fs.entryLockTable = fs.filer.EntryLockTable
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
		if err := fs.filer.MaybeBootstrapFromOnePeer(context.Background(), option.Host, existingNodes, startFromTime); err != nil {
			glog.Fatalf("%s bootstrap from %+v: %v", option.Host, existingNodes, err)
		}
		// An empty scan can be a settling master rather than an empty cluster;
		// the announce gate then re-checks membership before registering.
		fs.peerBootstrapPending = len(existingNodes) == 0
	}
	v.SetDefault("filer.options.s3.empty_folder_cleanup_delay", "2m")
	if d, err := time.ParseDuration(v.GetString("filer.options.s3.empty_folder_cleanup_delay")); err == nil {
		fs.filer.EmptyFolderCleanupDelay = d
	}
	fs.filer.AggregateFromPeers(option.Host, existingNodes, startFromTime)

	// A peer replaying its backlog applies the old owner's events over this
	// filer's fresh writes if it takes lock-ring ownership before the replay
	// finishes, so the announce gate waits for these peers to catch up.
	for _, node := range existingNodes {
		if addr := pb.ServerAddress(node.Address); !addr.Equals(option.Host) {
			fs.startupPeers = append(fs.startupPeers, addr)
		}
	}

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
		fs.CredentialManager.SetMasterClient(fs.filer.MasterClient, fs.s3GrpcDialOption())
	}

	return fs, nil
}

// Target-specific dial options fall back to the shared filer option, which is
// what FilerServer literals in tests set up.
func (fs *FilerServer) masterGrpcDialOption() grpc.DialOption {
	if fs.grpcDialOptionMaster != nil {
		return fs.grpcDialOptionMaster
	}
	return fs.grpcDialOption
}

func (fs *FilerServer) volumeGrpcDialOption() grpc.DialOption {
	if fs.grpcDialOptionVolume != nil {
		return fs.grpcDialOptionVolume
	}
	return fs.grpcDialOption
}

func (fs *FilerServer) s3GrpcDialOption() grpc.DialOption {
	if fs.grpcDialOptionS3 != nil {
		return fs.grpcDialOptionS3
	}
	return fs.grpcDialOption
}

func (fs *FilerServer) checkWithMaster() {

	isConnected := false
	for !isConnected {
		fs.option.Masters.RefreshBySrvIfAvailable()
		for _, master := range fs.option.Masters.GetInstances() {
			readErr := operation.WithMasterServerClient(context.Background(), false, master, fs.masterGrpcDialOption(), func(masterClient master_pb.SeaweedClient) error {
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

// announceCatchUpCap bounds how long startup holds the master registration
// while the metadata replay catches up with the peers found at startup. Past
// it the filer announces anyway, as it did when the gate opened on serve.
const announceCatchUpCap = time.Minute

// AnnounceOnceCaughtUp closes announce once every peer found at startup has
// replayed its backlog. Each peer's stream advances a watermark per event or
// idle heartbeat, so a watermark past waitStart means nothing older can still
// land on top of this filer's fresh writes. Called once gRPC is serving, so
// routed traffic is answerable the moment the filer is listed.
func (fs *FilerServer) AnnounceOnceCaughtUp(announce chan struct{}, waitStart time.Time) {
	defer close(announce)
	ma := fs.filer.MetaAggregator
	if ma == nil {
		return
	}
	// The bootstrap re-check and the catch-up wait share one deadline so a
	// late peer cannot stretch the registration hold past the cap.
	deadline := time.Now().Add(announceCatchUpCap)
	peers := fs.startupPeers
	if fs.peerBootstrapPending && !fs.option.SkipEmptyMembershipConfirm {
		peers = append(peers, fs.awaitPeerBootstrap(deadline)...)
	}
	if len(peers) == 0 {
		return
	}
	for {
		// Take the channel before reading the watermarks so a rise in
		// between still wakes this wait (see DeliveryWatermarkAdvancedChan).
		advanced := ma.DeliveryWatermarkAdvancedChan()
		if ma.PeersCaughtUpTo(peers, waitStart.UnixNano()) {
			glog.V(0).Infof("%s caught up with %d startup peers, announcing to masters", fs.option.Host, len(peers))
			return
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			glog.Warningf("%s metadata replay not caught up after %v, announcing anyway", fs.option.Host, announceCatchUpCap)
			return
		}
		select {
		case <-advanced:
		case <-time.After(min(remaining, time.Second)):
		}
	}
}

// emptyMembershipConfirmTimeout bounds the announce hold for a peer to
// disprove an empty startup scan on a fresh store. This runs after the ports
// already serve, so a genuinely empty cluster pays a delayed master listing,
// not a stalled startup.
const emptyMembershipConfirmTimeout = 10 * time.Second

// freshMembershipConfirmTimeout bounds the same hold when the co-located
// master minted its TopologyId this boot: a fresh mint proves the master kept
// no cluster state, but not that no external filer survived its loss — one
// may still be mid-reconnect. A few keepalive cycles of empty scans cover the
// reconnect while keeping a genuinely fresh cluster's announce prompt.
const freshMembershipConfirmTimeout = 3 * time.Second

// awaitPeerBootstrap re-polls membership while a fresh store's empty startup
// scan can still be disproved: masters restarted or re-elected report no
// filers until the existing ones reconnect. Peers found late are backfilled
// from before their streams start, then reported so the caller waits for
// their catch-up like startup peers. The whole wait — including each peer
// metadata copy — is bounded by the shared announce deadline.
func (fs *FilerServer) awaitPeerBootstrap(deadline time.Time) (peers []pb.ServerAddress) {
	confirmBy := time.Now().Add(emptyMembershipConfirmTimeout)
	var freshSince time.Time
	peerSeen := false
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		nodes, listErr := fs.filer.ListExistingPeerUpdates(ctx)
		cancel()
		if listErr != nil {
			glog.Warningf("%s cannot re-check existing peers: %v; announcing anyway", fs.option.Host, listErr)
			return nil
		}
		var remotes []*master_pb.ClusterNodeUpdate
		for _, node := range nodes {
			if !pb.ServerAddress(node.Address).Equals(fs.option.Host) {
				remotes = append(remotes, node)
			}
		}
		if len(remotes) == 0 {
			exitBy := confirmBy
			// The mint lands after raft election, which can postdate this
			// wait's start, so the hint is re-evaluated each pass rather than
			// once up front. An observed peer — even one that later
			// disconnects — disables the shortened deadline: an empty scan
			// afterward no longer proves a fresh cluster.
			if !peerSeen && fs.option.ClusterFreshHint != nil && fs.option.ClusterFreshHint() {
				if freshSince.IsZero() {
					freshSince = time.Now()
					glog.V(0).Infof("%s embedded master minted a fresh topology id; shortening the empty-scan confirm", fs.option.Host)
				}
				if freshBy := freshSince.Add(freshMembershipConfirmTimeout); freshBy.Before(exitBy) {
					exitBy = freshBy
				}
			}
			if !time.Now().Before(exitBy) {
				return nil
			}
			time.Sleep(time.Second)
			continue
		}
		peerSeen = true
		bootstrapFrom := time.Now().Add(-filer.LogFlushInterval)
		ctx, cancel = context.WithDeadline(context.Background(), deadline)
		err := fs.filer.MaybeBootstrapFromOnePeer(ctx, fs.option.Host, remotes, bootstrapFrom)
		cancel()
		if err != nil {
			glog.Warningf("%s bootstrap from late peers %+v: %v; retrying", fs.option.Host, remotes, err)
			time.Sleep(2 * time.Second)
			continue
		}
		glog.V(0).Infof("%s bootstrap from late-arriving peers %+v", fs.option.Host, remotes)
		for _, node := range remotes {
			peers = append(peers, pb.ServerAddress(node.Address))
			fs.filer.MetaAggregator.OnPeerUpdate(node, bootstrapFrom)
		}
		return peers
	}
	return nil
}

// masterRemovalWaitBudget bounds how long shutdown waits for masters to
// drop this filer before gRPC drains anyway.
const masterRemovalWaitBudget = 5 * time.Second

// waitForClusterRemoval polls the configured masters until none still lists
// this filer. The master removes membership when the KeepConnected handler
// unwinds, which is asynchronous to cancel, so new requests could otherwise
// route to this filer after its gRPC listener has already closed.
func (fs *FilerServer) waitForClusterRemoval() {
	ctx, cancel := context.WithTimeout(context.Background(), masterRemovalWaitBudget)
	defer cancel()
	confirmedAbsent := make(map[pb.ServerAddress]struct{})
	for ctx.Err() == nil {
		pending := false
		// The cached list keeps a slow SRV lookup from stretching the
		// removal-wait budget.
		for _, master := range fs.filer.MasterClient.ListCachedMasters() {
			if _, ok := confirmedAbsent[master]; ok {
				continue
			}
			if ctx.Err() != nil {
				return
			}
			// The per-query timeout shares the overall deadline, so a stalled
			// master cannot stretch the wait past the budget.
			queryCtx, queryCancel := context.WithTimeout(ctx, 2*time.Second)
			nodes, err := cluster.ListExistingPeerUpdates(queryCtx, master, fs.masterGrpcDialOption(), fs.filer.MasterClient.FilerGroup, cluster.FilerType)
			queryCancel()
			if err != nil {
				// A failed query is not confirmed removal; retry until the
				// master answers or the overall deadline expires.
				pending = true
				continue
			}
			listed := false
			for _, node := range nodes {
				if node.Address == string(fs.option.Host) {
					listed = true
					break
				}
			}
			if listed {
				pending = true
			} else {
				confirmedAbsent[master] = struct{}{}
			}
		}
		if !pending {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(200 * time.Millisecond):
		}
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
