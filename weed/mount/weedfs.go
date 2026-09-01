package mount

import (
	"bytes"
	"context"
	"math/rand/v2"
	"os"
	"path"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/filer/posixlock"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mount/meta_cache"
	"github.com/seaweedfs/seaweedfs/weed/mount/page_writer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mount_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/chunk_cache"
	"github.com/seaweedfs/seaweedfs/weed/util/grace"
	"github.com/seaweedfs/seaweedfs/weed/util/version"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
)

type Option struct {
	filerIndex                  int32 // align memory for atomic read/write
	FilerAddresses              []pb.ServerAddress
	MountDirectory              string
	GrpcDialOption              grpc.DialOption
	FilerSigningKey             security.SigningKey
	FilerSigningExpiresAfterSec int
	FilerMountRootPath          string
	Collection                  string
	Replication                 string
	TtlSec                      int32
	DiskType                    types.DiskType
	ChunkSizeLimit              int64
	ConcurrentWriters           int
	ConcurrentReaders           int
	CacheDirForRead             string
	CacheSizeMBForRead          int64
	CacheDirForWrite            string
	WriteBufferSizeMB           int64
	CacheMetaTTlSec             int
	CacheDirMaxEntries          int
	DataCenter                  string
	Umask                       os.FileMode
	Quota                       int64
	DisableXAttr                bool
	IsMacOs                     bool

	// LogicalDiskUsage reports data sizes rather than the space they occupy,
	// for both df and the quota. See WFS.diskSizes.
	LogicalDiskUsage bool

	MountUid         uint32
	MountGid         uint32
	MountMode        os.FileMode
	MountCtime       time.Time
	MountMtime       time.Time
	MountParentInode uint64

	VolumeServerAccess   string // how to access volume servers
	Cipher               bool   // whether encrypt data on volume server
	UidGidMapper         *meta_cache.UidGidMapper
	IncludeSystemEntries bool

	// DefaultPermissions mirrors the FUSE default_permissions mount option.
	// When set, the kernel enforces unix permission bits from the getattr/
	// lookup attributes before it ever calls Open/Create/Mknod, so the mount
	// skips its own redundant permission checks (and the group lookups behind
	// them) on those hot paths.
	DefaultPermissions bool

	// Periodic metadata flush interval in seconds (0 to disable)
	// This protects chunks from being purged by volume.fsck for long-running writes
	MetadataFlushSeconds int

	// RDMA acceleration options
	RdmaEnabled       bool
	RdmaSidecarAddr   string
	RdmaFallback      bool
	RdmaReadOnly      bool
	RdmaMaxConcurrent int
	RdmaTimeoutMs     int

	// Peer chunk sharing options (design-weed-mount-peer-chunk-sharing.md).
	// When PeerEnabled is false (default), the mount runs exactly as today.
	// One gRPC port carries everything: directory RPCs (ChunkAnnounce /
	// ChunkLookup) and streaming FetchChunk byte transfers.
	PeerEnabled    bool
	PeerListen     string // host:port to bind the peer gRPC server
	PeerAdvertise  string // externally reachable host:port (optional; defaults to auto-detected host + PeerListen port)
	PeerDataCenter string // optional data-center label advertised to peers
	PeerRack       string // optional rack label advertised to peers (finer than DC)

	// Directory cache refresh/eviction controls
	DirIdleEvictSec int

	// EnableDistributedLock enables DLM-based write coordination across mounts.
	// When true, opening a file for write acquires a distributed lock that is
	// held (with auto-renewal) until the file is closed, so only one mount can
	// have a file open for writing at a time; POSIX advisory locks (flock/fcntl)
	// are also routed to the inode's owner filer so they are honored across
	// mounts. Disabled under writeback cache, which implies single-writer.
	EnableDistributedLock bool

	// WritebackCache enables async flush on close for improved small file write performance.
	// When true, Flush() returns immediately and data upload + metadata flush happen in background.
	WritebackCache bool

	// EagerFilerCreate persists a created file's entry at create time instead
	// of deferring it to flush. Deferring is only safe when the flush runs
	// before the application's close returns; a platform that cannot
	// guarantee that sets this so listings and reopens through the filer
	// cannot race an unflushed close.
	EagerFilerCreate bool

	// PosixDirNlink enables POSIX-compliant directory nlink counting
	// (nlink = 2 + number_of_subdirectories). This requires listing
	// cached directory entries on every stat, which has a performance cost.
	// When false (default), directories report nlink=2.
	PosixDirNlink bool

	uniqueCacheDirForRead  string
	uniqueCacheDirForWrite string
}

type WFS struct {
	// https://dl.acm.org/doi/fullHtml/10.1145/3310148
	// follow https://github.com/hanwen/go-fuse/blob/master/fuse/api.go
	fuse.RawFileSystem
	mount_pb.UnimplementedSeaweedMountServer
	option                *Option
	metaCache             *meta_cache.MetaCache
	stats                 statsCache
	chunkCache            *chunk_cache.TieredChunkCache
	writeBufferAccountant *page_writer.WriteBufferAccountant
	signature             int32
	concurrentWriters     *util.LimitedConcurrentExecutor
	copyBufferPool        sync.Pool
	concurrentCopiersSem  chan struct{}
	inodeToPath           *InodeToPath
	fhMap                 *FileHandleToInode
	dhMap                 *DirectoryHandleToInode
	fuseServer            *fuse.Server
	IsOverQuota           bool
	fhLockTable           *util.LockTable[FileHandleId]
	hardLinkLockTable     *util.LockTable[string]
	posixLocks            *PosixLockTable
	posixSid              uint64             // this mount's session id, for routed-lock owner identity
	posixHint             *posixLockHint     // local fcntl-lock hint for routed mode
	posixOwn              *posixlock.Manager // mirror of locks this mount holds, re-asserted via keepalive
	rdmaClient            *RDMAMountClient
	peerRegistrar         *PeerRegistrar
	peerDirectory         *PeerDirectory
	peerGrpcServer        *PeerGrpcServer
	peerAnnouncer         *PeerAnnouncer
	peerConnPool          *PeerConnPool
	peerDirectoryStop     chan struct{} // closed on unmount to stop the sweeper goroutine
	FilerConf             *filer.FilerConf
	filerClient           *wdclient.FilerClient // Cached volume location client
	refreshMu             sync.Mutex
	refreshingDirs        map[util.FullPath]struct{}
	atimeMu               sync.Mutex
	atimeMap              map[uint64]time.Time // inode -> atime, in-memory only, bounded
	dirMtimeMu            sync.Mutex
	dirMtimeMap           map[uint64]time.Time // inode -> mtime/ctime, in-memory overlay for dirs
	removedDirMu          sync.Mutex
	removedDirs           map[uint64]*filer_pb.Entry // inode -> last-known entry of a directory removed while still referenced
	entryValidSec         uint64                     // kernel FUSE entry cache TTL in seconds
	attrValidSec          uint64                     // kernel FUSE attr cache TTL in seconds
	dirIdleEvict          time.Duration

	// openMtimeCache maps inode -> [mtime_sec, mtime_ns] from the last Open.
	// Used to decide whether to set FOPEN_KEEP_CACHE on subsequent opens.
	// Bounded to openMtimeCacheMaxSize entries; when full a random entry is
	// evicted. This trades a small amount of cache-miss overhead for
	// predictable memory usage on mounts that touch many files.
	openMtimeMu    sync.Mutex
	openMtimeCache map[uint64][2]int64

	// asyncFlushWg tracks pending background flush work items for writebackCache mode.
	// Must be waited on before unmount cleanup to prevent data loss.
	asyncFlushWg sync.WaitGroup

	// entryChanged is notified of every applied metadata event, for a front
	// end that has to push invalidations to its own client.
	entryChangeMu   sync.RWMutex
	entryChanged    func(meta_cache.EntryInvalidation)
	asyncFlushClose sync.Once

	// asyncFlushCh is a bounded work queue for background flush operations.
	// A fixed pool of worker goroutines processes items from this channel,
	// preventing resource exhaustion from unbounded goroutine creation.
	asyncFlushCh chan *asyncFlushItem

	// pendingAsyncFlush tracks in-flight async flush goroutines by inode.
	// AcquireHandle checks this to wait for a pending flush before reopening
	// the same inode, preventing stale metadata from overwriting the async flush.
	pendingAsyncFlushMu sync.Mutex
	pendingAsyncFlush   map[uint64]chan struct{}

	// streamMutate is the multiplexed streaming gRPC connection for all filer
	// mutations (create, update, delete, rename). All mutations go through one
	// ordered stream to prevent cross-operation reordering.
	streamMutate *streamMutateMux

	// lockClient is the DLM client for cross-mount write coordination.
	// Non-nil only when EnableDistributedLock is true.
	lockClient *cluster.LockClient
}

const defaultDirIdleEvict = 10 * time.Minute

func NewSeaweedFileSystem(option *Option) *WFS {
	// Only create FilerClient for direct volume access modes
	// When VolumeServerAccess == "filerProxy", all reads go through filer, so no volume lookup needed
	var filerClient *wdclient.FilerClient
	if option.VolumeServerAccess != "filerProxy" {
		// Create FilerClient for efficient volume location caching
		// Pass all filer addresses for high availability with automatic failover
		// Configure URL preference based on VolumeServerAccess option
		var opts *wdclient.FilerClientOption
		if option.VolumeServerAccess == "publicUrl" {
			opts = &wdclient.FilerClientOption{
				UrlPreference: wdclient.PreferPublicUrl,
			}
		}

		filerClient = wdclient.NewFilerClient(
			option.FilerAddresses, // Pass all filer addresses for HA
			option.GrpcDialOption,
			option.DataCenter,
			opts,
		)
	}

	dirIdleEvict := defaultDirIdleEvict
	if option.DirIdleEvictSec != 0 {
		dirIdleEvict = time.Duration(option.DirIdleEvictSec) * time.Second
	} else {
		dirIdleEvict = 0
	}

	wfs := &WFS{
		RawFileSystem:     fuse.NewDefaultRawFileSystem(),
		option:            option,
		signature:         util.RandomInt32(),
		inodeToPath:       NewInodeToPath(util.FullPath(option.FilerMountRootPath), option.CacheMetaTTlSec),
		fhMap:             NewFileHandleToInode(),
		dhMap:             NewDirectoryHandleToInode(),
		filerClient:       filerClient, // nil for proxy mode, initialized for direct access
		pendingAsyncFlush: make(map[uint64]chan struct{}),
		fhLockTable:       util.NewLockTable[FileHandleId](),
		hardLinkLockTable: util.NewLockTable[string](),
		posixLocks:        NewPosixLockTable(),
		posixSid:          randomPosixSid(),
		posixHint:         newPosixLockHint(),
		posixOwn:          posixlock.NewManager(),
		refreshingDirs:    make(map[util.FullPath]struct{}),
		atimeMap:          make(map[uint64]time.Time, 8192),
		openMtimeCache:    make(map[uint64][2]int64, 8192),
		dirMtimeMap:       make(map[uint64]time.Time, 1024),
		entryValidSec:     1,
		attrValidSec:      1,
		dirIdleEvict:      dirIdleEvict,
	}

	// With writeback caching, this mount is the single writer. Increase kernel
	// FUSE cache TTLs so the kernel doesn't re-issue Lookup/GetAttr for every
	// path component and stat — the local meta cache is authoritative.
	if option.WritebackCache {
		wfs.entryValidSec = 10
		wfs.attrValidSec = 10
	}

	if option.EnableDistributedLock && !option.WritebackCache && len(option.FilerAddresses) > 0 {
		wfs.lockClient = cluster.NewLockClient(option.GrpcDialOption, option.FilerAddresses[0])
		glog.V(0).Infof("distributed lock manager enabled for mount")
	} else if option.EnableDistributedLock && option.WritebackCache {
		glog.V(0).Infof("distributed lock manager disabled: writeback cache implies single-writer mode")
	}

	wfs.option.filerIndex = int32(rand.IntN(len(option.FilerAddresses)))
	wfs.option.setupUniqueCacheDirectory()
	if option.CacheSizeMBForRead > 0 {
		wfs.chunkCache = chunk_cache.NewTieredChunkCache(256, option.getUniqueCacheDirForRead(), option.CacheSizeMBForRead, 1024*1024)
	}
	if option.WriteBufferSizeMB > 0 {
		wfs.writeBufferAccountant = page_writer.NewWriteBufferAccountant(option.WriteBufferSizeMB * 1024 * 1024)
		wfs.writeBufferAccountant.SetEvictor(wfs.evictOneWritableChunk)
	}

	wfs.metaCache = meta_cache.NewMetaCache(path.Join(option.getUniqueCacheDirForRead(), "meta"), option.UidGidMapper,
		util.FullPath(option.FilerMountRootPath),
		option.IncludeSystemEntries,
		func(path util.FullPath) {
			wfs.inodeToPath.MarkChildrenCached(path)
		}, func(path util.FullPath) bool {
			return wfs.inodeToPath.IsChildrenCached(path)
		}, wfs.onEntryInvalidation, nil)
	wfs.metaCache.SetPinnedChildFn(wfs.isLocalOnlyEntry)
	grace.OnInterrupt(func() {
		// grace calls os.Exit(0) after all hooks, so WaitForAsyncFlush
		// after server.Serve() would never execute.  Drain here first.
		//
		// Use a timeout to avoid hanging on Ctrl-C if the filer is
		// unreachable (metadata retry can take up to 7 seconds).
		// If the timeout expires, skip the write-cache removal so that
		// still-running goroutines can finish reading swap files.
		asyncDrained := true
		if wfs.option.WritebackCache {
			done := make(chan struct{})
			go func() {
				wfs.asyncFlushWg.Wait()
				close(done)
			}()
			select {
			case <-done:
				glog.V(0).Infof("all async flushes completed before shutdown")
			case <-time.After(30 * time.Second):
				glog.Warningf("timed out waiting for async flushes — swap files preserved for in-flight uploads")
				asyncDrained = false
			}
		}
		wfs.metaCache.Shutdown()
		if asyncDrained {
			os.RemoveAll(option.getUniqueCacheDirForWrite())
		}
		os.RemoveAll(option.getUniqueCacheDirForRead())
		if wfs.rdmaClient != nil {
			wfs.rdmaClient.Close()
		}
		if wfs.peerAnnouncer != nil {
			wfs.peerAnnouncer.Stop()
		}
		if wfs.peerConnPool != nil {
			wfs.peerConnPool.Close()
		}
		if wfs.peerGrpcServer != nil {
			wfs.peerGrpcServer.Stop()
		}
		if wfs.peerDirectoryStop != nil {
			select {
			case <-wfs.peerDirectoryStop:
				// already closed
			default:
				close(wfs.peerDirectoryStop)
			}
		}
		if wfs.peerRegistrar != nil {
			wfs.peerRegistrar.Stop()
		}
	})

	// Initialize RDMA client if enabled
	if option.RdmaEnabled && option.RdmaSidecarAddr != "" {
		rdmaClient, err := NewRDMAMountClient(
			option.RdmaSidecarAddr,
			wfs.LookupFn(),
			option.RdmaMaxConcurrent,
			option.RdmaTimeoutMs,
		)
		if err != nil {
			glog.Warningf("Failed to initialize RDMA client: %v", err)
		} else {
			wfs.rdmaClient = rdmaClient
			glog.Infof("RDMA acceleration enabled: sidecar=%s, maxConcurrent=%d, timeout=%dms",
				option.RdmaSidecarAddr, option.RdmaMaxConcurrent, option.RdmaTimeoutMs)
		}
	}

	// Peer chunk sharing: register with every configured filer's mount
	// registry + start the single gRPC server that handles ChunkAnnounce /
	// ChunkLookup / FetchChunk. Broadcasting registration to the full
	// filer set is what lets mounts pointing at different filers see
	// each other — each filer's registry is in-memory with no
	// filer-to-filer sync, so the registrar reconstructs the union
	// client-side. One port, one identity — the advertise address
	// resolved in PR #3 is used for everything.
	if option.PeerEnabled {
		selfAddr, err := ResolvePeerAdvertiseAddr(option.PeerListen, option.PeerAdvertise)
		if err != nil {
			// Downstream code treats PeerEnabled as "peer infrastructure
			// is ready": later PRs wire the gRPC server, fetcher hook,
			// and announcer from this flag. If we can't resolve a
			// reachable self-address those components would nil-deref
			// or advertise garbage, so disable the feature instead of
			// limping along half-initialized.
			glog.Warningf("peer: cannot resolve advertise addr, disabling peer sharing: %v", err)
			option.PeerEnabled = false
		} else {
			dial := func(ctx context.Context, addr pb.ServerAddress, fn func(client filer_pb.SeaweedFilerClient) error) error {
				return pb.WithGrpcFilerClient(false, 0, addr, option.GrpcDialOption, fn)
			}
			wfs.peerRegistrar = NewPeerRegistrar(option.FilerAddresses, dial, selfAddr, option.PeerDataCenter, option.PeerRack)
			if err := wfs.peerRegistrar.Start(context.Background()); err != nil {
				glog.Warningf("peer registrar start: %v", err)
			}

			wfs.peerDirectory = NewPeerDirectory()
			// Wire TLS/mTLS from security.toml's grpc.mount section so
			// cross-host peer RPCs are authenticated + encrypted. When
			// the section is empty both options come back nil and the
			// server runs plaintext — intentional for dev/test.
			peerTLSCreds, peerTLSVerify := security.LoadServerTLS(util.GetViper(), "grpc.mount")
			var peerServerOpts []grpc.ServerOption
			if peerTLSCreds != nil {
				peerServerOpts = append(peerServerOpts, peerTLSCreds)
			}
			if peerTLSVerify != nil {
				peerServerOpts = append(peerServerOpts, peerTLSVerify)
			}
			wfs.peerGrpcServer = NewPeerGrpcServer(
				wfs.chunkCache,
				wfs.peerDirectory,
				wfs.peerRegistrar.OwnerFor,
				selfAddr,
				peerServerOpts...,
			)
			if err := wfs.peerGrpcServer.Start(option.PeerListen); err != nil {
				glog.Warningf("peer grpc start: %v", err)
				wfs.peerGrpcServer = nil
			} else {
				wfs.peerDirectoryStop = make(chan struct{})
				go wfs.runPeerDirectorySweeper(wfs.peerDirectoryStop)

				// Shared connection pool + announcer. Pool reuses one
				// grpc.ClientConn per owner mount across both the
				// announcer flush and the fetcher's ChunkLookup +
				// FetchChunk calls. Transport credentials come from
				// option.GrpcDialOption (security.LoadClientTLS), so
				// peer dials match the TLS posture the server wants.
				wfs.peerConnPool = NewPeerConnPool(option.GrpcDialOption)
				wfs.peerAnnouncer = NewPeerAnnouncer(
					selfAddr,
					option.PeerDataCenter,
					option.PeerRack,
					wfs.peerRegistrar.OwnerFor,
					wfs.peerConnPool.Dialer(),
					wfs.peerDirectory,
				)
				// Close the write→announce race: between SetChunk and
				// the flush tick (up to 15 s) the cache can LRU-evict
				// the chunk. Skip announcing fids we no longer hold.
				if wfs.chunkCache != nil {
					cache := wfs.chunkCache
					wfs.peerAnnouncer.SetCachePresence(func(fid string) bool {
						return cache.IsInCache(fid, true)
					})
				}
				wfs.peerAnnouncer.Start()
			}
		}
	}

	if wfs.option.ConcurrentWriters > 0 {
		wfs.concurrentWriters = util.NewLimitedConcurrentExecutor(wfs.option.ConcurrentWriters)
		wfs.concurrentCopiersSem = make(chan struct{}, wfs.option.ConcurrentWriters)
	}
	if wfs.option.WritebackCache {
		numWorkers := wfs.option.ConcurrentWriters
		if numWorkers <= 0 {
			numWorkers = 128
		}
		wfs.startAsyncFlushWorkers(numWorkers)
	}
	wfs.streamMutate = newStreamMutateMux(wfs)
	wfs.copyBufferPool.New = func() any {
		return make([]byte, option.ChunkSizeLimit)
	}
	return wfs
}

func (wfs *WFS) StartBackgroundTasks() error {
	if wfs.option.WritebackCache {
		glog.V(0).Infof("writebackCache enabled: async flush on close() for improved small file performance")
	}

	follower, err := wfs.subscribeFilerConfEvents()
	if err != nil {
		return err
	}

	startTime := time.Now()
	go meta_cache.SubscribeMetaEvents(wfs.metaCache, wfs.signature, wfs, wfs.LookupFn(), wfs.option.FilerMountRootPath, startTime.UnixNano(), wfs.option.WritebackCache, func(lastTsNs int64, err error) {
		glog.Warningf("meta events follow retry from %v: %v", time.Unix(0, lastTsNs), err)
		// A subscription gap may have dropped events, so distrust every cached
		// listing. Reset the flags first (safe — it never deletes entries), then
		// wipe the root's stale children through the apply loop so the delete
		// cannot strand a concurrent rebuild cached-but-empty.
		wfs.inodeToPath.InvalidateAllChildrenCache()
		wfs.purgeDirectoryCache(util.FullPath(wfs.option.FilerMountRootPath))
	}, follower)
	go wfs.loopCheckQuota()
	go wfs.loopFlushDirtyMetadata()
	go wfs.loopEvictIdleDirCache()
	go wfs.loopProactiveFlush()
	if wfs.crossMountLocks() {
		go wfs.loopRenewPosixLeases()
	}

	return nil
}

func (wfs *WFS) String() string {
	return "seaweedfs"
}

func (wfs *WFS) Init(server *fuse.Server) {
	wfs.fuseServer = server
}

// maybeReadEntry resolves an inode to the entry metadata operations act on. An
// open handle answers ahead of the path: unlink drops the name while the
// descriptor stays valid, so an unlinked-but-open file returns its handle's
// entry with an empty path instead of ENOENT. A removed directory has no
// handle; its remembered entry answers the same way.
func (wfs *WFS) maybeReadEntry(inode uint64) (path util.FullPath, fh *FileHandle, entry *filer_pb.Entry, status fuse.Status) {
	var found bool
	if fh, found = wfs.fhMap.FindFileHandle(inode); found {
		path, _ = wfs.inodeToPath.GetPath(inode)
		entry = fh.UpdateEntry(func(entry *filer_pb.Entry) {
			if entry != nil && fh.entry.Attributes == nil {
				entry.Attributes = &filer_pb.FuseAttributes{}
			}
		})
		return path, fh, entry, fuse.OK
	}
	path, status = wfs.inodeToPath.GetPath(inode)
	if status != fuse.OK {
		if entry = wfs.removedDirEntry(inode); entry != nil {
			return "", nil, entry, fuse.OK
		}
		return
	}
	entry, _, status = wfs.maybeLoadEntry(path)
	return
}

// isLocalOnlyEntry reports whether entry holds local-only state not yet on the
// filer — an open handle with dirty metadata, or a pending async flush. A
// directory rebuild refills from a filer listing that omits such an entry, so it
// must be preserved across the wipe; this is the same signal lookupEntry trusts
// over a filer ErrNotFound for deferred creates.
//
// Keyed off the inode the entry carries, not inodeToPath: a kernel Forget can
// drop the path→inode mapping while an async writeback flush is still in flight,
// and the entry must stay pinned until that flush reaches the filer.
func (wfs *WFS) isLocalOnlyEntry(entry *filer.Entry) bool {
	if entry == nil || entry.Attr.Inode == 0 {
		return false
	}
	inode := entry.Attr.Inode
	if fh, fhFound := wfs.fhMap.FindFileHandle(inode); fhFound && fh.dirtyMetadata {
		return true
	}
	wfs.pendingAsyncFlushMu.Lock()
	_, pending := wfs.pendingAsyncFlush[inode]
	wfs.pendingAsyncFlushMu.Unlock()
	return pending
}

// maybeLoadEntry returns the entry and the log position it reflects, or a zero
// position when unknown.
func (wfs *WFS) maybeLoadEntry(fullpath util.FullPath) (*filer_pb.Entry, entryVersion, fuse.Status) {
	// glog.V(3).Infof("read entry cache miss %s", fullpath)
	_, name := fullpath.DirAndName()

	// return a valid entry for the mount root
	if string(fullpath) == wfs.option.FilerMountRootPath {
		return &filer_pb.Entry{
			Name:        name,
			IsDirectory: true,
			Attributes: &filer_pb.FuseAttributes{
				Mtime:    wfs.option.MountMtime.Unix(),
				FileMode: uint32(wfs.option.MountMode),
				Uid:      wfs.option.MountUid,
				Gid:      wfs.option.MountGid,
				Crtime:   wfs.option.MountCtime.Unix(),
			},
		}, entryVersion{}, fuse.OK
	}

	entry, version, status := wfs.lookupEntry(fullpath)
	if status != fuse.OK {
		return nil, entryVersion{}, status
	}
	return entry.ToProtoEntry(), version, fuse.OK
}

// lookupEntry looks up an entry by path, checking the local cache first.
// Cached metadata is only authoritative when the parent directory itself is cached.
// For uncached/read-through directories, always consult the filer directly so stale
// local entries do not leak back into lookup results.
// It also returns the log position the entry reflects: the entry's stored
// version, the lookup response's, or zero if unknown.
func (wfs *WFS) lookupEntry(fullpath util.FullPath) (*filer.Entry, entryVersion, fuse.Status) {
	dir, _ := fullpath.DirAndName()
	dirPath := util.FullPath(dir)

	if wfs.metaCache.IsDirectoryCached(dirPath) && wfs.metaCache.IsNameFresh(fullpath) {
		cachedEntry, cachedVersionTsNs, cacheErr := wfs.metaCache.FindEntry(context.Background(), fullpath)
		if cacheErr != nil && cacheErr != filer_pb.ErrNotFound {
			glog.Errorf("lookupEntry: cache lookup for %s failed: %v", fullpath, cacheErr)
			return nil, entryVersion{}, fuse.EIO
		}
		if cachedEntry != nil {
			glog.V(4).Infof("lookupEntry cache hit %s", fullpath)
			// Store versions come from applied events, not an RPC fence.
			return cachedEntry, entryVersion{tsNs: cachedVersionTsNs}, fuse.OK
		}
		// Re-check: the directory may have been evicted from cache between
		// our IsDirectoryCached check and FindEntry.
		// If it's no longer cached, fall through to the filer lookup below.
		if wfs.metaCache.IsDirectoryCached(dirPath) && wfs.metaCache.IsNameFresh(fullpath) {
			// Authoritative ENOENT only if inodeToPath also has no record.
			// If the kernel still tracks this inode, the three layers
			// disagree; trust the filer over the local cache (the
			// filer-ErrNotFound branch below logs the confirmed drift).
			if _, inodeFound := wfs.inodeToPath.GetInode(fullpath); !inodeFound {
				glog.V(4).Infof("lookupEntry cache miss (dir cached) %s", fullpath)
				return nil, entryVersion{}, fuse.ENOENT
			}
			glog.V(2).Infof("lookupEntry: %s missing from cache while parent %s is cached; inode tracked, consulting filer", fullpath, dirPath)
		}
	}

	// About to trust the filer, so first let any async flush of a just-closed
	// handle land: it would otherwise answer with pre-close metadata, a
	// truncate's old size or a write's old chunks. The cache paths above are
	// already consistent and must not pay this wait.
	if inode, found := wfs.inodeToPath.GetInode(fullpath); found {
		wfs.waitForPendingAsyncFlush(inode)
	}

	// Directory not cached - fetch directly from filer without caching the entire directory.
	glog.V(4).Infof("lookupEntry fetching from filer %s", fullpath)
	var entry *filer_pb.Entry
	var lookupVersion entryVersion
	lookupDir, lookupName := fullpath.DirAndName()
	err := wfs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		resp, lookupErr := filer_pb.LookupEntry(context.Background(), client, &filer_pb.LookupDirectoryEntryRequest{
			Directory: lookupDir,
			Name:      lookupName,
		})
		if lookupErr != nil {
			return lookupErr
		}
		entry = resp.Entry
		lookupVersion = entryVersion{tsNs: resp.LogTsNs, signature: resp.LogSignature}
		return nil
	})
	if err != nil {
		if err == filer_pb.ErrNotFound {
			// The entry may exist in the local store from a deferred create
			// (deferFilerCreate=true) that hasn't been flushed yet. Only trust
			// the local store when an open file handle or pending async flush
			// confirms the entry is genuinely local-only; otherwise a stale
			// cache hit could resurrect a deleted/renamed entry.
			inode, inodeFound := wfs.inodeToPath.GetInode(fullpath)
			hasDirtyHandle := false
			hasPendingFlush := false
			if inodeFound {
				if fh, fhFound := wfs.fhMap.FindFileHandle(inode); fhFound && fh.dirtyMetadata {
					hasDirtyHandle = true
				}
				wfs.pendingAsyncFlushMu.Lock()
				_, hasPendingFlush = wfs.pendingAsyncFlush[inode]
				wfs.pendingAsyncFlushMu.Unlock()

				if hasDirtyHandle || hasPendingFlush {
					if localEntry, localVersionTsNs, localErr := wfs.metaCache.FindEntry(context.Background(), fullpath); localErr == nil && localEntry != nil {
						glog.V(4).Infof("lookupEntry found deferred entry in local cache %s", fullpath)
						return localEntry, entryVersion{tsNs: localVersionTsNs}, fuse.OK
					}
					// Cache eviction (idle, kernel Forget) can drop the local
					// placeholder a deferred create left behind. The handle
					// still holding the unflushed entry is authoritative for
					// it, so read it from there rather than reporting a file
					// that plainly exists as missing.
					if fh, fhFound := wfs.fhMap.FindFileHandle(inode); fhFound {
						// Async upload workers append chunks under this lock;
						// hold it for reading so FromPbEntry does not walk the
						// chunk slice mid-reallocation.
						fh.entryLock.RLock()
						pbEntry := fh.GetEntry().GetEntry()
						var localEntry *filer.Entry
						if pbEntry != nil {
							localEntry = filer.FromPbEntry(dir, pbEntry)
						}
						fh.entryLock.RUnlock()
						if localEntry != nil {
							glog.V(4).Infof("lookupEntry found deferred entry on its open handle %s", fullpath)
							return localEntry, entryVersion{}, fuse.OK
						}
					}
				}
			}
			if inodeFound {
				// Filer reports ErrNotFound for a path the kernel/local map
				// still tracks, with no in-flight create or flush to excuse
				// it. Log loudly (Warningf, not V(4)) so flake captures show
				// up without -v=4 — and include layer-by-layer state so the
				// next failed run pinpoints which layer dropped the entry.
				localPresent := false
				if localEntry, _, localErr := wfs.metaCache.FindEntry(context.Background(), fullpath); localErr == nil && localEntry != nil {
					localPresent = true
				}
				glog.Warningf("lookupEntry: filer ErrNotFound for tracked path %s (inode=%d dirtyHandle=%v pendingFlush=%v localCache=%v dirCached=%v) — possible coherence bug",
					fullpath, inode, hasDirtyHandle, hasPendingFlush, localPresent, wfs.metaCache.IsDirectoryCached(dirPath))
			} else {
				glog.V(4).Infof("lookupEntry not found %s", fullpath)
			}
			return nil, entryVersion{}, fuse.ENOENT
		}
		glog.Warningf("lookupEntry GetEntry %s: %v", fullpath, err)
		return nil, entryVersion{}, fuse.EIO
	}
	if entry != nil && entry.Attributes != nil && wfs.option.UidGidMapper != nil {
		entry.Attributes.Uid, entry.Attributes.Gid = wfs.option.UidGidMapper.FilerToLocal(entry.Attributes.Uid, entry.Attributes.Gid)
	}
	return filer.FromPbEntry(dir, entry), lookupVersion, fuse.OK
}

// entryVersion is a filer log position together with the clock domain it
// belongs to: the signature of the filer that stamped it, or zero when the
// position came from an event rather than an RPC fence.
type entryVersion struct {
	tsNs      int64
	signature int32
}

// sameClockDomain reports whether an event's timestamp is comparable with a
// fence stamped by the filer identified by fenceSignature. The filer that logs
// an event appends its own signature, so its presence means one clock produced
// both positions. A zero fence signature means the handle's position came from
// an event, whose ordering the subscription already provides.
func sameClockDomain(fenceSignature int32, eventSignatures []int32) bool {
	if fenceSignature == 0 {
		return true
	}
	for _, sig := range eventSignatures {
		if sig == fenceSignature {
			return true
		}
	}
	return false
}

// sameEntryContent reports whether two entries carry the same file content:
// size, inline bytes, and chunk list. Attributes are deliberately excluded —
// its caller decides whether the dirty-page overlay is still valid, and a
// metadata change (chmod, chown, touch, xattr) does not invalidate it.
func sameEntryContent(a, b *filer_pb.Entry) bool {
	if a == nil || b == nil {
		return a == b
	}
	if filer.FileSize(a) != filer.FileSize(b) || !bytes.Equal(a.Content, b.Content) {
		return false
	}
	if len(a.Chunks) != len(b.Chunks) {
		return false
	}
	for i := range a.Chunks {
		if a.Chunks[i].GetFileIdString() != b.Chunks[i].GetFileIdString() ||
			a.Chunks[i].Offset != b.Chunks[i].Offset || a.Chunks[i].Size != b.Chunks[i].Size {
			return false
		}
	}
	return true
}

// invalidateOpenFileHandle refreshes an open file handle from a metadata
// subscription event. No filer lookup here: it can fail transiently, and with
// the subscription cursor already past the event, nothing would retry.

// IsFileOpen reports whether any open file handle refers to the inode, which
// tells a caching front end that the handle's view of the file, not its own
// cached copy, is current.
func (wfs *WFS) IsFileOpen(inode uint64) bool {
	_, found := wfs.fhMap.FindFileHandle(inode)
	return found
}

// PathForInode reports the path the mount currently tracks for the inode, so
// a caching front end can tell whether a path it resolved earlier still names
// the same file. An unlinked file has no path while its handles drain.
func (wfs *WFS) PathForInode(inode uint64) (util.FullPath, bool) {
	path, status := wfs.inodeToPath.GetPath(inode)
	return path, status == fuse.OK
}

// SetEntryChangeListener registers a callback for every metadata event this
// mount applies. A front end whose client caches entries on its own side, and
// which the mount cannot invalidate directly, uses it to push the change out.
func (wfs *WFS) SetEntryChangeListener(fn func(meta_cache.EntryInvalidation)) {
	wfs.entryChangeMu.Lock()
	wfs.entryChanged = fn
	wfs.entryChangeMu.Unlock()
}

// onEntryInvalidation runs for every applied event, whether or not the path is
// open here, so a listener sees changes made anywhere in the cluster.
func (wfs *WFS) onEntryInvalidation(invalidation meta_cache.EntryInvalidation) {
	wfs.entryChangeMu.RLock()
	listener := wfs.entryChanged
	wfs.entryChangeMu.RUnlock()
	if listener != nil {
		listener(invalidation)
	}
	wfs.invalidateKernelDirListing(invalidation.Path)
	// An inode with an open handle has its rename applied above, together with
	// the handle's own path bookkeeping. This is for the rest: the kernel goes
	// on addressing a moved inode by nodeid whether or not anything holds it
	// open.
	handled, replacedInode := wfs.invalidateOpenFileHandle(invalidation)
	if !handled && invalidation.RenamedTo != "" {
		_, replacedInode = wfs.inodeToPath.MovePath(invalidation.Path, invalidation.RenamedTo)
	}
	// A rename over an existing file destroys it, so its handle must not flush
	// the old content back over what took the name. Marked from here, holding
	// no other handle's lock.
	if replacedInode != 0 {
		wfs.markHandleDeleted(replacedInode)
	}
}

// invalidateKernelDirListing drops the kernel's cached listing of the directory
// holding path. Safe here because invalidations run on their own worker, never
// on a thread serving a kernel request; notifying from a handler can deadlock
// against the page it holds, which is why the file paths avoid InodeNotify.
// A directory the kernel has not looked up has no inode here and nothing
// cached, so it is skipped.
func (wfs *WFS) invalidateKernelDirListing(path util.FullPath) {
	server := wfs.fuseServer
	if server == nil {
		return
	}
	dir, _ := path.DirAndName()
	dirInode, found := wfs.inodeToPath.GetInode(util.FullPath(dir))
	if !found {
		return
	}
	// ENOENT is the kernel not holding the inode, ENOSYS a kernel without the
	// notify; neither is worth a line.
	if status := server.InodeNotify(dirInode, 0, -1); status != fuse.OK && status != fuse.ENOENT && status != fuse.ENOSYS {
		glog.V(4).Infof("invalidate kernel listing of %s: %v", dir, status)
	}
}

// MountRoot is the filer path this mount is rooted at. Event paths are absolute
// on the filer; a front end that addresses files relative to the mount needs it
// to translate them.
func (wfs *WFS) MountRoot() util.FullPath {
	return util.FullPath(wfs.option.FilerMountRootPath)
}

// invalidateOpenFileHandle applies one event to the handle that has the entry
// open, reporting whether it found one: a handle owns its inode's rename
// bookkeeping, and the caller moves the table only for inodes without one. Any
// inode a rename destroyed comes back for the caller to mark, since marking it
// here would hold two handle locks at once.
func (wfs *WFS) invalidateOpenFileHandle(invalidation meta_cache.EntryInvalidation) (handled bool, replacedInode uint64) {
	filePath, eventEntry, eventTsNs := invalidation.Path, invalidation.Entry, invalidation.TsNs
	inode, inodeFound := wfs.inodeToPath.GetInode(filePath)
	if !inodeFound {
		return
	}
	fh, fhFound := wfs.fhMap.FindFileHandle(inode)
	if !fhFound {
		return
	}
	handled = true
	fhActiveLock := wfs.fhLockTable.AcquireLock("invalidateFunc", fh.fh, util.ExclusiveLock)
	defer wfs.fhLockTable.ReleaseLock(fh.fh, fhActiveLock)

	// A rename changes which name the inode answers to, not which version of
	// its content the handle holds, so it is applied ahead of the version fence
	// below - a fence that skipped it would strand the inode and the handle on
	// a name the filer has vacated. MovePath reports whether the source was
	// still ours to move, which is what makes a replayed rename a no-op here.
	if invalidation.RenamedTo != "" {
		if sourceInode, replaced := wfs.inodeToPath.MovePath(filePath, invalidation.RenamedTo); sourceInode != 0 {
			if replaced != inode {
				replacedInode = replaced
			}
			fh.RememberPath(invalidation.RenamedTo)
			if _, newName := invalidation.RenamedTo.DirAndName(); newName != "" {
				fh.UpdateEntry(func(entry *filer_pb.Entry) {
					if entry != nil {
						entry.Name = newName
					}
				})
			}
		}
	}

	// Invalidations apply asynchronously: the handle may already reflect this
	// event or newer state, and rolling it back would never be corrected.
	// Only skip within one clock domain — the handle's position may have been
	// stamped by a different filer, whose clock says nothing about this
	// event's. Applying across domains costs a re-apply the base-equality
	// check absorbs; skipping across them leaves the handle stale for good.
	if eventTsNs != 0 && eventTsNs <= fh.entryVersionTsNs.Load() &&
		sameClockDomain(fh.entryVersionSignature.Load(), invalidation.Signatures) {
		return
	}

	// A cached parent's store entry is the ordered merge of this event and
	// anything applied since. An uncached parent takes no store writes, so a
	// hit there is a stale leftover — use the event entry instead.
	var candidate *filer_pb.Entry
	candidateTsNs := eventTsNs
	dir, _ := filePath.DirAndName()
	if wfs.metaCache.IsDirectoryCached(util.FullPath(dir)) {
		if storeEntry, storeVersionTsNs, findErr := wfs.metaCache.FindEntry(context.Background(), filePath); findErr == nil && storeEntry != nil && storeVersionTsNs >= eventTsNs {
			candidate = storeEntry.ToProtoEntry()
			candidateTsNs = storeVersionTsNs
		}
	}
	if candidate == nil && eventEntry != nil {
		candidate = proto.Clone(eventEntry).(*filer_pb.Entry)
		if candidate.Attributes == nil {
			candidate.Attributes = &filer_pb.FuseAttributes{}
		}
		if wfs.option.UidGidMapper != nil {
			candidate.Attributes.Uid, candidate.Attributes.Gid = wfs.option.UidGidMapper.FilerToLocal(candidate.Attributes.Uid, candidate.Attributes.Gid)
		}
	}
	if candidate == nil {
		// Path vacated. A rename left the file alive at its new name, so the
		// handle follows it — an open fd tracks the inode, and leaving it on
		// the old path would make its next flush recreate that name instead of
		// updating the renamed file. An actual delete instead marks the handle
		// so no flush recreates the unlinked name. Either way the entry and
		// dirty pages stay, so the open fd still reads its buffered writes.
		if invalidation.Deleted {
			fh.isDeleted = true
			fh.deleteEpoch++
		}
		if !fh.dirtyMetadata {
			fh.dirtyPages.Destroy()
			fh.dirtyPages = newPageWriter(fh, wfs.option.ChunkSizeLimit)
		}
		fh.advanceEntryVersion(eventTsNs, 0)
		return
	}
	if candidate.Attributes != nil {
		candidate.Attributes.FileSize = filer.FileSize(candidate)
	}
	// Already reflected — an under-fenced re-delivery (a fence is a lower
	// bound). Judged against the base, not the live entry: local writes move
	// the live entry, and a re-delivered base must not discard them.
	base := fh.baseEntry.Load()
	if base != nil && proto.Equal(candidate, base) {
		fh.advanceEntryVersion(candidateTsNs, 0)
		return
	}
	// Dirty pages overlay content, so only a content change invalidates them:
	// a metadata-only event (chmod, touch, or a committed copy's own event
	// against an approximate base) leaves them valid. A dirty handle likewise
	// keeps its diverged entry unless foreign content supersedes it.
	contentChanged := base == nil || !sameEntryContent(candidate, base)
	if contentChanged {
		fh.dirtyPages.Destroy()
		fh.dirtyPages = newPageWriter(fh, wfs.option.ChunkSizeLimit)
	}
	if contentChanged || !fh.dirtyMetadata {
		fh.SetEntry(candidate)
	}
	fh.baseEntry.Store(proto.Clone(candidate).(*filer_pb.Entry))
	fh.advanceEntryVersion(candidateTsNs, 0)
	return
}

func (wfs *WFS) LookupFn() wdclient.LookupFileIdFunctionType {
	if wfs.option.VolumeServerAccess == "filerProxy" {
		return func(ctx context.Context, fileId string) (targetUrls []string, err error) {
			return []string{"http://" + wfs.getCurrentFiler().ToHttpAddress() + "/?proxyChunkId=" + fileId}, nil
		}
	}
	// Use the cached FilerClient for efficient lookups with singleflight and cache history
	return wfs.filerClient.GetLookupFileIdFunction()
}

// CacheInvalidator lets a chunk read that failed against every cached location
// drop that entry and look the volume up again, so a mount does not keep
// hammering a volume server that has since moved or died. Nil under filerProxy,
// where there is no filerClient and LookupFn never consults a location cache.
func (wfs *WFS) CacheInvalidator() filer.CacheInvalidator {
	if wfs.filerClient == nil {
		return nil
	}
	return wfs.filerClient
}

func (wfs *WFS) getCurrentFiler() pb.ServerAddress {
	i := atomic.LoadInt32(&wfs.option.filerIndex)
	return wfs.option.FilerAddresses[i]
}

func (wfs *WFS) ClearCacheDir() {
	wfs.metaCache.Shutdown()
	os.RemoveAll(wfs.option.getUniqueCacheDirForWrite())
	os.RemoveAll(wfs.option.getUniqueCacheDirForRead())
}

// purgeDirectoryCache drops a directory's cached listing from off the apply loop
// (idle eviction, kernel Forget, copy-range fallback), routing through it so a
// stale wipe can't strand a concurrently-rebuilt directory cached-but-empty.
func (wfs *WFS) purgeDirectoryCache(dirPath util.FullPath) {
	wfs.metaCache.PurgeDirectoryChildren(dirPath, func() {
		wfs.inodeToPath.InvalidateChildrenCache(dirPath)
	})
}

func (wfs *WFS) loopEvictIdleDirCache() {
	if wfs.dirIdleEvict <= 0 {
		return
	}
	ticker := time.NewTicker(wfs.dirIdleEvict / 2)
	defer ticker.Stop()
	for range ticker.C {
		dirs := wfs.inodeToPath.CollectEvictableDirs(time.Now(), wfs.dirIdleEvict)
		for _, dir := range dirs {
			wfs.purgeDirectoryCache(dir)
		}
	}
}

func (option *Option) setupUniqueCacheDirectory() {
	cacheUniqueId := util.Md5String([]byte(option.MountDirectory + string(option.FilerAddresses[0]) + option.FilerMountRootPath + version.Version()))[0:8]
	option.uniqueCacheDirForRead = path.Join(option.CacheDirForRead, cacheUniqueId)
	os.MkdirAll(option.uniqueCacheDirForRead, os.FileMode(0777)&^option.Umask)
	option.uniqueCacheDirForWrite = filepath.Join(path.Join(option.CacheDirForWrite, cacheUniqueId), "swap")
	os.MkdirAll(option.uniqueCacheDirForWrite, os.FileMode(0777)&^option.Umask)
}

func (option *Option) getUniqueCacheDirForWrite() string {
	return option.uniqueCacheDirForWrite
}

func (option *Option) getUniqueCacheDirForRead() string {
	return option.uniqueCacheDirForRead
}
