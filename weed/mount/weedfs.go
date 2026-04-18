package mount

import (
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

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/filer"
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

	"github.com/seaweedfs/go-fuse/v2/fs"
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
	DataCenter                  string
	Umask                       os.FileMode
	Quota                       int64
	DisableXAttr                bool
	IsMacOs                     bool

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
	// held (with auto-renewal) until the file is closed. Only one mount can
	// have a file open for writing at a time.
	EnableDistributedLock bool

	// WritebackCache enables async flush on close for improved small file write performance.
	// When true, Flush() returns immediately and data upload + metadata flush happen in background.
	WritebackCache bool

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
	fs.Inode
	option               *Option
	metaCache            *meta_cache.MetaCache
	stats                statsCache
	chunkCache           *chunk_cache.TieredChunkCache
	writeBufferAccountant *page_writer.WriteBufferAccountant
	signature            int32
	concurrentWriters    *util.LimitedConcurrentExecutor
	copyBufferPool       sync.Pool
	concurrentCopiersSem chan struct{}
	inodeToPath          *InodeToPath
	fhMap                *FileHandleToInode
	dhMap                *DirectoryHandleToInode
	fuseServer           *fuse.Server
	IsOverQuota          bool
	fhLockTable          *util.LockTable[FileHandleId]
	hardLinkLockTable    *util.LockTable[string]
	posixLocks           *PosixLockTable
	rdmaClient           *RDMAMountClient
	FilerConf            *filer.FilerConf
	filerClient          *wdclient.FilerClient // Cached volume location client
	refreshMu            sync.Mutex
	refreshingDirs       map[util.FullPath]struct{}
	atimeMu              sync.Mutex
	atimeMap             map[uint64]time.Time // inode -> atime, in-memory only, bounded
	dirMtimeMu           sync.Mutex
	dirMtimeMap          map[uint64]time.Time // inode -> mtime/ctime, in-memory overlay for dirs
	entryValidSec        uint64 // kernel FUSE entry cache TTL in seconds
	attrValidSec         uint64 // kernel FUSE attr cache TTL in seconds
	dirHotWindow         time.Duration
	dirHotThreshold      int
	dirIdleEvict         time.Duration

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

const (
	defaultDirHotWindow    = 2 * time.Second
	defaultDirHotThreshold = 64
	defaultDirIdleEvict    = 10 * time.Minute
)

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

	dirHotWindow := defaultDirHotWindow
	dirHotThreshold := defaultDirHotThreshold
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
		refreshingDirs:    make(map[util.FullPath]struct{}),
		atimeMap:          make(map[uint64]time.Time, 8192),
		openMtimeCache:    make(map[uint64][2]int64, 8192),
		dirMtimeMap:       make(map[uint64]time.Time, 1024),
		entryValidSec:    1,
		attrValidSec:     1,
		dirHotWindow:      dirHotWindow,
		dirHotThreshold:   dirHotThreshold,
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
		}, func(filePath util.FullPath, entry *filer_pb.Entry) {
			// Find inode if it is not a deleted path
			if inode, inodeFound := wfs.inodeToPath.GetInode(filePath); inodeFound {
				// Find open file handle
				if fh, fhFound := wfs.fhMap.FindFileHandle(inode); fhFound {
					fhActiveLock := fh.wfs.fhLockTable.AcquireLock("invalidateFunc", fh.fh, util.ExclusiveLock)
					defer fh.wfs.fhLockTable.ReleaseLock(fh.fh, fhActiveLock)

					// Recreate dirty pages
					fh.dirtyPages.Destroy()
					fh.dirtyPages = newPageWriter(fh, wfs.option.ChunkSizeLimit)

					// Update handle entry
					newEntry, status := wfs.maybeLoadEntry(filePath)
					if status == fuse.OK {
						if fh.GetEntry().GetEntry() != newEntry {
							fh.SetEntry(newEntry)
						}
					}
				}
			}
		}, func(dirPath util.FullPath) {
			if wfs.inodeToPath.RecordDirectoryUpdate(dirPath, time.Now(), wfs.dirHotWindow, wfs.dirHotThreshold) {
				wfs.markDirectoryReadThrough(dirPath)
			}
		})
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
	go meta_cache.SubscribeMetaEvents(wfs.metaCache, wfs.signature, wfs, wfs.option.FilerMountRootPath, startTime.UnixNano(), wfs.option.WritebackCache, func(lastTsNs int64, err error) {
		glog.Warningf("meta events follow retry from %v: %v", time.Unix(0, lastTsNs), err)
		if deleteErr := wfs.metaCache.DeleteFolderChildren(context.Background(), util.FullPath(wfs.option.FilerMountRootPath)); deleteErr != nil {
			glog.Warningf("meta cache cleanup failed: %v", deleteErr)
		}
		wfs.inodeToPath.InvalidateAllChildrenCache()
	}, follower)
	go wfs.loopCheckQuota()
	go wfs.loopFlushDirtyMetadata()
	go wfs.loopEvictIdleDirCache()
	go wfs.loopProactiveFlush()

	return nil
}

func (wfs *WFS) String() string {
	return "seaweedfs"
}

func (wfs *WFS) Init(server *fuse.Server) {
	wfs.fuseServer = server
}

func (wfs *WFS) maybeReadEntry(inode uint64) (path util.FullPath, fh *FileHandle, entry *filer_pb.Entry, status fuse.Status) {
	path, status = wfs.inodeToPath.GetPath(inode)
	if status != fuse.OK {
		return
	}
	var found bool
	if fh, found = wfs.fhMap.FindFileHandle(inode); found {
		entry = fh.UpdateEntry(func(entry *filer_pb.Entry) {
			if entry != nil && fh.entry.Attributes == nil {
				entry.Attributes = &filer_pb.FuseAttributes{}
			}
		})
	} else {
		entry, status = wfs.maybeLoadEntry(path)
	}
	return
}

func (wfs *WFS) maybeLoadEntry(fullpath util.FullPath) (*filer_pb.Entry, fuse.Status) {
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
		}, fuse.OK
	}

	entry, status := wfs.lookupEntry(fullpath)
	if status != fuse.OK {
		return nil, status
	}
	return entry.ToProtoEntry(), fuse.OK
}

// lookupEntry looks up an entry by path, checking the local cache first.
// Cached metadata is only authoritative when the parent directory itself is cached.
// For uncached/read-through directories, always consult the filer directly so stale
// local entries do not leak back into lookup results.
func (wfs *WFS) lookupEntry(fullpath util.FullPath) (*filer.Entry, fuse.Status) {
	dir, _ := fullpath.DirAndName()
	dirPath := util.FullPath(dir)

	if wfs.metaCache.IsDirectoryCached(dirPath) {
		cachedEntry, cacheErr := wfs.metaCache.FindEntry(context.Background(), fullpath)
		if cacheErr != nil && cacheErr != filer_pb.ErrNotFound {
			glog.Errorf("lookupEntry: cache lookup for %s failed: %v", fullpath, cacheErr)
			return nil, fuse.EIO
		}
		if cachedEntry != nil {
			glog.V(4).Infof("lookupEntry cache hit %s", fullpath)
			return cachedEntry, fuse.OK
		}
		// Re-check: the directory may have been evicted from cache between
		// our IsDirectoryCached check and FindEntry (e.g. markDirectoryReadThrough).
		// If it's no longer cached, fall through to the filer lookup below.
		if wfs.metaCache.IsDirectoryCached(dirPath) {
			glog.V(4).Infof("lookupEntry cache miss (dir cached) %s", fullpath)
			return nil, fuse.ENOENT
		}
	}

	// Directory not cached - fetch directly from filer without caching the entire directory.
	glog.V(4).Infof("lookupEntry fetching from filer %s", fullpath)
	entry, err := filer_pb.GetEntry(context.Background(), wfs, fullpath)
	if err != nil {
		if err == filer_pb.ErrNotFound {
			// The entry may exist in the local store from a deferred create
			// (deferFilerCreate=true) that hasn't been flushed yet. Only trust
			// the local store when an open file handle or pending async flush
			// confirms the entry is genuinely local-only; otherwise a stale
			// cache hit could resurrect a deleted/renamed entry.
			if inode, inodeFound := wfs.inodeToPath.GetInode(fullpath); inodeFound {
				hasDirtyHandle := false
				if fh, fhFound := wfs.fhMap.FindFileHandle(inode); fhFound && fh.dirtyMetadata {
					hasDirtyHandle = true
				}
				wfs.pendingAsyncFlushMu.Lock()
				_, hasPendingFlush := wfs.pendingAsyncFlush[inode]
				wfs.pendingAsyncFlushMu.Unlock()

				if hasDirtyHandle || hasPendingFlush {
					if localEntry, localErr := wfs.metaCache.FindEntry(context.Background(), fullpath); localErr == nil && localEntry != nil {
						glog.V(4).Infof("lookupEntry found deferred entry in local cache %s", fullpath)
						return localEntry, fuse.OK
					}
				}
			}
			glog.V(4).Infof("lookupEntry not found %s", fullpath)
			return nil, fuse.ENOENT
		}
		glog.Warningf("lookupEntry GetEntry %s: %v", fullpath, err)
		return nil, fuse.EIO
	}
	if entry != nil && entry.Attributes != nil && wfs.option.UidGidMapper != nil {
		entry.Attributes.Uid, entry.Attributes.Gid = wfs.option.UidGidMapper.FilerToLocal(entry.Attributes.Uid, entry.Attributes.Gid)
	}
	return filer.FromPbEntry(dir, entry), fuse.OK
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

func (wfs *WFS) getCurrentFiler() pb.ServerAddress {
	i := atomic.LoadInt32(&wfs.option.filerIndex)
	return wfs.option.FilerAddresses[i]
}

func (wfs *WFS) ClearCacheDir() {
	wfs.metaCache.Shutdown()
	os.RemoveAll(wfs.option.getUniqueCacheDirForWrite())
	os.RemoveAll(wfs.option.getUniqueCacheDirForRead())
}

func (wfs *WFS) markDirectoryReadThrough(dirPath util.FullPath) {
	if !wfs.inodeToPath.MarkDirectoryReadThrough(dirPath, time.Now()) {
		return
	}
	if err := wfs.metaCache.DeleteFolderChildren(context.Background(), dirPath); err != nil {
		glog.V(2).Infof("clear dir cache %s: %v", dirPath, err)
	}
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
			if err := wfs.metaCache.DeleteFolderChildren(context.Background(), dir); err != nil {
				glog.V(2).Infof("evict dir cache %s: %v", dir, err)
			}
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
