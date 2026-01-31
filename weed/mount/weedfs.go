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

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mount/meta_cache"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mount_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/chunk_cache"
	"github.com/seaweedfs/seaweedfs/weed/util/grace"
	"github.com/seaweedfs/seaweedfs/weed/util/version"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"

	"github.com/seaweedfs/go-fuse/v2/fs"
)

type Option struct {
	filerIndex         int32 // align memory for atomic read/write
	FilerAddresses     []pb.ServerAddress
	MountDirectory     string
	GrpcDialOption     grpc.DialOption
	FilerMountRootPath string
	Collection         string
	Replication        string
	TtlSec             int32
	DiskType           types.DiskType
	ChunkSizeLimit     int64
	ConcurrentWriters  int
	ConcurrentReaders  int
	CacheDirForRead    string
	CacheSizeMBForRead int64
	CacheDirForWrite   string
	CacheMetaTTlSec    int
	DataCenter         string
	Umask              os.FileMode
	Quota              int64
	DisableXAttr       bool
	IsMacOs            bool

	MountUid         uint32
	MountGid         uint32
	MountMode        os.FileMode
	MountCtime       time.Time
	MountMtime       time.Time
	MountParentInode uint64

	VolumeServerAccess string // how to access volume servers
	Cipher             bool   // whether encrypt data on volume server
	UidGidMapper       *meta_cache.UidGidMapper

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

	// Directory cache refresh/eviction controls
	DirIdleEvictSec int

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
	rdmaClient           *RDMAMountClient
	FilerConf            *filer.FilerConf
	filerClient          *wdclient.FilerClient // Cached volume location client
	refreshMu            sync.Mutex
	refreshingDirs       map[util.FullPath]struct{}
	dirHotWindow         time.Duration
	dirHotThreshold      int
	dirIdleEvict         time.Duration
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
	}

	wfs := &WFS{
		RawFileSystem:   fuse.NewDefaultRawFileSystem(),
		option:          option,
		signature:       util.RandomInt32(),
		inodeToPath:     NewInodeToPath(util.FullPath(option.FilerMountRootPath), option.CacheMetaTTlSec),
		fhMap:           NewFileHandleToInode(),
		dhMap:           NewDirectoryHandleToInode(),
		filerClient:     filerClient, // nil for proxy mode, initialized for direct access
		fhLockTable:     util.NewLockTable[FileHandleId](),
		refreshingDirs:  make(map[util.FullPath]struct{}),
		dirHotWindow:    dirHotWindow,
		dirHotThreshold: dirHotThreshold,
		dirIdleEvict:    dirIdleEvict,
	}

	wfs.option.filerIndex = int32(rand.IntN(len(option.FilerAddresses)))
	wfs.option.setupUniqueCacheDirectory()
	if option.CacheSizeMBForRead > 0 {
		wfs.chunkCache = chunk_cache.NewTieredChunkCache(256, option.getUniqueCacheDirForRead(), option.CacheSizeMBForRead, 1024*1024)
	}

	wfs.metaCache = meta_cache.NewMetaCache(path.Join(option.getUniqueCacheDirForRead(), "meta"), option.UidGidMapper,
		util.FullPath(option.FilerMountRootPath),
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
				wfs.maybeRefreshDirectory(dirPath)
			}
		})
	grace.OnInterrupt(func() {
		wfs.metaCache.Shutdown()
		os.RemoveAll(option.getUniqueCacheDirForWrite())
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
	wfs.copyBufferPool.New = func() any {
		return make([]byte, option.ChunkSizeLimit)
	}
	return wfs
}

func (wfs *WFS) StartBackgroundTasks() error {
	follower, err := wfs.subscribeFilerConfEvents()
	if err != nil {
		return err
	}

	startTime := time.Now()
	go meta_cache.SubscribeMetaEvents(wfs.metaCache, wfs.signature, wfs, wfs.option.FilerMountRootPath, startTime.UnixNano(), func(lastTsNs int64, err error) {
		glog.Warningf("meta events follow retry from %v: %v", time.Unix(0, lastTsNs), err)
		if deleteErr := wfs.metaCache.DeleteFolderChildren(context.Background(), util.FullPath(wfs.option.FilerMountRootPath)); deleteErr != nil {
			glog.Warningf("meta cache cleanup failed: %v", deleteErr)
		}
		wfs.inodeToPath.InvalidateAllChildrenCache()
	}, follower)
	go wfs.loopCheckQuota()
	go wfs.loopFlushDirtyMetadata()
	go wfs.loopEvictIdleDirCache()

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
// If the directory is cached, it trusts the cache. Otherwise, it fetches
// directly from the filer without caching the entire directory.
// This avoids the performance issue of listing millions of files just to open one.
func (wfs *WFS) lookupEntry(fullpath util.FullPath) (*filer.Entry, fuse.Status) {
	dir, _ := fullpath.DirAndName()

	// Try to find the entry in the local cache first.
	cachedEntry, cacheErr := wfs.metaCache.FindEntry(context.Background(), fullpath)
	if cacheErr != nil && cacheErr != filer_pb.ErrNotFound {
		glog.Errorf("lookupEntry: cache lookup for %s failed: %v", fullpath, cacheErr)
		return nil, fuse.EIO
	}
	if cachedEntry != nil {
		glog.V(4).Infof("lookupEntry cache hit %s", fullpath)
		return cachedEntry, fuse.OK
	}

	// If the directory is cached but entry not found, file doesn't exist.
	// No need to query the filer again.
	if wfs.metaCache.IsDirectoryCached(util.FullPath(dir)) {
		glog.V(4).Infof("lookupEntry cache miss (dir cached) %s", fullpath)
		return nil, fuse.ENOENT
	}

	// Directory not cached - fetch directly from filer without caching the entire directory.
	glog.V(4).Infof("lookupEntry fetching from filer %s", fullpath)
	entry, err := filer_pb.GetEntry(context.Background(), wfs, fullpath)
	if err != nil {
		glog.V(1).Infof("lookupEntry GetEntry %s: %v", fullpath, err)
		return nil, fuse.ENOENT
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

func (wfs *WFS) maybeRefreshDirectory(dirPath util.FullPath) {
	if !wfs.inodeToPath.NeedsRefresh(dirPath) {
		return
	}
	now := time.Now()
	wfs.refreshMu.Lock()
	if _, exists := wfs.refreshingDirs[dirPath]; exists {
		wfs.refreshMu.Unlock()
		return
	}
	wfs.refreshingDirs[dirPath] = struct{}{}
	wfs.refreshMu.Unlock()

	go func() {
		defer func() {
			wfs.refreshMu.Lock()
			delete(wfs.refreshingDirs, dirPath)
			wfs.refreshMu.Unlock()
		}()
		wfs.inodeToPath.InvalidateChildrenCache(dirPath)
		if err := meta_cache.EnsureVisited(wfs.metaCache, wfs, dirPath); err != nil {
			glog.Warningf("refresh dir cache %s: %v", dirPath, err)
			return
		}
		wfs.inodeToPath.MarkDirectoryRefreshed(dirPath, now)
	}()
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
