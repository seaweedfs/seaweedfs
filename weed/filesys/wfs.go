package filesys

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"math"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/storage/types"
	"github.com/chrislusf/seaweedfs/weed/wdclient"

	"google.golang.org/grpc"

	"github.com/chrislusf/seaweedfs/weed/util/grace"

	"github.com/seaweedfs/fuse"
	"github.com/seaweedfs/fuse/fs"

	"github.com/chrislusf/seaweedfs/weed/filesys/meta_cache"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/chrislusf/seaweedfs/weed/util/chunk_cache"
)

type Option struct {
	MountDirectory     string
	FilerAddresses     []pb.ServerAddress
	filerIndex         int
	GrpcDialOption     grpc.DialOption
	FilerMountRootPath string
	Collection         string
	Replication        string
	TtlSec             int32
	DiskType           types.DiskType
	ChunkSizeLimit     int64
	ConcurrentWriters  int
	CacheDir           string
	CacheSizeMB        int64
	DataCenter         string
	Umask              os.FileMode

	MountUid         uint32
	MountGid         uint32
	MountMode        os.FileMode
	MountCtime       time.Time
	MountMtime       time.Time
	MountParentInode uint64

	VolumeServerAccess string // how to access volume servers
	Cipher             bool   // whether encrypt data on volume server
	UidGidMapper       *meta_cache.UidGidMapper

	uniqueCacheDir         string
	uniqueCacheTempPageDir string
}

var _ = fs.FS(&WFS{})
var _ = fs.FSStatfser(&WFS{})

type WFS struct {
	option *Option

	// contains all open handles, protected by handlesLock
	handlesLock sync.Mutex
	handles     map[uint64]*FileHandle

	bufPool sync.Pool

	stats statsCache

	root        fs.Node
	fsNodeCache *FsCache

	chunkCache *chunk_cache.TieredChunkCache
	metaCache  *meta_cache.MetaCache
	signature  int32

	// throttle writers
	concurrentWriters *util.LimitedConcurrentExecutor
	Server            *fs.Server
}
type statsCache struct {
	filer_pb.StatisticsResponse
	lastChecked int64 // unix time in seconds
}

func NewSeaweedFileSystem(option *Option) *WFS {
	wfs := &WFS{
		option:  option,
		handles: make(map[uint64]*FileHandle),
		bufPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, option.ChunkSizeLimit)
			},
		},
		signature: util.RandomInt32(),
	}
	wfs.option.filerIndex = rand.Intn(len(option.FilerAddresses))
	wfs.option.setupUniqueCacheDirectory()
	if option.CacheSizeMB > 0 {
		wfs.chunkCache = chunk_cache.NewTieredChunkCache(256, option.getUniqueCacheDir(), option.CacheSizeMB, 1024*1024)
	}

	wfs.metaCache = meta_cache.NewMetaCache(path.Join(option.getUniqueCacheDir(), "meta"), util.FullPath(option.FilerMountRootPath), option.UidGidMapper, func(filePath util.FullPath, entry *filer_pb.Entry) {

		fsNode := NodeWithId(filePath.AsInode(entry.FileMode()))
		if err := wfs.Server.InvalidateNodeData(fsNode); err != nil {
			glog.V(4).Infof("InvalidateNodeData %s : %v", filePath, err)
		}

		dir, name := filePath.DirAndName()
		parent := NodeWithId(util.FullPath(dir).AsInode(os.ModeDir))
		if dir == option.FilerMountRootPath {
			parent = NodeWithId(1)
		}
		if err := wfs.Server.InvalidateEntry(parent, name); err != nil {
			glog.V(4).Infof("InvalidateEntry %s : %v", filePath, err)
		}
	})
	grace.OnInterrupt(func() {
		wfs.metaCache.Shutdown()
	})

	wfs.root = &Dir{name: wfs.option.FilerMountRootPath, wfs: wfs, id: 1}
	wfs.fsNodeCache = newFsCache(wfs.root)

	if wfs.option.ConcurrentWriters > 0 {
		wfs.concurrentWriters = util.NewLimitedConcurrentExecutor(wfs.option.ConcurrentWriters)
	}

	return wfs
}

func (wfs *WFS) StartBackgroundTasks() {
	startTime := time.Now()
	go meta_cache.SubscribeMetaEvents(wfs.metaCache, wfs.signature, wfs, wfs.option.FilerMountRootPath, startTime.UnixNano())
}

func (wfs *WFS) Root() (fs.Node, error) {
	return wfs.root, nil
}

func (wfs *WFS) AcquireHandle(file *File, uid, gid uint32) (fileHandle *FileHandle) {

	fullpath := file.fullpath()
	glog.V(4).Infof("AcquireHandle %s uid=%d gid=%d", fullpath, uid, gid)

	inodeId := file.Id()

	wfs.handlesLock.Lock()
	existingHandle, found := wfs.handles[inodeId]
	if found && existingHandle != nil && existingHandle.f.isOpen > 0 {
		existingHandle.f.isOpen++
		wfs.handlesLock.Unlock()
		glog.V(4).Infof("Reuse AcquiredHandle %s open %d", fullpath, existingHandle.f.isOpen)
		return existingHandle
	}
	wfs.handlesLock.Unlock()

	entry, _ := file.maybeLoadEntry(context.Background())
	file.entry = entry
	fileHandle = newFileHandle(file, uid, gid)

	wfs.handlesLock.Lock()
	file.isOpen++
	wfs.handles[inodeId] = fileHandle
	wfs.handlesLock.Unlock()
	fileHandle.handle = inodeId

	glog.V(4).Infof("Acquired new Handle %s open %d", fullpath, file.isOpen)
	return
}

func (wfs *WFS) Fsync(file *File, header fuse.Header) error {

	inodeId := file.Id()

	wfs.handlesLock.Lock()
	existingHandle, found := wfs.handles[inodeId]
	wfs.handlesLock.Unlock()

	if found && existingHandle != nil {

		existingHandle.Lock()
		defer existingHandle.Unlock()

		if existingHandle.f.isOpen > 0 {
			return existingHandle.doFlush(context.Background(), header)
		}

	}

	return nil
}

func (wfs *WFS) ReleaseHandle(fullpath util.FullPath, handleId fuse.HandleID) {
	wfs.handlesLock.Lock()
	defer wfs.handlesLock.Unlock()

	glog.V(4).Infof("ReleaseHandle %s id %d current handles length %d", fullpath, handleId, len(wfs.handles))

	delete(wfs.handles, uint64(handleId))

	return
}

// Statfs is called to obtain file system metadata. Implements fuse.FSStatfser
func (wfs *WFS) Statfs(ctx context.Context, req *fuse.StatfsRequest, resp *fuse.StatfsResponse) error {

	glog.V(4).Infof("reading fs stats: %+v", req)

	if wfs.stats.lastChecked < time.Now().Unix()-20 {

		err := wfs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

			request := &filer_pb.StatisticsRequest{
				Collection:  wfs.option.Collection,
				Replication: wfs.option.Replication,
				Ttl:         fmt.Sprintf("%ds", wfs.option.TtlSec),
				DiskType:    string(wfs.option.DiskType),
			}

			glog.V(4).Infof("reading filer stats: %+v", request)
			resp, err := client.Statistics(context.Background(), request)
			if err != nil {
				glog.V(0).Infof("reading filer stats %v: %v", request, err)
				return err
			}
			glog.V(4).Infof("read filer stats: %+v", resp)

			wfs.stats.TotalSize = resp.TotalSize
			wfs.stats.UsedSize = resp.UsedSize
			wfs.stats.FileCount = resp.FileCount
			wfs.stats.lastChecked = time.Now().Unix()

			return nil
		})
		if err != nil {
			glog.V(0).Infof("filer Statistics: %v", err)
			return err
		}
	}

	totalDiskSize := wfs.stats.TotalSize
	usedDiskSize := wfs.stats.UsedSize
	actualFileCount := wfs.stats.FileCount

	// Compute the total number of available blocks
	resp.Blocks = totalDiskSize / blockSize

	// Compute the number of used blocks
	numBlocks := uint64(usedDiskSize / blockSize)

	// Report the number of free and available blocks for the block size
	resp.Bfree = resp.Blocks - numBlocks
	resp.Bavail = resp.Blocks - numBlocks
	resp.Bsize = uint32(blockSize)

	// Report the total number of possible files in the file system (and those free)
	resp.Files = math.MaxInt64
	resp.Ffree = math.MaxInt64 - actualFileCount

	// Report the maximum length of a name and the minimum fragment size
	resp.Namelen = 1024
	resp.Frsize = uint32(blockSize)

	return nil
}

func (wfs *WFS) mapPbIdFromFilerToLocal(entry *filer_pb.Entry) {
	if entry.Attributes == nil {
		return
	}
	entry.Attributes.Uid, entry.Attributes.Gid = wfs.option.UidGidMapper.FilerToLocal(entry.Attributes.Uid, entry.Attributes.Gid)
}
func (wfs *WFS) mapPbIdFromLocalToFiler(entry *filer_pb.Entry) {
	if entry.Attributes == nil {
		return
	}
	entry.Attributes.Uid, entry.Attributes.Gid = wfs.option.UidGidMapper.LocalToFiler(entry.Attributes.Uid, entry.Attributes.Gid)
}

func (wfs *WFS) LookupFn() wdclient.LookupFileIdFunctionType {
	if wfs.option.VolumeServerAccess == "filerProxy" {
		return func(fileId string) (targetUrls []string, err error) {
			return []string{"http://" + wfs.getCurrentFiler().ToHttpAddress() + "/?proxyChunkId=" + fileId}, nil
		}
	}
	return filer.LookupFn(wfs)
}
func (wfs *WFS) getCurrentFiler() pb.ServerAddress {
	return wfs.option.FilerAddresses[wfs.option.filerIndex]
}

func (option *Option) setupUniqueCacheDirectory() {
	cacheUniqueId := util.Md5String([]byte(option.MountDirectory + string(option.FilerAddresses[0]) + option.FilerMountRootPath + util.Version()))[0:8]
	option.uniqueCacheDir = path.Join(option.CacheDir, cacheUniqueId)
	option.uniqueCacheTempPageDir = filepath.Join(option.uniqueCacheDir, "sw")
	os.MkdirAll(option.uniqueCacheTempPageDir, os.FileMode(0777)&^option.Umask)
}

func (option *Option) getTempFilePageDir() string {
	return option.uniqueCacheTempPageDir
}
func (option *Option) getUniqueCacheDir() string {
	return option.uniqueCacheDir
}

type NodeWithId uint64

func (n NodeWithId) Id() uint64 {
	return uint64(n)
}
func (n NodeWithId) Attr(ctx context.Context, attr *fuse.Attr) error {
	return nil
}
