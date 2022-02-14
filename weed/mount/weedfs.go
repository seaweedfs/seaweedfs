package mount

import (
	"context"
	"github.com/chrislusf/seaweedfs/weed/filesys/meta_cache"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/types"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/chrislusf/seaweedfs/weed/util/grace"
	"github.com/hanwen/go-fuse/v2/fuse"
	"google.golang.org/grpc"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
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

type WFS struct {
	// follow https://github.com/hanwen/go-fuse/blob/master/fuse/api.go
	fuse.RawFileSystem
	fs.Inode
	option      *Option
	metaCache   *meta_cache.MetaCache
	stats       statsCache
	root        Directory
	signature   int32
	inodeToPath *InodeToPath
	fhmap       *FileHandleToInode
}

func NewSeaweedFileSystem(option *Option) *WFS {
	wfs := &WFS{
		RawFileSystem: fuse.NewDefaultRawFileSystem(),
		option:        option,
		signature:     util.RandomInt32(),
		inodeToPath:   NewInodeToPath(),
		fhmap:         NewFileHandleToInode(),
	}

	wfs.root = Directory{
		name:   "/",
		wfs:    wfs,
		entry:  nil,
		parent: nil,
	}

	wfs.metaCache = meta_cache.NewMetaCache(path.Join(option.getUniqueCacheDir(), "meta"), util.FullPath(option.FilerMountRootPath), option.UidGidMapper, func(filePath util.FullPath, entry *filer_pb.Entry) {
	})
	grace.OnInterrupt(func() {
		wfs.metaCache.Shutdown()
	})

	return wfs
}

func (wfs *WFS) Root() *Directory {
	return &wfs.root
}

func (wfs *WFS) String() string {
	return "seaweedfs"
}

func (wfs *WFS) maybeReadEntry(inode uint64) (path util.FullPath, entry *filer_pb.Entry, status fuse.Status) {
	path = wfs.inodeToPath.GetPath(inode)
	entry, status = wfs.maybeLoadEntry(path)
	return
}

func (wfs *WFS) maybeLoadEntry(fullpath util.FullPath) (*filer_pb.Entry, fuse.Status) {

	// glog.V(3).Infof("read entry cache miss %s", fullpath)
	dir, name := fullpath.DirAndName()

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

	// TODO Use inode to selectively filetering metadata updates
	// read from async meta cache
	meta_cache.EnsureVisited(wfs.metaCache, wfs, util.FullPath(dir))
	cachedEntry, cacheErr := wfs.metaCache.FindEntry(context.Background(), fullpath)
	if cacheErr == filer_pb.ErrNotFound {
		return nil, fuse.ENOENT
	}
	return cachedEntry.ToProtoEntry(), fuse.OK
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
