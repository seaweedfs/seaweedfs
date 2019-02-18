package filesys

import (
	"context"
	"fmt"
	"math"
	"os"
	"sync"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/karlseguin/ccache"
	"github.com/seaweedfs/fuse"
	"github.com/seaweedfs/fuse/fs"
	"google.golang.org/grpc"
)

type Option struct {
	FilerGrpcAddress   string
	GrpcDialOption     grpc.DialOption
	FilerMountRootPath string
	Collection         string
	Replication        string
	TtlSec             int32
	ChunkSizeLimit     int64
	DataCenter         string
	DirListingLimit    int
	EntryCacheTtl      time.Duration

	MountUid  uint32
	MountGid  uint32
	MountMode os.FileMode
}

var _ = fs.FS(&WFS{})
var _ = fs.FSStatfser(&WFS{})

type WFS struct {
	option                    *Option
	listDirectoryEntriesCache *ccache.Cache

	// contains all open handles
	handles           []*FileHandle
	pathToHandleIndex map[string]int
	pathToHandleLock  sync.Mutex
	bufPool           sync.Pool

	stats statsCache
}
type statsCache struct {
	filer_pb.StatisticsResponse
	lastChecked int64 // unix time in seconds
}

func NewSeaweedFileSystem(option *Option) *WFS {
	wfs := &WFS{
		option:                    option,
		listDirectoryEntriesCache: ccache.New(ccache.Configure().MaxSize(1024 * 8).ItemsToPrune(100)),
		pathToHandleIndex:         make(map[string]int),
		bufPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, option.ChunkSizeLimit)
			},
		},
	}

	return wfs
}

func (wfs *WFS) Root() (fs.Node, error) {
	return &Dir{Path: wfs.option.FilerMountRootPath, wfs: wfs}, nil
}

func (wfs *WFS) withFilerClient(fn func(filer_pb.SeaweedFilerClient) error) error {

	return util.WithCachedGrpcClient(func(grpcConnection *grpc.ClientConn) error {
		client := filer_pb.NewSeaweedFilerClient(grpcConnection)
		return fn(client)
	}, wfs.option.FilerGrpcAddress, wfs.option.GrpcDialOption)

}

func (wfs *WFS) AcquireHandle(file *File, uid, gid uint32) (fileHandle *FileHandle) {
	wfs.pathToHandleLock.Lock()
	defer wfs.pathToHandleLock.Unlock()

	fullpath := file.fullpath()

	index, found := wfs.pathToHandleIndex[fullpath]
	if found && wfs.handles[index] != nil {
		glog.V(2).Infoln(fullpath, "found fileHandle id", index)
		return wfs.handles[index]
	}

	fileHandle = newFileHandle(file, uid, gid)
	for i, h := range wfs.handles {
		if h == nil {
			wfs.handles[i] = fileHandle
			fileHandle.handle = uint64(i)
			wfs.pathToHandleIndex[fullpath] = i
			glog.V(4).Infoln(fullpath, "reuse fileHandle id", fileHandle.handle)
			return
		}
	}

	wfs.handles = append(wfs.handles, fileHandle)
	fileHandle.handle = uint64(len(wfs.handles) - 1)
	glog.V(2).Infoln(fullpath, "new fileHandle id", fileHandle.handle)
	wfs.pathToHandleIndex[fullpath] = int(fileHandle.handle)

	return
}

func (wfs *WFS) ReleaseHandle(fullpath string, handleId fuse.HandleID) {
	wfs.pathToHandleLock.Lock()
	defer wfs.pathToHandleLock.Unlock()

	glog.V(4).Infof("%s releasing handle id %d current handles length %d", fullpath, handleId, len(wfs.handles))
	delete(wfs.pathToHandleIndex, fullpath)
	if int(handleId) < len(wfs.handles) {
		wfs.handles[int(handleId)] = nil
	}

	return
}

// Statfs is called to obtain file system metadata. Implements fuse.FSStatfser
func (wfs *WFS) Statfs(ctx context.Context, req *fuse.StatfsRequest, resp *fuse.StatfsResponse) error {

	glog.V(4).Infof("reading fs stats: %+v", req)

	if wfs.stats.lastChecked < time.Now().Unix()-20 {

		err := wfs.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

			request := &filer_pb.StatisticsRequest{
				Collection:  wfs.option.Collection,
				Replication: wfs.option.Replication,
				Ttl:         fmt.Sprintf("%ds", wfs.option.TtlSec),
			}

			glog.V(4).Infof("reading filer stats: %+v", request)
			resp, err := client.Statistics(ctx, request)
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
