package filesys

import (
	"fmt"
	"sync"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/karlseguin/ccache"
	"google.golang.org/grpc"
)

type Option struct {
	FilerGrpcAddress   string
	FilerMountRootPath string
	Collection         string
	Replication        string
	TtlSec             int32
	ChunkSizeLimit     int64
	DataCenter         string
	DirListingLimit    int
	EntryCacheTtl      time.Duration
}

type WFS struct {
	option                    *Option
	listDirectoryEntriesCache *ccache.Cache

	// contains all open handles
	handles           []*FileHandle
	pathToHandleIndex map[string]int
	pathToHandleLock  sync.Mutex

	// cache grpc connections
	grpcClients     map[string]*grpc.ClientConn
	grpcClientsLock sync.Mutex
}

func NewSeaweedFileSystem(option *Option) *WFS {
	return &WFS{
		option:                    option,
		listDirectoryEntriesCache: ccache.New(ccache.Configure().MaxSize(int64(option.DirListingLimit) + 200).ItemsToPrune(100)),
		pathToHandleIndex:         make(map[string]int),
		grpcClients:               make(map[string]*grpc.ClientConn),
	}
}

func (wfs *WFS) Root() (fs.Node, error) {
	return &Dir{Path: wfs.option.FilerMountRootPath, wfs: wfs}, nil
}

func (wfs *WFS) withFilerClient(fn func(filer_pb.SeaweedFilerClient) error) error {

	wfs.grpcClientsLock.Lock()

	existingConnection, found := wfs.grpcClients[wfs.option.FilerGrpcAddress]
	if found {
		wfs.grpcClientsLock.Unlock()
		client := filer_pb.NewSeaweedFilerClient(existingConnection)
		return fn(client)
	}

	grpcConnection, err := util.GrpcDial(wfs.option.FilerGrpcAddress)
	if err != nil {
		wfs.grpcClientsLock.Unlock()
		return fmt.Errorf("fail to dial %s: %v", wfs.option.FilerGrpcAddress, err)
	}

	wfs.grpcClients[wfs.option.FilerGrpcAddress] = grpcConnection
	wfs.grpcClientsLock.Unlock()

	client := filer_pb.NewSeaweedFilerClient(grpcConnection)

	return fn(client)
}

func (wfs *WFS) AcquireHandle(file *File, uid, gid uint32) (fileHandle *FileHandle) {
	wfs.pathToHandleLock.Lock()
	defer wfs.pathToHandleLock.Unlock()

	fullpath := file.fullpath()

	index, found := wfs.pathToHandleIndex[fullpath]
	if found && wfs.handles[index] != nil {
		glog.V(4).Infoln(fullpath, "found fileHandle id", index)
		return wfs.handles[index]
	}

	if found && wfs.handles[index] != nil {
		glog.V(4).Infoln(fullpath, "reuse previous fileHandle id", index)
		wfs.handles[index].InitializeToFile(file, uid, gid)
		fileHandle.handle = uint64(index)
		return
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
	glog.V(4).Infoln(fullpath, "new fileHandle id", fileHandle.handle)
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
