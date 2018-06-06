package filesys

import (
	"bazil.org/fuse/fs"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/karlseguin/ccache"
	"google.golang.org/grpc"
	"sync"
	"bazil.org/fuse"
	"github.com/chrislusf/seaweedfs/weed/glog"
)

type WFS struct {
	filerGrpcAddress          string
	listDirectoryEntriesCache *ccache.Cache
	collection                string
	replication               string
	chunkSizeLimit            int64

	// contains all open handles
	handles           []*FileHandle
	pathToHandleIndex map[string]int
	pathToHandleLock  sync.Mutex
}

func NewSeaweedFileSystem(filerGrpcAddress string, collection string, replication string, chunkSizeLimitMB int) *WFS {
	return &WFS{
		filerGrpcAddress:          filerGrpcAddress,
		listDirectoryEntriesCache: ccache.New(ccache.Configure().MaxSize(6000).ItemsToPrune(100)),
		collection:                collection,
		replication:               replication,
		chunkSizeLimit:            int64(chunkSizeLimitMB) * 1024 * 1024,
		pathToHandleIndex:         make(map[string]int),
	}
}

func (wfs *WFS) Root() (fs.Node, error) {
	return &Dir{Path: "/", wfs: wfs}, nil
}

func (wfs *WFS) withFilerClient(fn func(filer_pb.SeaweedFilerClient) error) error {

	grpcConnection, err := grpc.Dial(wfs.filerGrpcAddress, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("fail to dial %s: %v", wfs.filerGrpcAddress, err)
	}
	defer grpcConnection.Close()

	client := filer_pb.NewSeaweedFilerClient(grpcConnection)

	return fn(client)
}

func (wfs *WFS) AcquireHandle(file *File, uid, gid uint32) (handle *FileHandle) {
	wfs.pathToHandleLock.Lock()
	defer wfs.pathToHandleLock.Unlock()

	fullpath := file.fullpath()

	index, found := wfs.pathToHandleIndex[fullpath]
	if found && wfs.handles[index] != nil {
		glog.V(4).Infoln(fullpath, "found handle id", index)
		return wfs.handles[index]
	}

	// create a new handler
	handle = &FileHandle{
		f:          file,
		dirtyPages: newDirtyPages(file),
		Uid:        uid,
		Gid:        gid,
	}

	if found && wfs.handles[index] != nil {
		glog.V(4).Infoln(fullpath, "reuse previous handle id", index)
		wfs.handles[index] = handle
		handle.handle = uint64(index)
		return
	}

	for i, h := range wfs.handles {
		if h == nil {
			wfs.handles[i] = handle
			handle.handle = uint64(i)
			wfs.pathToHandleIndex[fullpath] = i
			glog.V(4).Infoln(fullpath, "reuse handle id", handle.handle)
			return
		}
	}

	wfs.handles = append(wfs.handles, handle)
	handle.handle = uint64(len(wfs.handles) - 1)
	glog.V(4).Infoln(fullpath, "new handle id", handle.handle)
	wfs.pathToHandleIndex[fullpath] = int(handle.handle)

	return
}

func (wfs *WFS) ReleaseHandle(handleId fuse.HandleID) {
	wfs.pathToHandleLock.Lock()
	defer wfs.pathToHandleLock.Unlock()

	glog.V(4).Infoln("releasing handle id", handleId, "current handles lengh", len(wfs.handles))
	if int(handleId) < len(wfs.handles) {
		wfs.handles[int(handleId)] = nil
	}

	return
}
