package filesys

import (
	"context"
	"fmt"
	"os"
	"path"
	"sync"

	"bazil.org/fuse/fs"
	"bazil.org/fuse"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"time"
)

type Dir struct {
	Path        string
	NodeMap     map[string]fs.Node
	NodeMapLock sync.Mutex
	wfs         *WFS
}

func (dir *Dir) Attr(context context.Context, attr *fuse.Attr) error {
	attr.Mode = os.ModeDir | 0777
	return nil
}

func (dir *Dir) Create(ctx context.Context, req *fuse.CreateRequest,
	resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {

	err := dir.wfs.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.CreateEntryRequest{
			Directory: dir.Path,
			Entry: &filer_pb.Entry{
				Name:        req.Name,
				IsDirectory: req.Mode&os.ModeDir > 0,
				Attributes: &filer_pb.FuseAttributes{
					Mtime:    time.Now().Unix(),
					FileMode: uint32(req.Mode),
					Uid:      req.Uid,
					Gid:      req.Gid,
				},
			},
		}

		glog.V(1).Infof("create: %v", request)
		if _, err := client.CreateEntry(ctx, request); err != nil {
			return fmt.Errorf("create file: %v", err)
		}

		return nil
	})

	if err == nil {
		node := &File{Name: req.Name, dir: dir, wfs: dir.wfs}
		dir.NodeMap[req.Name] = node
		return node, node, nil
	}

	return nil, nil, err
}

func (dir *Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	dir.NodeMapLock.Lock()
	defer dir.NodeMapLock.Unlock()

	err := dir.wfs.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.CreateEntryRequest{
			Directory: dir.Path,
			Entry: &filer_pb.Entry{
				Name:        req.Name,
				IsDirectory: true,
				Attributes: &filer_pb.FuseAttributes{
					Mtime:    time.Now().Unix(),
					FileMode: uint32(req.Mode),
					Uid:      req.Uid,
					Gid:      req.Gid,
				},
			},
		}

		glog.V(1).Infof("mkdir: %v", request)
		if _, err := client.CreateEntry(ctx, request); err != nil {
			glog.V(0).Infof("mkdir %v: %v", request, err)
			return fmt.Errorf("make dir: %v", err)
		}

		return nil
	})

	if err == nil {
		node := &Dir{Path: path.Join(dir.Path, req.Name), wfs: dir.wfs}
		dir.NodeMap[req.Name] = node
		return node, nil
	}

	return nil, err
}

func (dir *Dir) Lookup(ctx context.Context, name string) (node fs.Node, err error) {

	dir.NodeMapLock.Lock()
	defer dir.NodeMapLock.Unlock()

	if dir.NodeMap == nil {
		dir.NodeMap = make(map[string]fs.Node)
	}

	if node, ok := dir.NodeMap[name]; ok {
		return node, nil
	}

	var entry *filer_pb.Entry
	err = dir.wfs.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.LookupDirectoryEntryRequest{
			Directory: dir.Path,
			Name:      name,
		}

		glog.V(1).Infof("lookup directory entry: %v", request)
		resp, err := client.LookupDirectoryEntry(ctx, request)
		if err != nil {
			return err
		}

		entry = resp.Entry

		return nil
	})

	if entry != nil {
		if entry.IsDirectory {
			node = &Dir{Path: path.Join(dir.Path, name), wfs: dir.wfs}
		} else {
			node = &File{Chunks: entry.Chunks, Name: name, dir: dir, wfs: dir.wfs}
		}
		dir.NodeMap[name] = node
		return node, nil
	}

	return nil, fuse.ENOENT
}

func (dir *Dir) ReadDirAll(ctx context.Context) (ret []fuse.Dirent, err error) {

	err = dir.wfs.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.ListEntriesRequest{
			Directory: dir.Path,
		}

		glog.V(1).Infof("read directory: %v", request)
		resp, err := client.ListEntries(ctx, request)
		if err != nil {
			return err
		}

		for _, entry := range resp.Entries {
			if entry.IsDirectory {
				dirent := fuse.Dirent{Name: entry.Name, Type: fuse.DT_Dir}
				ret = append(ret, dirent)
			} else {
				dirent := fuse.Dirent{Name: entry.Name, Type: fuse.DT_File}
				ret = append(ret, dirent)
				dir.wfs.listDirectoryEntriesCache.Set(dir.Path+"/"+entry.Name, entry.Attributes, 3*time.Second)
			}
		}

		return nil
	})

	return ret, err
}

func (dir *Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {

	dir.NodeMapLock.Lock()
	defer dir.NodeMapLock.Unlock()

	return dir.wfs.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.DeleteEntryRequest{
			Directory:   dir.Path,
			Name:        req.Name,
			IsDirectory: req.Dir,
		}

		glog.V(1).Infof("remove directory entry: %v", request)
		_, err := client.DeleteEntry(ctx, request)
		if err != nil {
			return err
		}

		delete(dir.NodeMap, req.Name)

		return nil
	})

}
