package filesys

import (
	"context"
	"fmt"
	"os"
	"path"
	"sync"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"path/filepath"
	"time"
)

type Dir struct {
	Path        string
	NodeMap     map[string]fs.Node
	NodeMapLock sync.Mutex
	wfs         *WFS
}

var _ = fs.Node(&Dir{})
var _ = fs.NodeCreater(&Dir{})
var _ = fs.NodeMkdirer(&Dir{})
var _ = fs.NodeStringLookuper(&Dir{})
var _ = fs.HandleReadDirAller(&Dir{})
var _ = fs.NodeRemover(&Dir{})

func (dir *Dir) Attr(context context.Context, attr *fuse.Attr) error {

	if dir.Path == "/" {
		attr.Valid = time.Second
		attr.Mode = os.ModeDir | 0777
		return nil
	}

	parent, name := filepath.Split(dir.Path)

	var attributes *filer_pb.FuseAttributes

	err := dir.wfs.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.GetEntryAttributesRequest{
			Name:      name,
			ParentDir: parent,
		}

		glog.V(1).Infof("read dir attr: %v", request)
		resp, err := client.GetEntryAttributes(context, request)
		if err != nil {
			glog.V(0).Infof("read dir attr %v: %v", request, err)
			return err
		}

		attributes = resp.Attributes

		return nil
	})

	if err != nil {
		return err
	}

	// glog.V(1).Infof("dir %s: %v", dir.Path, attributes)
	// glog.V(1).Infof("dir %s permission: %v", dir.Path, os.FileMode(attributes.FileMode))

	attr.Mode = os.FileMode(attributes.FileMode) | os.ModeDir
	if dir.Path == "/" && attributes.FileMode == 0 {
		attr.Valid = time.Second
	}

	attr.Mtime = time.Unix(attributes.Mtime, 0)
	attr.Ctime = time.Unix(attributes.Mtime, 0)
	attr.Gid = attributes.Gid
	attr.Uid = attributes.Uid

	return nil
}

func (dir *Dir) newFile(name string, chunks []*filer_pb.FileChunk) *File {
	return &File{
		Name: name,
		dir:  dir,
		wfs:  dir.wfs,
		// attributes: &filer_pb.FuseAttributes{},
		Chunks: chunks,
	}
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
					Crtime:   time.Now().Unix(),
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
		file := dir.newFile(req.Name, nil)
		dir.NodeMap[req.Name] = file
		file.isOpen = true
		return file, dir.wfs.AcquireHandle(file, req.Uid, req.Gid), nil
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
					Crtime:   time.Now().Unix(),
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

		glog.V(4).Infof("lookup directory entry: %v", request)
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
			node = dir.newFile(name, entry.Chunks)
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

		glog.V(4).Infof("read directory: %v", request)
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
				dir.wfs.listDirectoryEntriesCache.Set(dir.Path+"/"+entry.Name, entry, 300*time.Millisecond)
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
