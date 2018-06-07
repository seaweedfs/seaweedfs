package filesys

import (
	"context"
	"os"
	"path"
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"path/filepath"
	"time"
)

type Dir struct {
	Path string
	wfs  *WFS
}

var _ = fs.Node(&Dir{})
var _ = fs.NodeCreater(&Dir{})
var _ = fs.NodeMkdirer(&Dir{})
var _ = fs.NodeStringLookuper(&Dir{})
var _ = fs.HandleReadDirAller(&Dir{})
var _ = fs.NodeRemover(&Dir{})
var _ = fs.NodeRenamer(&Dir{})

func (dir *Dir) Attr(context context.Context, attr *fuse.Attr) error {

	if dir.Path == "/" {
		attr.Valid = time.Second
		attr.Mode = os.ModeDir | 0777
		return nil
	}

	item := dir.wfs.listDirectoryEntriesCache.Get(dir.Path)
	if item != nil && !item.Expired() {
		entry := item.Value().(*filer_pb.Entry)

		attr.Mtime = time.Unix(entry.Attributes.Mtime, 0)
		attr.Ctime = time.Unix(entry.Attributes.Crtime, 0)
		attr.Mode = os.FileMode(entry.Attributes.FileMode)
		attr.Gid = entry.Attributes.Gid
		attr.Uid = entry.Attributes.Uid

		return nil
	}

	parent, name := filepath.Split(dir.Path)

	var attributes *filer_pb.FuseAttributes

	err := dir.wfs.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.GetEntryAttributesRequest{
			Name:      name,
			ParentDir: parent,
		}

		glog.V(1).Infof("read dir %s attr: %v", dir.Path, request)
		resp, err := client.GetEntryAttributes(context, request)
		if err != nil {
			glog.V(0).Infof("read dir %s attr %v: %v", dir.Path, request, err)
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
	attr.Ctime = time.Unix(attributes.Crtime, 0)
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
			glog.V(0).Infof("create %s/%s: %v", dir.Path, req.Name, err)
			return fuse.EIO
		}

		return nil
	})

	if err == nil {
		file := dir.newFile(req.Name, nil)
		file.isOpen = true
		return file, dir.wfs.AcquireHandle(file, req.Uid, req.Gid), nil
	}

	return nil, nil, err
}

func (dir *Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {

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
			glog.V(0).Infof("mkdir %s/%s: %v", dir.Path, req.Name, err)
			return fuse.EIO
		}

		return nil
	})

	if err == nil {
		node := &Dir{Path: path.Join(dir.Path, req.Name), wfs: dir.wfs}
		return node, nil
	}

	return nil, err
}

func (dir *Dir) Lookup(ctx context.Context, name string) (node fs.Node, err error) {

	var entry *filer_pb.Entry
	err = dir.wfs.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.LookupDirectoryEntryRequest{
			Directory: dir.Path,
			Name:      name,
		}

		glog.V(4).Infof("lookup directory entry: %v", request)
		resp, err := client.LookupDirectoryEntry(ctx, request)
		if err != nil {
			// glog.V(0).Infof("lookup %s/%s: %v", dir.Path, name, err)
			return fuse.ENOENT
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
			glog.V(0).Infof("list %s: %v", dir.Path, err)
			return fuse.EIO
		}

		for _, entry := range resp.Entries {
			if entry.IsDirectory {
				dirent := fuse.Dirent{Name: entry.Name, Type: fuse.DT_Dir}
				ret = append(ret, dirent)
			} else {
				dirent := fuse.Dirent{Name: entry.Name, Type: fuse.DT_File}
				ret = append(ret, dirent)
			}
			dir.wfs.listDirectoryEntriesCache.Set(dir.Path+"/"+entry.Name, entry, 300*time.Millisecond)
		}

		return nil
	})

	return ret, err
}

func (dir *Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {

	return dir.wfs.withFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.DeleteEntryRequest{
			Directory:    dir.Path,
			Name:         req.Name,
			IsDirectory:  req.Dir,
			IsDeleteData: true,
		}

		glog.V(1).Infof("remove directory entry: %v", request)
		_, err := client.DeleteEntry(ctx, request)
		if err != nil {
			glog.V(0).Infof("remove %s/%s: %v", dir.Path, req.Name, err)
			return fuse.EIO
		}

		return nil
	})

}
