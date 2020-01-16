package filesys

import (
	"context"
	"os"
	"path"
	"time"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/fuse"
	"github.com/seaweedfs/fuse/fs"
)

type Dir struct {
	Path  string
	wfs   *WFS
	entry *filer_pb.Entry
}

var _ = fs.Node(&Dir{})
var _ = fs.NodeCreater(&Dir{})
var _ = fs.NodeMkdirer(&Dir{})
var _ = fs.NodeRequestLookuper(&Dir{})
var _ = fs.HandleReadDirAller(&Dir{})
var _ = fs.NodeRemover(&Dir{})
var _ = fs.NodeRenamer(&Dir{})
var _ = fs.NodeSetattrer(&Dir{})
var _ = fs.NodeGetxattrer(&Dir{})
var _ = fs.NodeSetxattrer(&Dir{})
var _ = fs.NodeRemovexattrer(&Dir{})
var _ = fs.NodeListxattrer(&Dir{})

func (dir *Dir) Attr(ctx context.Context, attr *fuse.Attr) error {

	glog.V(3).Infof("dir Attr %s", dir.Path)

	// https://github.com/bazil/fuse/issues/196
	attr.Valid = time.Second

	if dir.Path == dir.wfs.option.FilerMountRootPath {
		dir.setRootDirAttributes(attr)
		return nil
	}

	if err := dir.maybeLoadEntry(ctx); err != nil {
		return err
	}

	attr.Mode = os.FileMode(dir.entry.Attributes.FileMode) | os.ModeDir
	attr.Mtime = time.Unix(dir.entry.Attributes.Mtime, 0)
	attr.Ctime = time.Unix(dir.entry.Attributes.Crtime, 0)
	attr.Gid = dir.entry.Attributes.Gid
	attr.Uid = dir.entry.Attributes.Uid

	return nil
}

func (dir *Dir) Getxattr(ctx context.Context, req *fuse.GetxattrRequest, resp *fuse.GetxattrResponse) error {

	glog.V(4).Infof("dir Getxattr %s", dir.Path)

	if err := dir.maybeLoadEntry(ctx); err != nil {
		return err
	}

	return getxattr(dir.entry, req, resp)
}

func (dir *Dir) setRootDirAttributes(attr *fuse.Attr) {
	attr.Uid = dir.wfs.option.MountUid
	attr.Gid = dir.wfs.option.MountGid
	attr.Mode = dir.wfs.option.MountMode
	attr.Crtime = dir.wfs.option.MountCtime
	attr.Ctime = dir.wfs.option.MountCtime
	attr.Mtime = dir.wfs.option.MountMtime
	attr.Atime = dir.wfs.option.MountMtime
}

func (dir *Dir) newFile(name string, entry *filer_pb.Entry) *File {
	return &File{
		Name:           name,
		dir:            dir,
		wfs:            dir.wfs,
		entry:          entry,
		entryViewCache: nil,
	}
}

func (dir *Dir) Create(ctx context.Context, req *fuse.CreateRequest,
	resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {

	request := &filer_pb.CreateEntryRequest{
		Directory: dir.Path,
		Entry: &filer_pb.Entry{
			Name:        req.Name,
			IsDirectory: req.Mode&os.ModeDir > 0,
			Attributes: &filer_pb.FuseAttributes{
				Mtime:       time.Now().Unix(),
				Crtime:      time.Now().Unix(),
				FileMode:    uint32(req.Mode &^ dir.wfs.option.Umask),
				Uid:         req.Uid,
				Gid:         req.Gid,
				Collection:  dir.wfs.option.Collection,
				Replication: dir.wfs.option.Replication,
				TtlSec:      dir.wfs.option.TtlSec,
			},
		},
	}
	glog.V(1).Infof("create: %v", request)

	if request.Entry.IsDirectory {
		if err := dir.wfs.WithFilerClient(ctx, func(client filer_pb.SeaweedFilerClient) error {
			if _, err := client.CreateEntry(ctx, request); err != nil {
				glog.V(0).Infof("create %s/%s: %v", dir.Path, req.Name, err)
				return fuse.EIO
			}
			return nil
		}); err != nil {
			return nil, nil, err
		}
	}

	file := dir.newFile(req.Name, request.Entry)
	if !request.Entry.IsDirectory {
		file.isOpen = true
	}
	fh := dir.wfs.AcquireHandle(file, req.Uid, req.Gid)
	fh.dirtyMetadata = true
	return file, fh, nil

}

func (dir *Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {

	err := dir.wfs.WithFilerClient(ctx, func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.CreateEntryRequest{
			Directory: dir.Path,
			Entry: &filer_pb.Entry{
				Name:        req.Name,
				IsDirectory: true,
				Attributes: &filer_pb.FuseAttributes{
					Mtime:    time.Now().Unix(),
					Crtime:   time.Now().Unix(),
					FileMode: uint32(req.Mode &^ dir.wfs.option.Umask),
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

func (dir *Dir) Lookup(ctx context.Context, req *fuse.LookupRequest, resp *fuse.LookupResponse) (node fs.Node, err error) {

	glog.V(4).Infof("dir Lookup %s: %s", dir.Path, req.Name)

	var entry *filer_pb.Entry
	fullFilePath := path.Join(dir.Path, req.Name)

	item := dir.wfs.listDirectoryEntriesCache.Get(fullFilePath)
	if item != nil && !item.Expired() {
		entry = item.Value().(*filer_pb.Entry)
	}

	if entry == nil {
		glog.V(3).Infof("dir Lookup cache miss %s", fullFilePath)
		entry, err = filer2.GetEntry(ctx, dir.wfs, fullFilePath)
		if err != nil {
			glog.V(1).Infof("dir GetEntry %s: %v", fullFilePath, err)
			return nil, fuse.ENOENT
		}
		if entry != nil {
			dir.wfs.listDirectoryEntriesCache.Set(fullFilePath, entry, 5*time.Minute)
		}
	} else {
		glog.V(4).Infof("dir Lookup cache hit %s", fullFilePath)
	}

	if entry != nil {
		if entry.IsDirectory {
			node = &Dir{Path: path.Join(dir.Path, req.Name), wfs: dir.wfs, entry: entry}
		} else {
			node = dir.newFile(req.Name, entry)
		}

		resp.EntryValid = time.Duration(0)
		resp.Attr.Mtime = time.Unix(entry.Attributes.Mtime, 0)
		resp.Attr.Ctime = time.Unix(entry.Attributes.Crtime, 0)
		resp.Attr.Mode = os.FileMode(entry.Attributes.FileMode)
		resp.Attr.Gid = entry.Attributes.Gid
		resp.Attr.Uid = entry.Attributes.Uid

		return node, nil
	}

	return nil, fuse.ENOENT
}

func (dir *Dir) ReadDirAll(ctx context.Context) (ret []fuse.Dirent, err error) {

	glog.V(3).Infof("dir ReadDirAll %s", dir.Path)

	cacheTtl := 5 * time.Minute

	readErr := filer2.ReadDirAllEntries(ctx, dir.wfs, dir.Path, "", func(entry *filer_pb.Entry, isLast bool) {
		if entry.IsDirectory {
			dirent := fuse.Dirent{Name: entry.Name, Type: fuse.DT_Dir}
			ret = append(ret, dirent)
		} else {
			dirent := fuse.Dirent{Name: entry.Name, Type: fuse.DT_File}
			ret = append(ret, dirent)
		}
		dir.wfs.listDirectoryEntriesCache.Set(path.Join(dir.Path, entry.Name), entry, cacheTtl)
	})
	if readErr != nil {
		glog.V(0).Infof("list %s: %v", dir.Path, err)
		return ret, fuse.EIO
	}

	return ret, err
}

func (dir *Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {

	if !req.Dir {
		return dir.removeOneFile(ctx, req)
	}

	return dir.removeFolder(ctx, req)

}

func (dir *Dir) removeOneFile(ctx context.Context, req *fuse.RemoveRequest) error {

	entry, err := filer2.GetEntry(ctx, dir.wfs, path.Join(dir.Path, req.Name))
	if err != nil {
		return err
	}

	dir.wfs.deleteFileChunks(ctx, entry.Chunks)

	dir.wfs.listDirectoryEntriesCache.Delete(path.Join(dir.Path, req.Name))

	return dir.wfs.WithFilerClient(ctx, func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.DeleteEntryRequest{
			Directory:    dir.Path,
			Name:         req.Name,
			IsDeleteData: false,
		}

		glog.V(3).Infof("remove file: %v", request)
		_, err := client.DeleteEntry(ctx, request)
		if err != nil {
			glog.V(3).Infof("remove file %s/%s: %v", dir.Path, req.Name, err)
			return fuse.ENOENT
		}

		return nil
	})

}

func (dir *Dir) removeFolder(ctx context.Context, req *fuse.RemoveRequest) error {

	dir.wfs.listDirectoryEntriesCache.Delete(path.Join(dir.Path, req.Name))

	return dir.wfs.WithFilerClient(ctx, func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.DeleteEntryRequest{
			Directory:    dir.Path,
			Name:         req.Name,
			IsDeleteData: true,
		}

		glog.V(3).Infof("remove directory entry: %v", request)
		_, err := client.DeleteEntry(ctx, request)
		if err != nil {
			glog.V(3).Infof("remove %s/%s: %v", dir.Path, req.Name, err)
			return fuse.ENOENT
		}

		return nil
	})

}

func (dir *Dir) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {

	if err := dir.maybeLoadEntry(ctx); err != nil {
		return err
	}

	glog.V(3).Infof("%v dir setattr %+v, fh=%d", dir.Path, req, req.Handle)
	if req.Valid.Mode() {
		dir.entry.Attributes.FileMode = uint32(req.Mode)
	}

	if req.Valid.Uid() {
		dir.entry.Attributes.Uid = req.Uid
	}

	if req.Valid.Gid() {
		dir.entry.Attributes.Gid = req.Gid
	}

	if req.Valid.Mtime() {
		dir.entry.Attributes.Mtime = req.Mtime.Unix()
	}

	dir.wfs.listDirectoryEntriesCache.Delete(dir.Path)

	return dir.saveEntry(ctx)

}

func (dir *Dir) Setxattr(ctx context.Context, req *fuse.SetxattrRequest) error {

	glog.V(4).Infof("dir Setxattr %s: %s", dir.Path, req.Name)

	if err := dir.maybeLoadEntry(ctx); err != nil {
		return err
	}

	if err := setxattr(dir.entry, req); err != nil {
		return err
	}

	dir.wfs.listDirectoryEntriesCache.Delete(dir.Path)

	return dir.saveEntry(ctx)

}

func (dir *Dir) Removexattr(ctx context.Context, req *fuse.RemovexattrRequest) error {

	glog.V(4).Infof("dir Removexattr %s: %s", dir.Path, req.Name)

	if err := dir.maybeLoadEntry(ctx); err != nil {
		return err
	}

	if err := removexattr(dir.entry, req); err != nil {
		return err
	}

	dir.wfs.listDirectoryEntriesCache.Delete(dir.Path)

	return dir.saveEntry(ctx)

}

func (dir *Dir) Listxattr(ctx context.Context, req *fuse.ListxattrRequest, resp *fuse.ListxattrResponse) error {

	glog.V(4).Infof("dir Listxattr %s", dir.Path)

	if err := dir.maybeLoadEntry(ctx); err != nil {
		return err
	}

	if err := listxattr(dir.entry, req, resp); err != nil {
		return err
	}

	return nil

}

func (dir *Dir) maybeLoadEntry(ctx context.Context) error {
	if dir.entry == nil {
		parentDirPath, name := filer2.FullPath(dir.Path).DirAndName()
		entry, err := dir.wfs.maybeLoadEntry(ctx, parentDirPath, name)
		if err != nil {
			return err
		}
		dir.entry = entry
	}
	return nil
}

func (dir *Dir) saveEntry(ctx context.Context) error {

	parentDir, name := filer2.FullPath(dir.Path).DirAndName()

	return dir.wfs.WithFilerClient(ctx, func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.UpdateEntryRequest{
			Directory: parentDir,
			Entry:     dir.entry,
		}

		glog.V(1).Infof("save dir entry: %v", request)
		_, err := client.UpdateEntry(ctx, request)
		if err != nil {
			glog.V(0).Infof("UpdateEntry dir %s/%s: %v", parentDir, name, err)
			return fuse.EIO
		}

		return nil
	})
}
