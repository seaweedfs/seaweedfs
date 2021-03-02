package filesys

import (
	"bytes"
	"context"
	"math"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/seaweedfs/fuse"
	"github.com/seaweedfs/fuse/fs"

	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/filesys/meta_cache"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
)

type Dir struct {
	name   string
	wfs    *WFS
	entry  *filer_pb.Entry
	parent *Dir
}

var _ = fs.Node(&Dir{})
var _ = fs.NodeCreater(&Dir{})
var _ = fs.NodeMknoder(&Dir{})
var _ = fs.NodeMkdirer(&Dir{})
var _ = fs.NodeFsyncer(&Dir{})
var _ = fs.NodeRequestLookuper(&Dir{})
var _ = fs.HandleReadDirAller(&Dir{})
var _ = fs.NodeRemover(&Dir{})
var _ = fs.NodeRenamer(&Dir{})
var _ = fs.NodeSetattrer(&Dir{})
var _ = fs.NodeGetxattrer(&Dir{})
var _ = fs.NodeSetxattrer(&Dir{})
var _ = fs.NodeRemovexattrer(&Dir{})
var _ = fs.NodeListxattrer(&Dir{})
var _ = fs.NodeForgetter(&Dir{})

func (dir *Dir) Attr(ctx context.Context, attr *fuse.Attr) error {

	// https://github.com/bazil/fuse/issues/196
	attr.Valid = time.Second

	if dir.FullPath() == dir.wfs.option.FilerMountRootPath {
		dir.setRootDirAttributes(attr)
		glog.V(3).Infof("root dir Attr %s, attr: %+v", dir.FullPath(), attr)
		return nil
	}

	if err := dir.maybeLoadEntry(); err != nil {
		glog.V(3).Infof("dir Attr %s,err: %+v", dir.FullPath(), err)
		return err
	}

	// attr.Inode = util.FullPath(dir.FullPath()).AsInode()
	attr.Mode = os.FileMode(dir.entry.Attributes.FileMode) | os.ModeDir
	attr.Mtime = time.Unix(dir.entry.Attributes.Mtime, 0)
	attr.Crtime = time.Unix(dir.entry.Attributes.Crtime, 0)
	attr.Gid = dir.entry.Attributes.Gid
	attr.Uid = dir.entry.Attributes.Uid

	glog.V(4).Infof("dir Attr %s, attr: %+v", dir.FullPath(), attr)

	return nil
}

func (dir *Dir) Getxattr(ctx context.Context, req *fuse.GetxattrRequest, resp *fuse.GetxattrResponse) error {

	glog.V(4).Infof("dir Getxattr %s", dir.FullPath())

	if err := dir.maybeLoadEntry(); err != nil {
		return err
	}

	return getxattr(dir.entry, req, resp)
}

func (dir *Dir) setRootDirAttributes(attr *fuse.Attr) {
	// attr.Inode = 1 // filer2.FullPath(dir.Path).AsInode()
	attr.Valid = time.Second
	attr.Uid = dir.wfs.option.MountUid
	attr.Gid = dir.wfs.option.MountGid
	attr.Mode = dir.wfs.option.MountMode
	attr.Crtime = dir.wfs.option.MountCtime
	attr.Ctime = dir.wfs.option.MountCtime
	attr.Mtime = dir.wfs.option.MountMtime
	attr.Atime = dir.wfs.option.MountMtime
	attr.BlockSize = blockSize
}

func (dir *Dir) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	// fsync works at OS level
	// write the file chunks to the filerGrpcAddress
	glog.V(3).Infof("dir %s fsync %+v", dir.FullPath(), req)

	return nil
}

func (dir *Dir) newFile(name string, entry *filer_pb.Entry) fs.Node {
	f := dir.wfs.fsNodeCache.EnsureFsNode(util.NewFullPath(dir.FullPath(), name), func() fs.Node {
		return &File{
			Name:           name,
			dir:            dir,
			wfs:            dir.wfs,
			entry:          entry,
			entryViewCache: nil,
		}
	})
	f.(*File).dir = dir // in case dir node was created later
	return f
}

func (dir *Dir) newDirectory(fullpath util.FullPath, entry *filer_pb.Entry) fs.Node {

	d := dir.wfs.fsNodeCache.EnsureFsNode(fullpath, func() fs.Node {
		return &Dir{name: entry.Name, wfs: dir.wfs, entry: entry, parent: dir}
	})
	d.(*Dir).parent = dir // in case dir node was created later
	return d
}

func (dir *Dir) Create(ctx context.Context, req *fuse.CreateRequest,
	resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {

	request, err := dir.doCreateEntry(req.Name, req.Mode, req.Uid, req.Gid, req.Flags&fuse.OpenExclusive != 0)

	if err != nil {
		return nil, nil, err
	}
	var node fs.Node
	if request.Entry.IsDirectory {
		node = dir.newDirectory(util.NewFullPath(dir.FullPath(), req.Name), request.Entry)
		return node, nil, nil
	}

	node = dir.newFile(req.Name, request.Entry)
	file := node.(*File)
	fh := dir.wfs.AcquireHandle(file, req.Uid, req.Gid)
	return file, fh, nil

}

func (dir *Dir) Mknod(ctx context.Context, req *fuse.MknodRequest) (fs.Node, error) {

	request, err := dir.doCreateEntry(req.Name, req.Mode, req.Uid, req.Gid, false)

	if err != nil {
		return nil, err
	}
	var node fs.Node
	node = dir.newFile(req.Name, request.Entry)
	return node, nil
}

func (dir *Dir) doCreateEntry(name string, mode os.FileMode, uid, gid uint32, exlusive bool) (*filer_pb.CreateEntryRequest, error) {
	request := &filer_pb.CreateEntryRequest{
		Directory: dir.FullPath(),
		Entry: &filer_pb.Entry{
			Name:        name,
			IsDirectory: mode&os.ModeDir > 0,
			Attributes: &filer_pb.FuseAttributes{
				Mtime:       time.Now().Unix(),
				Crtime:      time.Now().Unix(),
				FileMode:    uint32(mode &^ dir.wfs.option.Umask),
				Uid:         uid,
				Gid:         gid,
				Collection:  dir.wfs.option.Collection,
				Replication: dir.wfs.option.Replication,
				TtlSec:      dir.wfs.option.TtlSec,
			},
		},
		OExcl:      exlusive,
		Signatures: []int32{dir.wfs.signature},
	}
	glog.V(1).Infof("create %s/%s", dir.FullPath(), name)

	err := dir.wfs.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		dir.wfs.mapPbIdFromLocalToFiler(request.Entry)
		defer dir.wfs.mapPbIdFromFilerToLocal(request.Entry)

		if err := filer_pb.CreateEntry(client, request); err != nil {
			if strings.Contains(err.Error(), "EEXIST") {
				return fuse.EEXIST
			}
			glog.V(0).Infof("create %s/%s: %v", dir.FullPath(), name, err)
			return fuse.EIO
		}

		dir.wfs.metaCache.InsertEntry(context.Background(), filer.FromPbEntry(request.Directory, request.Entry))

		return nil
	})
	return request, err
}

func (dir *Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {

	glog.V(4).Infof("mkdir %s: %s", dir.FullPath(), req.Name)

	newEntry := &filer_pb.Entry{
		Name:        req.Name,
		IsDirectory: true,
		Attributes: &filer_pb.FuseAttributes{
			Mtime:    time.Now().Unix(),
			Crtime:   time.Now().Unix(),
			FileMode: uint32(req.Mode &^ dir.wfs.option.Umask),
			Uid:      req.Uid,
			Gid:      req.Gid,
		},
	}

	err := dir.wfs.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		dir.wfs.mapPbIdFromLocalToFiler(newEntry)
		defer dir.wfs.mapPbIdFromFilerToLocal(newEntry)

		request := &filer_pb.CreateEntryRequest{
			Directory:  dir.FullPath(),
			Entry:      newEntry,
			Signatures: []int32{dir.wfs.signature},
		}

		glog.V(1).Infof("mkdir: %v", request)
		if err := filer_pb.CreateEntry(client, request); err != nil {
			glog.V(0).Infof("mkdir %s/%s: %v", dir.FullPath(), req.Name, err)
			return err
		}

		dir.wfs.metaCache.InsertEntry(context.Background(), filer.FromPbEntry(request.Directory, request.Entry))

		return nil
	})

	if err == nil {
		node := dir.newDirectory(util.NewFullPath(dir.FullPath(), req.Name), newEntry)

		return node, nil
	}

	glog.V(0).Infof("mkdir %s/%s: %v", dir.FullPath(), req.Name, err)

	return nil, fuse.EIO
}

func (dir *Dir) Lookup(ctx context.Context, req *fuse.LookupRequest, resp *fuse.LookupResponse) (node fs.Node, err error) {

	glog.V(4).Infof("dir Lookup %s: %s by %s", dir.FullPath(), req.Name, req.Header.String())

	fullFilePath := util.NewFullPath(dir.FullPath(), req.Name)
	dirPath := util.FullPath(dir.FullPath())
	visitErr := meta_cache.EnsureVisited(dir.wfs.metaCache, dir.wfs, dirPath)
	if visitErr != nil {
		glog.Errorf("dir Lookup %s: %v", dirPath, visitErr)
		return nil, fuse.EIO
	}
	cachedEntry, cacheErr := dir.wfs.metaCache.FindEntry(context.Background(), fullFilePath)
	if cacheErr == filer_pb.ErrNotFound {
		return nil, fuse.ENOENT
	}
	entry := cachedEntry.ToProtoEntry()

	if entry == nil {
		// glog.V(3).Infof("dir Lookup cache miss %s", fullFilePath)
		entry, err = filer_pb.GetEntry(dir.wfs, fullFilePath)
		if err != nil {
			glog.V(1).Infof("dir GetEntry %s: %v", fullFilePath, err)
			return nil, fuse.ENOENT
		}
	} else {
		glog.V(4).Infof("dir Lookup cache hit %s", fullFilePath)
	}

	if entry != nil {
		if entry.IsDirectory {
			node = dir.newDirectory(fullFilePath, entry)
		} else {
			node = dir.newFile(req.Name, entry)
		}

		// resp.EntryValid = time.Second
		// resp.Attr.Inode = fullFilePath.AsInode()
		resp.Attr.Valid = time.Second
		resp.Attr.Mtime = time.Unix(entry.Attributes.Mtime, 0)
		resp.Attr.Crtime = time.Unix(entry.Attributes.Crtime, 0)
		resp.Attr.Mode = os.FileMode(entry.Attributes.FileMode)
		resp.Attr.Gid = entry.Attributes.Gid
		resp.Attr.Uid = entry.Attributes.Uid
		if entry.HardLinkCounter > 0 {
			resp.Attr.Nlink = uint32(entry.HardLinkCounter)
		}

		return node, nil
	}

	glog.V(4).Infof("not found dir GetEntry %s: %v", fullFilePath, err)
	return nil, fuse.ENOENT
}

func (dir *Dir) ReadDirAll(ctx context.Context) (ret []fuse.Dirent, err error) {

	glog.V(4).Infof("dir ReadDirAll %s", dir.FullPath())

	processEachEntryFn := func(entry *filer_pb.Entry, isLast bool) error {
		if entry.IsDirectory {
			dirent := fuse.Dirent{Name: entry.Name, Type: fuse.DT_Dir}
			ret = append(ret, dirent)
		} else {
			dirent := fuse.Dirent{Name: entry.Name, Type: findFileType(uint16(entry.Attributes.FileMode))}
			ret = append(ret, dirent)
		}
		return nil
	}

	dirPath := util.FullPath(dir.FullPath())
	if err = meta_cache.EnsureVisited(dir.wfs.metaCache, dir.wfs, dirPath); err != nil {
		glog.Errorf("dir ReadDirAll %s: %v", dirPath, err)
		return nil, fuse.EIO
	}
	listErr := dir.wfs.metaCache.ListDirectoryEntries(context.Background(), util.FullPath(dir.FullPath()), "", false, int64(math.MaxInt32), func(entry *filer.Entry) bool {
		processEachEntryFn(entry.ToProtoEntry(), false)
		return true
	})
	if listErr != nil {
		glog.Errorf("list meta cache: %v", listErr)
		return nil, fuse.EIO
	}
	return
}

func findFileType(mode uint16) fuse.DirentType {
	switch mode & (syscall.S_IFMT & 0xffff) {
	case syscall.S_IFSOCK:
		return fuse.DT_Socket
	case syscall.S_IFLNK:
		return fuse.DT_Link
	case syscall.S_IFREG:
		return fuse.DT_File
	case syscall.S_IFBLK:
		return fuse.DT_Block
	case syscall.S_IFDIR:
		return fuse.DT_Dir
	case syscall.S_IFCHR:
		return fuse.DT_Char
	case syscall.S_IFIFO:
		return fuse.DT_FIFO
	}
	return fuse.DT_File
}

func (dir *Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {

	if !req.Dir {
		return dir.removeOneFile(req)
	}

	return dir.removeFolder(req)

}

func (dir *Dir) removeOneFile(req *fuse.RemoveRequest) error {

	filePath := util.NewFullPath(dir.FullPath(), req.Name)
	entry, err := filer_pb.GetEntry(dir.wfs, filePath)
	if err != nil {
		return err
	}
	if entry == nil {
		return nil
	}

	// first, ensure the filer store can correctly delete
	glog.V(3).Infof("remove file: %v", req)
	isDeleteData := entry.HardLinkCounter <= 1
	err = filer_pb.Remove(dir.wfs, dir.FullPath(), req.Name, isDeleteData, false, false, false, []int32{dir.wfs.signature})
	if err != nil {
		glog.V(3).Infof("not found remove file %s/%s: %v", dir.FullPath(), req.Name, err)
		return fuse.ENOENT
	}

	// then, delete meta cache and fsNode cache
	dir.wfs.metaCache.DeleteEntry(context.Background(), filePath)

	// clear entry inside the file
	fsNode := dir.wfs.fsNodeCache.GetFsNode(filePath)
	dir.wfs.fsNodeCache.DeleteFsNode(filePath)
	if fsNode != nil {
		if file, ok := fsNode.(*File); ok {
			file.clearEntry()
		}
	}

	// remove current file handle if any
	dir.wfs.handlesLock.Lock()
	defer dir.wfs.handlesLock.Unlock()
	inodeId := util.NewFullPath(dir.FullPath(), req.Name).AsInode()
	delete(dir.wfs.handles, inodeId)

	return nil

}

func (dir *Dir) removeFolder(req *fuse.RemoveRequest) error {

	glog.V(3).Infof("remove directory entry: %v", req)
	ignoreRecursiveErr := true // ignore recursion error since the OS should manage it
	err := filer_pb.Remove(dir.wfs, dir.FullPath(), req.Name, true, false, ignoreRecursiveErr, false, []int32{dir.wfs.signature})
	if err != nil {
		glog.V(0).Infof("remove %s/%s: %v", dir.FullPath(), req.Name, err)
		if strings.Contains(err.Error(), "non-empty") {
			return fuse.EEXIST
		}
		return fuse.ENOENT
	}

	t := util.NewFullPath(dir.FullPath(), req.Name)
	dir.wfs.metaCache.DeleteEntry(context.Background(), t)
	dir.wfs.fsNodeCache.DeleteFsNode(t)

	return nil

}

func (dir *Dir) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {

	glog.V(4).Infof("%v dir setattr %+v", dir.FullPath(), req)

	if err := dir.maybeLoadEntry(); err != nil {
		return err
	}

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

	return dir.saveEntry()

}

func (dir *Dir) Setxattr(ctx context.Context, req *fuse.SetxattrRequest) error {

	glog.V(4).Infof("dir Setxattr %s: %s", dir.FullPath(), req.Name)

	if err := dir.maybeLoadEntry(); err != nil {
		return err
	}

	if err := setxattr(dir.entry, req); err != nil {
		return err
	}

	return dir.saveEntry()

}

func (dir *Dir) Removexattr(ctx context.Context, req *fuse.RemovexattrRequest) error {

	glog.V(4).Infof("dir Removexattr %s: %s", dir.FullPath(), req.Name)

	if err := dir.maybeLoadEntry(); err != nil {
		return err
	}

	if err := removexattr(dir.entry, req); err != nil {
		return err
	}

	return dir.saveEntry()

}

func (dir *Dir) Listxattr(ctx context.Context, req *fuse.ListxattrRequest, resp *fuse.ListxattrResponse) error {

	glog.V(4).Infof("dir Listxattr %s", dir.FullPath())

	if err := dir.maybeLoadEntry(); err != nil {
		return err
	}

	if err := listxattr(dir.entry, req, resp); err != nil {
		return err
	}

	return nil

}

func (dir *Dir) Forget() {
	glog.V(4).Infof("Forget dir %s", dir.FullPath())

	dir.wfs.fsNodeCache.DeleteFsNode(util.FullPath(dir.FullPath()))
}

func (dir *Dir) maybeLoadEntry() error {
	if dir.entry == nil {
		parentDirPath, name := util.FullPath(dir.FullPath()).DirAndName()
		entry, err := dir.wfs.maybeLoadEntry(parentDirPath, name)
		if err != nil {
			return err
		}
		dir.entry = entry
	}
	return nil
}

func (dir *Dir) saveEntry() error {

	parentDir, name := util.FullPath(dir.FullPath()).DirAndName()

	return dir.wfs.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		dir.wfs.mapPbIdFromLocalToFiler(dir.entry)
		defer dir.wfs.mapPbIdFromFilerToLocal(dir.entry)

		request := &filer_pb.UpdateEntryRequest{
			Directory:  parentDir,
			Entry:      dir.entry,
			Signatures: []int32{dir.wfs.signature},
		}

		glog.V(1).Infof("save dir entry: %v", request)
		_, err := client.UpdateEntry(context.Background(), request)
		if err != nil {
			glog.Errorf("UpdateEntry dir %s/%s: %v", parentDir, name, err)
			return fuse.EIO
		}

		dir.wfs.metaCache.UpdateEntry(context.Background(), filer.FromPbEntry(request.Directory, request.Entry))

		return nil
	})
}

func (dir *Dir) FullPath() string {
	var parts []string
	for p := dir; p != nil; p = p.parent {
		if strings.HasPrefix(p.name, "/") {
			if len(p.name) > 1 {
				parts = append(parts, p.name[1:])
			}
		} else {
			parts = append(parts, p.name)
		}
	}

	if len(parts) == 0 {
		return "/"
	}

	var buf bytes.Buffer
	for i := len(parts) - 1; i >= 0; i-- {
		buf.WriteString("/")
		buf.WriteString(parts[i])
	}
	return buf.String()
}
