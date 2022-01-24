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
	id     uint64
}

var _ = fs.Node(&Dir{})

var _ = fs.NodeIdentifier(&Dir{})
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

func (dir *Dir) Id() uint64 {
	if dir.parent == nil {
		return 1
	}
	return dir.id
}

func (dir *Dir) Attr(ctx context.Context, attr *fuse.Attr) error {

	entry, err := dir.maybeLoadEntry()
	if err != nil {
		glog.V(3).Infof("dir Attr %s, err: %+v", dir.FullPath(), err)
		return err
	}

	// https://github.com/bazil/fuse/issues/196
	attr.Valid = time.Second
	attr.Inode = dir.Id()
	attr.Mode = os.FileMode(entry.Attributes.FileMode) | os.ModeDir
	attr.Mtime = time.Unix(entry.Attributes.Mtime, 0)
	attr.Crtime = time.Unix(entry.Attributes.Crtime, 0)
	attr.Ctime = time.Unix(entry.Attributes.Mtime, 0)
	attr.Atime = time.Unix(entry.Attributes.Mtime, 0)
	attr.Gid = entry.Attributes.Gid
	attr.Uid = entry.Attributes.Uid

	if dir.FullPath() == dir.wfs.option.FilerMountRootPath {
		attr.BlockSize = blockSize
	}

	glog.V(4).Infof("dir Attr %s, attr: %+v", dir.FullPath(), attr)

	return nil
}

func (dir *Dir) Getxattr(ctx context.Context, req *fuse.GetxattrRequest, resp *fuse.GetxattrResponse) error {

	glog.V(4).Infof("dir Getxattr %s", dir.FullPath())

	entry, err := dir.maybeLoadEntry()
	if err != nil {
		return err
	}

	return getxattr(entry, req, resp)
}

func (dir *Dir) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	// fsync works at OS level
	// write the file chunks to the filerGrpcAddress
	glog.V(3).Infof("dir %s fsync %+v", dir.FullPath(), req)

	return nil
}

func (dir *Dir) newFile(name string, fileMode os.FileMode) fs.Node {

	fileFullPath := util.NewFullPath(dir.FullPath(), name)
	fileId := fileFullPath.AsInode(fileMode)
	dir.wfs.handlesLock.Lock()
	existingHandle, found := dir.wfs.handles[fileId]
	dir.wfs.handlesLock.Unlock()

	if found {
		glog.V(4).Infof("newFile found opened file handle: %+v", fileFullPath)
		return existingHandle.f
	}
	return &File{
		Name: name,
		dir:  dir,
		wfs:  dir.wfs,
		id:   fileId,
	}
}

func (dir *Dir) newDirectory(fullpath util.FullPath) fs.Node {

	return &Dir{name: fullpath.Name(), wfs: dir.wfs, parent: dir, id: fullpath.AsInode(os.ModeDir)}

}

func (dir *Dir) Create(ctx context.Context, req *fuse.CreateRequest,
	resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {

	if err := checkName(req.Name); err != nil {
		return nil, nil, err
	}

	exclusive := req.Flags&fuse.OpenExclusive != 0
	isDirectory := req.Mode&os.ModeDir > 0

	if exclusive || isDirectory {
		_, err := dir.doCreateEntry(req.Name, req.Mode, req.Uid, req.Gid, exclusive)
		if err != nil {
			return nil, nil, err
		}
	}
	var node fs.Node
	if isDirectory {
		node = dir.newDirectory(util.NewFullPath(dir.FullPath(), req.Name))
		return node, node, nil
	}

	node = dir.newFile(req.Name, req.Mode)
	file := node.(*File)
	file.entry = &filer_pb.Entry{
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
	}
	file.dirtyMetadata = true
	fh := dir.wfs.AcquireHandle(file, req.Uid, req.Gid)
	return file, fh, nil

}

func (dir *Dir) Mknod(ctx context.Context, req *fuse.MknodRequest) (fs.Node, error) {

	if err := checkName(req.Name); err != nil {
		return nil, err
	}

	glog.V(3).Infof("dir %s Mknod %+v", dir.FullPath(), req)

	_, err := dir.doCreateEntry(req.Name, req.Mode, req.Uid, req.Gid, false)

	if err != nil {
		return nil, err
	}
	var node fs.Node
	node = dir.newFile(req.Name, req.Mode)
	return node, nil
}

func (dir *Dir) doCreateEntry(name string, mode os.FileMode, uid, gid uint32, exclusive bool) (*filer_pb.CreateEntryRequest, error) {
	dirFullPath := dir.FullPath()
	request := &filer_pb.CreateEntryRequest{
		Directory: dirFullPath,
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
		OExcl:      exclusive,
		Signatures: []int32{dir.wfs.signature},
	}
	glog.V(1).Infof("create %s/%s", dirFullPath, name)

	err := dir.wfs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

		dir.wfs.mapPbIdFromLocalToFiler(request.Entry)
		defer dir.wfs.mapPbIdFromFilerToLocal(request.Entry)

		if err := filer_pb.CreateEntry(client, request); err != nil {
			if strings.Contains(err.Error(), "EEXIST") {
				return fuse.EEXIST
			}
			glog.V(0).Infof("create %s/%s: %v", dirFullPath, name, err)
			return fuse.EIO
		}

		if err := dir.wfs.metaCache.InsertEntry(context.Background(), filer.FromPbEntry(request.Directory, request.Entry)); err != nil {
			glog.Errorf("local InsertEntry dir %s/%s: %v", dirFullPath, name, err)
			return fuse.EIO
		}

		return nil
	})
	return request, err
}

func (dir *Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {

	if err := checkName(req.Name); err != nil {
		return nil, err
	}

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

	dirFullPath := dir.FullPath()

	err := dir.wfs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

		dir.wfs.mapPbIdFromLocalToFiler(newEntry)
		defer dir.wfs.mapPbIdFromFilerToLocal(newEntry)

		request := &filer_pb.CreateEntryRequest{
			Directory:  dirFullPath,
			Entry:      newEntry,
			Signatures: []int32{dir.wfs.signature},
		}

		glog.V(1).Infof("mkdir: %v", request)
		if err := filer_pb.CreateEntry(client, request); err != nil {
			glog.V(0).Infof("mkdir %s/%s: %v", dirFullPath, req.Name, err)
			return err
		}

		if err := dir.wfs.metaCache.InsertEntry(context.Background(), filer.FromPbEntry(request.Directory, request.Entry)); err != nil {
			glog.Errorf("local mkdir dir %s/%s: %v", dirFullPath, req.Name, err)
			return fuse.EIO
		}

		return nil
	})

	if err == nil {
		node := dir.newDirectory(util.NewFullPath(dirFullPath, req.Name))

		return node, nil
	}

	glog.V(0).Infof("mkdir %s/%s: %v", dirFullPath, req.Name, err)

	return nil, fuse.EIO
}

func (dir *Dir) Lookup(ctx context.Context, req *fuse.LookupRequest, resp *fuse.LookupResponse) (node fs.Node, err error) {

	if err := checkName(req.Name); err != nil {
		return nil, err
	}

	dirPath := util.FullPath(dir.FullPath())
	// glog.V(4).Infof("dir Lookup %s: %s by %s", dirPath, req.Name, req.Header.String())

	fullFilePath := dirPath.Child(req.Name)
	visitErr := meta_cache.EnsureVisited(dir.wfs.metaCache, dir.wfs, dirPath)
	if visitErr != nil {
		glog.Errorf("dir Lookup %s: %v", dirPath, visitErr)
		return nil, fuse.EIO
	}
	localEntry, cacheErr := dir.wfs.metaCache.FindEntry(context.Background(), fullFilePath)
	if cacheErr == filer_pb.ErrNotFound {
		return nil, fuse.ENOENT
	}

	if localEntry == nil {
		// glog.V(3).Infof("dir Lookup cache miss %s", fullFilePath)
		entry, err := filer_pb.GetEntry(dir.wfs, fullFilePath)
		if err != nil {
			glog.V(1).Infof("dir GetEntry %s: %v", fullFilePath, err)
			return nil, fuse.ENOENT
		}
		localEntry = filer.FromPbEntry(string(dirPath), entry)
	} else {
		glog.V(4).Infof("dir Lookup cache hit %s", fullFilePath)
	}

	if localEntry != nil {
		if localEntry.IsDirectory() {
			node = dir.newDirectory(fullFilePath)
		} else {
			node = dir.newFile(req.Name, localEntry.Attr.Mode)
		}

		// resp.EntryValid = time.Second
		resp.Attr.Valid = time.Second
		resp.Attr.Size = localEntry.FileSize
		resp.Attr.Mtime = localEntry.Attr.Mtime
		resp.Attr.Crtime = localEntry.Attr.Crtime
		resp.Attr.Mode = localEntry.Attr.Mode
		resp.Attr.Gid = localEntry.Attr.Gid
		resp.Attr.Uid = localEntry.Attr.Uid
		if localEntry.HardLinkCounter > 0 {
			resp.Attr.Nlink = uint32(localEntry.HardLinkCounter)
		}

		return node, nil
	}

	glog.V(4).Infof("not found dir GetEntry %s: %v", fullFilePath, err)
	return nil, fuse.ENOENT
}

func (dir *Dir) ReadDirAll(ctx context.Context) (ret []fuse.Dirent, err error) {

	dirPath := util.FullPath(dir.FullPath())
	glog.V(4).Infof("dir ReadDirAll %s", dirPath)

	processEachEntryFn := func(entry *filer.Entry, isLast bool) {
		if entry.IsDirectory() {
			dirent := fuse.Dirent{Name: entry.Name(), Type: fuse.DT_Dir, Inode: dirPath.Child(entry.Name()).AsInode(os.ModeDir)}
			ret = append(ret, dirent)
		} else {
			dirent := fuse.Dirent{Name: entry.Name(), Type: findFileType(uint16(entry.Attr.Mode)), Inode: dirPath.Child(entry.Name()).AsInode(entry.Attr.Mode)}
			ret = append(ret, dirent)
		}
	}

	if err = meta_cache.EnsureVisited(dir.wfs.metaCache, dir.wfs, dirPath); err != nil {
		glog.Errorf("dir ReadDirAll %s: %v", dirPath, err)
		return nil, fuse.EIO
	}
	listErr := dir.wfs.metaCache.ListDirectoryEntries(context.Background(), dirPath, "", false, int64(math.MaxInt32), func(entry *filer.Entry) bool {
		processEachEntryFn(entry, false)
		return true
	})
	if listErr != nil {
		glog.Errorf("list meta cache: %v", listErr)
		return nil, fuse.EIO
	}

	// create proper . and .. directories
	ret = append(ret, fuse.Dirent{
		Inode: dirPath.AsInode(os.ModeDir),
		Name:  ".",
		Type:  fuse.DT_Dir,
	})

	// return the correct parent inode for the mount root
	var inode uint64
	if string(dirPath) == dir.wfs.option.FilerMountRootPath {
		inode = dir.wfs.option.MountParentInode
	} else {
		inode = util.FullPath(dir.parent.FullPath()).AsInode(os.ModeDir)
	}

	ret = append(ret, fuse.Dirent{
		Inode: inode,
		Name:  "..",
		Type:  fuse.DT_Dir,
	})

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

	parentHasPermission := false
	parentEntry, err := dir.maybeLoadEntry()
	if err != nil {
		return err
	}
	if err := checkPermission(parentEntry, req.Uid, req.Gid, true); err == nil {
		parentHasPermission = true
	}

	dirFullPath := dir.FullPath()
	filePath := util.NewFullPath(dirFullPath, req.Name)
	entry, err := filer_pb.GetEntry(dir.wfs, filePath)
	if err != nil {
		return err
	}
	if !parentHasPermission {
		if err := checkPermission(entry, req.Uid, req.Gid, true); err != nil {
			return err
		}
	}

	if !req.Dir {
		return dir.removeOneFile(entry, req)
	}

	return dir.removeFolder(entry, req)

}

func (dir *Dir) removeOneFile(entry *filer_pb.Entry, req *fuse.RemoveRequest) error {

	dirFullPath := dir.FullPath()
	filePath := util.NewFullPath(dirFullPath, req.Name)

	// first, ensure the filer store can correctly delete
	glog.V(3).Infof("remove file: %v", req)
	isDeleteData := entry != nil && entry.HardLinkCounter <= 1
	err := filer_pb.Remove(dir.wfs, dirFullPath, req.Name, isDeleteData, false, false, false, []int32{dir.wfs.signature})
	if err != nil {
		glog.V(3).Infof("not found remove file %s: %v", filePath, err)
		return fuse.ENOENT
	}

	// then, delete meta cache and fsNode cache
	if err = dir.wfs.metaCache.DeleteEntry(context.Background(), filePath); err != nil {
		glog.V(3).Infof("local DeleteEntry %s: %v", filePath, err)
		return fuse.ESTALE
	}

	// remove current file handle if any
	dir.wfs.handlesLock.Lock()
	defer dir.wfs.handlesLock.Unlock()
	inodeId := filePath.AsInode(0)
	if fh, ok := dir.wfs.handles[inodeId]; ok {
		delete(dir.wfs.handles, inodeId)
		fh.isDeleted = true
	}

	return nil

}

func (dir *Dir) removeFolder(entry *filer_pb.Entry, req *fuse.RemoveRequest) error {

	dirFullPath := dir.FullPath()

	glog.V(3).Infof("remove directory entry: %v", req)
	ignoreRecursiveErr := true // ignore recursion error since the OS should manage it
	err := filer_pb.Remove(dir.wfs, dirFullPath, req.Name, true, true, ignoreRecursiveErr, false, []int32{dir.wfs.signature})
	if err != nil {
		glog.V(0).Infof("remove %s/%s: %v", dirFullPath, req.Name, err)
		if strings.Contains(err.Error(), filer.MsgFailDelNonEmptyFolder) {
			return fuse.EEXIST
		}
		return fuse.ENOENT
	}

	t := util.NewFullPath(dirFullPath, req.Name)
	dir.wfs.metaCache.DeleteEntry(context.Background(), t)

	return nil

}

func (dir *Dir) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {

	glog.V(4).Infof("%v dir setattr %+v mode=%d", dir.FullPath(), req, req.Mode)

	entry, err := dir.maybeLoadEntry()
	if err != nil {
		return err
	}

	if req.Valid.Mode() {
		entry.Attributes.FileMode = uint32(req.Mode)
	}

	if req.Valid.Uid() {
		entry.Attributes.Uid = req.Uid
	}

	if req.Valid.Gid() {
		entry.Attributes.Gid = req.Gid
	}

	if req.Valid.Mtime() {
		entry.Attributes.Mtime = req.Mtime.Unix()
	}

	entry.Attributes.Mtime = time.Now().Unix()

	return dir.saveEntry(entry)

}

func (dir *Dir) Setxattr(ctx context.Context, req *fuse.SetxattrRequest) error {

	glog.V(4).Infof("dir Setxattr %s: %s", dir.FullPath(), req.Name)

	entry, err := dir.maybeLoadEntry()
	if err != nil {
		return err
	}

	if err := setxattr(entry, req); err != nil {
		return err
	}

	return dir.saveEntry(entry)

}

func (dir *Dir) Removexattr(ctx context.Context, req *fuse.RemovexattrRequest) error {

	glog.V(4).Infof("dir Removexattr %s: %s", dir.FullPath(), req.Name)

	entry, err := dir.maybeLoadEntry()
	if err != nil {
		return err
	}

	if err := removexattr(entry, req); err != nil {
		return err
	}

	return dir.saveEntry(entry)

}

func (dir *Dir) Listxattr(ctx context.Context, req *fuse.ListxattrRequest, resp *fuse.ListxattrResponse) error {

	glog.V(4).Infof("dir Listxattr %s", dir.FullPath())

	entry, err := dir.maybeLoadEntry()
	if err != nil {
		return err
	}

	if err := listxattr(entry, req, resp); err != nil {
		return err
	}

	return nil

}

func (dir *Dir) Forget() {
	glog.V(4).Infof("Forget dir %s", dir.FullPath())
}

func (dir *Dir) maybeLoadEntry() (*filer_pb.Entry, error) {
	parentDirPath, name := util.FullPath(dir.FullPath()).DirAndName()
	return dir.wfs.maybeLoadEntry(parentDirPath, name)
}

func (dir *Dir) saveEntry(entry *filer_pb.Entry) error {

	parentDir, name := util.FullPath(dir.FullPath()).DirAndName()

	return dir.wfs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

		dir.wfs.mapPbIdFromLocalToFiler(entry)
		defer dir.wfs.mapPbIdFromFilerToLocal(entry)

		request := &filer_pb.UpdateEntryRequest{
			Directory:  parentDir,
			Entry:      entry,
			Signatures: []int32{dir.wfs.signature},
		}

		glog.V(1).Infof("save dir entry: %v", request)
		_, err := client.UpdateEntry(context.Background(), request)
		if err != nil {
			glog.Errorf("UpdateEntry dir %s/%s: %v", parentDir, name, err)
			return fuse.EIO
		}

		if err := dir.wfs.metaCache.UpdateEntry(context.Background(), filer.FromPbEntry(request.Directory, request.Entry)); err != nil {
			glog.Errorf("UpdateEntry dir %s/%s: %v", parentDir, name, err)
			return fuse.ESTALE
		}

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
