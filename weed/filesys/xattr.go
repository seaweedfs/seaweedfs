package filesys

import (
	"context"
	"strings"
	"syscall"

	"github.com/seaweedfs/fuse"

	"github.com/chrislusf/seaweedfs/weed/filesys/meta_cache"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
)

const (
	XATTR_PREFIX = "xattr-" // same as filer
)

func getxattr(entry *filer_pb.Entry, req *fuse.GetxattrRequest, resp *fuse.GetxattrResponse) error {

	if entry == nil {
		return fuse.ErrNoXattr
	}
	if entry.Extended == nil {
		return fuse.ErrNoXattr
	}
	data, found := entry.Extended[XATTR_PREFIX+req.Name]
	if !found {
		return fuse.ErrNoXattr
	}
	if req.Position < uint32(len(data)) {
		size := req.Size
		if req.Position+size >= uint32(len(data)) {
			size = uint32(len(data)) - req.Position
		}
		if size == 0 {
			resp.Xattr = data[req.Position:]
		} else {
			resp.Xattr = data[req.Position : req.Position+size]
		}
	}

	return nil

}

func setxattr(entry *filer_pb.Entry, req *fuse.SetxattrRequest) error {

	if entry == nil {
		return fuse.EIO
	}

	if entry.Extended == nil {
		entry.Extended = make(map[string][]byte)
	}
	data, _ := entry.Extended[XATTR_PREFIX+req.Name]

	newData := make([]byte, int(req.Position)+len(req.Xattr))

	copy(newData, data)

	copy(newData[int(req.Position):], req.Xattr)

	entry.Extended[XATTR_PREFIX+req.Name] = newData

	return nil

}

func removexattr(entry *filer_pb.Entry, req *fuse.RemovexattrRequest) error {

	if entry == nil {
		return fuse.ErrNoXattr
	}

	if entry.Extended == nil {
		return fuse.ErrNoXattr
	}

	_, found := entry.Extended[XATTR_PREFIX+req.Name]

	if !found {
		return fuse.ErrNoXattr
	}

	delete(entry.Extended, XATTR_PREFIX+req.Name)

	return nil

}

func listxattr(entry *filer_pb.Entry, req *fuse.ListxattrRequest, resp *fuse.ListxattrResponse) error {

	if entry == nil {
		return fuse.EIO
	}

	for k := range entry.Extended {
		if strings.HasPrefix(k, XATTR_PREFIX) {
			resp.Append(k[len(XATTR_PREFIX):])
		}
	}

	size := req.Size
	if req.Position+size >= uint32(len(resp.Xattr)) {
		size = uint32(len(resp.Xattr)) - req.Position
	}

	if size == 0 {
		resp.Xattr = resp.Xattr[req.Position:]
	} else {
		resp.Xattr = resp.Xattr[req.Position : req.Position+size]
	}

	return nil

}

func (wfs *WFS) maybeLoadEntry(dir, name string) (entry *filer_pb.Entry, err error) {

	fullpath := util.NewFullPath(dir, name)
	// glog.V(3).Infof("read entry cache miss %s", fullpath)

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
		}, nil
	}

	// read from async meta cache
	meta_cache.EnsureVisited(wfs.metaCache, wfs, util.FullPath(dir))
	cachedEntry, cacheErr := wfs.metaCache.FindEntry(context.Background(), fullpath)
	if cacheErr == filer_pb.ErrNotFound {
		return nil, fuse.ENOENT
	}
	return cachedEntry.ToProtoEntry(), cacheErr
}

func checkName(name string) error {
	if len(name) >= 256 {
		return syscall.ENAMETOOLONG
	}
	return nil
}
