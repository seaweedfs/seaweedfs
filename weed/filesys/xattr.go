package filesys

import (
	"context"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/seaweedfs/fuse"
)

func getxattr(entry *filer_pb.Entry, req *fuse.GetxattrRequest, resp *fuse.GetxattrResponse) error {

	if entry == nil {
		return fuse.ErrNoXattr
	}
	if entry.Extended == nil {
		return fuse.ErrNoXattr
	}
	data, found := entry.Extended[req.Name]
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
	data, _ := entry.Extended[req.Name]

	newData := make([]byte, int(req.Position)+len(req.Xattr))

	copy(newData, data)

	copy(newData[int(req.Position):], req.Xattr)

	entry.Extended[req.Name] = newData

	return nil

}

func removexattr(entry *filer_pb.Entry, req *fuse.RemovexattrRequest) error {

	if entry == nil {
		return fuse.ErrNoXattr
	}

	if entry.Extended == nil {
		return fuse.ErrNoXattr
	}

	_, found := entry.Extended[req.Name]

	if !found {
		return fuse.ErrNoXattr
	}

	delete(entry.Extended, req.Name)

	return nil

}

func listxattr(entry *filer_pb.Entry, req *fuse.ListxattrRequest, resp *fuse.ListxattrResponse) error {

	if entry == nil {
		return fuse.EIO
	}

	for k := range entry.Extended {
		resp.Append(k)
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
	entry = wfs.cacheGet(fullpath)
	if entry != nil {
		return
	}
	// glog.V(3).Infof("read entry cache miss %s", fullpath)

	// read from async meta cache
	if wfs.option.AsyncMetaDataCaching {
		cachedEntry, cacheErr := wfs.metaCache.FindEntry(context.Background(), fullpath)
		if cacheErr == filer_pb.ErrNotFound {
			return nil, fuse.ENOENT
		}
		return cachedEntry.ToProtoEntry(), nil
	}

	err = wfs.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.LookupDirectoryEntryRequest{
			Name:      name,
			Directory: dir,
		}

		resp, err := filer_pb.LookupEntry(client, request)
		if err != nil {
			if err == filer_pb.ErrNotFound {
				glog.V(3).Infof("file attr read not found file %v: %v", request, err)
				return fuse.ENOENT
			}
			glog.V(3).Infof("attr read %v: %v", request, err)
			return fuse.EIO
		}

		entry = resp.Entry
		wfs.cacheSet(fullpath, entry, wfs.option.EntryCacheTtl)

		return nil
	})

	return
}
