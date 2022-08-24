package mount

import (
	"context"
	"fmt"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"syscall"
)

func (wfs *WFS) saveEntry(path util.FullPath, entry *filer_pb.Entry) (code fuse.Status) {

	parentDir, _ := path.DirAndName()

	err := wfs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

		wfs.mapPbIdFromLocalToFiler(entry)
		defer wfs.mapPbIdFromFilerToLocal(entry)

		request := &filer_pb.UpdateEntryRequest{
			Directory:  parentDir,
			Entry:      entry,
			Signatures: []int32{wfs.signature},
		}

		glog.V(1).Infof("save entry: %v", request)
		_, err := client.UpdateEntry(context.Background(), request)
		if err != nil {
			return fmt.Errorf("UpdateEntry dir %s: %v", path, err)
		}

		if err := wfs.metaCache.UpdateEntry(context.Background(), filer.FromPbEntry(request.Directory, request.Entry)); err != nil {
			return fmt.Errorf("UpdateEntry dir %s: %v", path, err)
		}

		return nil
	})
	if err != nil {
		glog.Errorf("saveEntry %s: %v", path, err)
		return fuse.EIO
	}

	return fuse.OK
}

func (wfs *WFS) mapPbIdFromFilerToLocal(entry *filer_pb.Entry) {
	if entry.Attributes == nil {
		return
	}
	entry.Attributes.Uid, entry.Attributes.Gid = wfs.option.UidGidMapper.FilerToLocal(entry.Attributes.Uid, entry.Attributes.Gid)
}
func (wfs *WFS) mapPbIdFromLocalToFiler(entry *filer_pb.Entry) {
	if entry.Attributes == nil {
		return
	}
	entry.Attributes.Uid, entry.Attributes.Gid = wfs.option.UidGidMapper.LocalToFiler(entry.Attributes.Uid, entry.Attributes.Gid)
}

func checkName(name string) fuse.Status {
	if len(name) >= 4096 {
		return fuse.Status(syscall.ENAMETOOLONG)
	}
	return fuse.OK
}
