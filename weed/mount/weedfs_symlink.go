package mount

import (
	"context"
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

/** Create a symbolic link */
func (wfs *WFS) Symlink(cancel <-chan struct{}, header *fuse.InHeader, target string, name string, out *fuse.EntryOut) (code fuse.Status) {

	if wfs.IsOverQuota {
		return fuse.Status(syscall.ENOSPC)
	}
	if s := checkName(name); s != fuse.OK {
		return s
	}

	dirPath, code := wfs.inodeToPath.GetPath(header.NodeId)
	if code != fuse.OK {
		return
	}
	entryFullPath := dirPath.Child(name)

	request := &filer_pb.CreateEntryRequest{
		Directory: string(dirPath),
		Entry: &filer_pb.Entry{
			Name:        name,
			IsDirectory: false,
			Attributes: &filer_pb.FuseAttributes{
				Mtime:         time.Now().Unix(),
				Crtime:        time.Now().Unix(),
				FileMode:      uint32(os.FileMode(0777) | os.ModeSymlink),
				Uid:           header.Uid,
				Gid:           header.Gid,
				SymlinkTarget: target,
			},
		},
		Signatures:               []int32{wfs.signature},
		SkipCheckParentDirectory: true,
	}

	err := wfs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

		wfs.mapPbIdFromLocalToFiler(request.Entry)
		defer wfs.mapPbIdFromFilerToLocal(request.Entry)

		if err := filer_pb.CreateEntry(context.Background(), client, request); err != nil {
			return fmt.Errorf("symlink %s: %v", entryFullPath, err)
		}

		wfs.metaCache.InsertEntry(context.Background(), filer.FromPbEntry(request.Directory, request.Entry))

		return nil
	})
	if err != nil {
		glog.V(0).Infof("Symlink %s => %s: %v", entryFullPath, target, err)
		return fuse.EIO
	}

	inode := wfs.inodeToPath.Lookup(entryFullPath, request.Entry.Attributes.Crtime, false, false, 0, true)

	wfs.outputPbEntry(out, inode, request.Entry)

	return fuse.OK
}

func (wfs *WFS) Readlink(cancel <-chan struct{}, header *fuse.InHeader) (out []byte, code fuse.Status) {
	entryFullPath, code := wfs.inodeToPath.GetPath(header.NodeId)
	if code != fuse.OK {
		return
	}

	entry, status := wfs.maybeLoadEntry(entryFullPath)
	if status != fuse.OK {
		return nil, status
	}
	if os.FileMode(entry.Attributes.FileMode)&os.ModeSymlink == 0 {
		return nil, fuse.EINVAL
	}

	return []byte(entry.Attributes.SymlinkTarget), fuse.OK
}
