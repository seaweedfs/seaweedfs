package mount

import (
	"context"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

/*
What is an inode?
If the file is an hardlinked file:
	use the hardlink id as inode
Otherwise:
	use the file path as inode

When creating a link:
	use the original file inode
*/

/** Create a hard link to a file */
func (wfs *WFS) Link(cancel <-chan struct{}, in *fuse.LinkIn, name string, out *fuse.EntryOut) (code fuse.Status) {
	if wfs.IsOverQuota {
		return fuse.Status(syscall.ENOSPC)
	}

	if s := checkName(name); s != fuse.OK {
		return s
	}

	newParentPath, code := wfs.inodeToPath.GetPath(in.NodeId)
	if code != fuse.OK {
		return
	}
	oldEntryPath, code := wfs.inodeToPath.GetPath(in.Oldnodeid)
	if code != fuse.OK {
		return
	}
	oldParentPath, _ := oldEntryPath.DirAndName()

	oldEntry, status := wfs.maybeLoadEntry(oldEntryPath)
	if status != fuse.OK {
		return status
	}

	// hardlink is not allowed in WORM mode
	if wormEnforced, _ := wfs.wormEnforcedForEntry(oldEntryPath, oldEntry); wormEnforced {
		return fuse.EPERM
	}

	// update old file to hardlink mode
	if len(oldEntry.HardLinkId) == 0 {
		oldEntry.HardLinkId = filer.NewHardLinkId()
		oldEntry.HardLinkCounter = 1
	}
	oldEntry.HardLinkCounter++
	updateOldEntryRequest := &filer_pb.UpdateEntryRequest{
		Directory:  oldParentPath,
		Entry:      oldEntry,
		Signatures: []int32{wfs.signature},
	}

	// CreateLink 1.2 : update new file to hardlink mode
	oldEntry.Attributes.Mtime = time.Now().Unix()
	request := &filer_pb.CreateEntryRequest{
		Directory: string(newParentPath),
		Entry: &filer_pb.Entry{
			Name:            name,
			IsDirectory:     false,
			Attributes:      oldEntry.Attributes,
			Chunks:          oldEntry.GetChunks(),
			Extended:        oldEntry.Extended,
			HardLinkId:      oldEntry.HardLinkId,
			HardLinkCounter: oldEntry.HardLinkCounter,
		},
		Signatures:               []int32{wfs.signature},
		SkipCheckParentDirectory: true,
	}

	// apply changes to the filer, and also apply to local metaCache
	err := wfs.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {

		wfs.mapPbIdFromLocalToFiler(request.Entry)
		defer wfs.mapPbIdFromFilerToLocal(request.Entry)

		if err := filer_pb.UpdateEntry(client, updateOldEntryRequest); err != nil {
			return err
		}
		wfs.metaCache.UpdateEntry(context.Background(), filer.FromPbEntry(updateOldEntryRequest.Directory, updateOldEntryRequest.Entry))

		if err := filer_pb.CreateEntry(client, request); err != nil {
			return err
		}

		wfs.metaCache.InsertEntry(context.Background(), filer.FromPbEntry(request.Directory, request.Entry))

		return nil
	})

	newEntryPath := newParentPath.Child(name)

	if err != nil {
		glog.V(0).Infof("Link %v -> %s: %v", oldEntryPath, newEntryPath, err)
		return fuse.EIO
	}

	wfs.inodeToPath.AddPath(oldEntry.Attributes.Inode, newEntryPath)

	wfs.outputPbEntry(out, oldEntry.Attributes.Inode, request.Entry)

	return fuse.OK
}
