package mount

import (
	"context"
	"syscall"
	"time"

	"github.com/seaweedfs/go-fuse/v2/fuse"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
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
	if wfs.IsOverQuotaWithUncommitted() {
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
	origHardLinkId := oldEntry.HardLinkId
	origHardLinkCounter := oldEntry.HardLinkCounter
	if len(oldEntry.HardLinkId) == 0 {
		oldEntry.HardLinkId = filer.NewHardLinkId()
		oldEntry.HardLinkCounter = 1
		glog.V(4).Infof("Link: new HardLinkId %x for %s", oldEntry.HardLinkId, oldEntryPath)
	}
	oldEntry.HardLinkCounter++
	glog.V(4).Infof("Link: %s -> %s/%s HardLinkId %x counter=%d",
		oldEntryPath, newParentPath, name, oldEntry.HardLinkId, oldEntry.HardLinkCounter)
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
	wfs.mapPbIdFromLocalToFiler(request.Entry)

	ctx := context.Background()
	updateResp, err := wfs.streamUpdateEntry(ctx, updateOldEntryRequest)
	if err == nil {
		updateEvent := updateResp.GetMetadataEvent()
		if updateEvent == nil {
			updateEvent = metadataUpdateEvent(oldParentPath, updateOldEntryRequest.Entry)
		}
		if applyErr := wfs.applyLocalMetadataEvent(ctx, updateEvent); applyErr != nil {
			glog.Warningf("link %s: best-effort metadata apply failed: %v", oldEntryPath, applyErr)
			wfs.inodeToPath.InvalidateChildrenCache(util.FullPath(oldParentPath))
		}
	}
	if err == nil {
		var createResp *filer_pb.CreateEntryResponse
		createResp, err = wfs.streamCreateEntry(ctx, request)
		if err != nil {
			// Rollback: restore original HardLinkId/Counter on the source entry
			oldEntry.HardLinkId = origHardLinkId
			oldEntry.HardLinkCounter = origHardLinkCounter
			rollbackReq := &filer_pb.UpdateEntryRequest{
				Directory:  oldParentPath,
				Entry:      oldEntry,
				Signatures: []int32{wfs.signature},
			}
			if _, rollbackErr := wfs.streamUpdateEntry(ctx, rollbackReq); rollbackErr != nil {
				glog.Warningf("link rollback %s: %v", oldEntryPath, rollbackErr)
			}
		} else {
			createEvent := createResp.GetMetadataEvent()
			if createEvent == nil {
				createEvent = metadataCreateEvent(string(newParentPath), request.Entry)
			}
			if applyErr := wfs.applyLocalMetadataEvent(ctx, createEvent); applyErr != nil {
				glog.Warningf("link %s: best-effort metadata apply failed: %v", newParentPath.Child(name), applyErr)
				wfs.inodeToPath.InvalidateChildrenCache(newParentPath)
			}
		}
	}

	newEntryPath := newParentPath.Child(name)

	// Map back to local uid/gid before writing attributes to the kernel.
	wfs.mapPbIdFromFilerToLocal(request.Entry)

	if err != nil {
		glog.V(0).Infof("Link %v -> %s: %v", oldEntryPath, newEntryPath, err)
		return fuse.EIO
	}

	wfs.inodeToPath.AddPath(oldEntry.Attributes.Inode, newEntryPath)

	wfs.outputPbEntry(out, oldEntry.Attributes.Inode, request.Entry)

	return fuse.OK
}
