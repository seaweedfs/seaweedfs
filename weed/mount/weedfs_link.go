package mount

import (
	"bytes"
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
	linkNow := time.Now()
	oldEntry.Attributes.Mtime = linkNow.Unix()
	oldEntry.Attributes.MtimeNs = int32(linkNow.Nanosecond())
	oldEntry.Attributes.Ctime = linkNow.Unix()
	oldEntry.Attributes.CtimeNs = int32(linkNow.Nanosecond())
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
			wfs.touchDirMtimeCtimeBest(newParentPath)
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

	// Propagate the new HardLinkCounter to sibling cache entries and
	// invalidate the kernel's inode attr cache. Without this, `stat` on any
	// existing sibling link (other than the source we just wrote) returns
	// the old nlink from the local metacache — pjdfstest link/00.t catches
	// this after `link n1 n2` when it stats n0.
	wfs.syncHardLinkSiblings(oldEntry.Attributes.Inode, oldEntry, oldEntryPath, newEntryPath)

	wfs.outputPbEntry(out, oldEntry.Attributes.Inode, request.Entry)

	return fuse.OK
}

// syncHardLinkSiblings rewrites the cached HardLinkCounter (and ctime) on
// every sibling link of the given inode, and invalidates the kernel's inode
// attr cache. `authoritativeEntry` carries the counter/ctime that should be
// propagated. `skipPaths` are the link paths already updated by the caller
// (typically the source and/or the newly created/removed link).
func (wfs *WFS) syncHardLinkSiblings(inode uint64, authoritativeEntry *filer_pb.Entry, skipPaths ...util.FullPath) {
	if authoritativeEntry == nil || len(authoritativeEntry.HardLinkId) == 0 {
		return
	}
	paths := wfs.inodeToPath.GetAllPaths(inode)
	if len(paths) == 0 {
		return
	}
	skip := make(map[util.FullPath]struct{}, len(skipPaths))
	for _, p := range skipPaths {
		skip[p] = struct{}{}
	}
	ctx := context.Background()
	for _, p := range paths {
		if _, skipped := skip[p]; skipped {
			continue
		}
		sibling, err := wfs.metaCache.FindEntry(ctx, p)
		if err != nil || sibling == nil {
			continue
		}
		// Only touch siblings that genuinely share the same hard-link id.
		// inodeToPath's shared-inode invariant should already guarantee
		// this, but a mismatch can occur transiently (e.g. a rename
		// replaced one of the paths), and blindly stamping an unrelated
		// entry's counter would corrupt it.
		if !bytes.Equal(sibling.HardLinkId, authoritativeEntry.HardLinkId) {
			continue
		}
		sibling.HardLinkCounter = authoritativeEntry.HardLinkCounter
		if authoritativeEntry.Attributes != nil {
			sibling.Attr.Ctime = time.Unix(authoritativeEntry.Attributes.Ctime, int64(authoritativeEntry.Attributes.CtimeNs))
		}
		if err := wfs.metaCache.UpdateEntry(ctx, sibling); err != nil {
			glog.V(4).Infof("syncHardLinkSiblings update %s: %v", p, err)
		}
	}
	if wfs.fuseServer != nil {
		if status := wfs.fuseServer.InodeNotify(inode, 0, -1); status != fuse.OK {
			glog.V(4).Infof("syncHardLinkSiblings invalidate inode %d: %v", inode, status)
		}
	}
}
