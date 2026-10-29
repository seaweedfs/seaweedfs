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

	var s fuse.Status
	if name, s = checkName(name); s != fuse.OK {
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

	// The new link has to be reported with the node id the kernel already holds
	// for the source, which is what makes the two names share one file.
	// oldEntry.Attributes.Inode is not a substitute. It is a mount-runtime
	// number that only entries created through a mount carry: entries written
	// by the S3 API, WebDAV or a direct filer call persist inode 0, and the
	// kernel rejects a LINK reply with NodeId 0 as EIO (invalid_nodeid). It is
	// also the wrong key for inodeToPath, which is keyed by the numbers handed
	// to the kernel rather than by the persisted attribute; Lookup's collision
	// probe can move the two apart even for an entry that does carry one.
	sourceInode := in.Oldnodeid

	ctx, cancelFunc := ctxFromCancel(cancel)
	defer cancelFunc()
	oldEntry, _, status := wfs.maybeLoadEntry(ctx, oldEntryPath)
	if status != fuse.OK {
		return status
	}

	// hardlink is not allowed in WORM mode
	if wormEnforced, _ := wfs.wormEnforcedForEntry(oldEntryPath, oldEntry); wormEnforced {
		return fuse.EPERM
	}

	// If the source is already a hard link, serialize on its HardLinkId
	// so concurrent Link/Unlink operations on different siblings cannot
	// both compute a new counter from a stale base. Re-load the entry
	// under the lock to pick up any prior holder's sibling update.
	if len(oldEntry.HardLinkId) > 0 {
		hlKey := string(oldEntry.HardLinkId)
		lock := wfs.hardLinkLockTable.AcquireLock("link", hlKey, util.ExclusiveLock)
		defer wfs.hardLinkLockTable.ReleaseLock(hlKey, lock)
		// Under the lock, re-resolve the source alias from the inode.
		// A concurrent Unlink that held this same lock may have removed
		// the specific alias we picked pre-lock even though other
		// sibling hard links for the same inode are still around;
		// GetPath(Oldnodeid) returns whichever alias is still active.
		refreshedPath, refreshedStatus := wfs.inodeToPath.GetPath(in.Oldnodeid)
		if refreshedStatus != fuse.OK {
			return refreshedStatus
		}
		oldEntryPath = refreshedPath
		oldParentPath, _ = oldEntryPath.DirAndName()
		// Do not fall back to the pre-lock snapshot: if every alias
		// was deleted while we waited, abort instead of deriving the
		// next counter from a stale entry.
		fresh, _, freshStatus := wfs.maybeLoadEntry(ctx, oldEntryPath)
		if freshStatus != fuse.OK {
			return freshStatus
		}
		oldEntry = fresh
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

	updateResp, err := wfs.streamUpdateEntry(context.Background(), updateOldEntryRequest)
	if err == nil {
		updateEvent := updateResp.GetMetadataEvent()
		if updateEvent == nil {
			updateEvent = metadataUpdateEvent(oldParentPath, updateOldEntryRequest.Entry)
			if updateEvent != nil {
				updateEvent.TsNs = ackVersionTsNs(updateResp)
			}
		}
		if applyErr := wfs.applyLocalMetadataEvent(context.Background(), updateEvent); applyErr != nil {
			glog.Warningf("link %s: best-effort metadata apply failed: %v", oldEntryPath, applyErr)
			wfs.inodeToPath.InvalidateChildrenCache(util.FullPath(oldParentPath))
		}
	}
	var linkVersionTsNs int64
	if err == nil {
		var createResp *filer_pb.CreateEntryResponse
		createResp, err = wfs.streamCreateEntry(context.Background(), request)
		if err != nil {
			// Rollback: restore original HardLinkId/Counter on the source entry
			oldEntry.HardLinkId = origHardLinkId
			oldEntry.HardLinkCounter = origHardLinkCounter
			rollbackReq := &filer_pb.UpdateEntryRequest{
				Directory:  oldParentPath,
				Entry:      oldEntry,
				Signatures: []int32{wfs.signature},
			}
			if _, rollbackErr := wfs.streamUpdateEntry(context.Background(), rollbackReq); rollbackErr != nil {
				glog.Warningf("link rollback %s: %v", oldEntryPath, rollbackErr)
			}
		} else {
			linkVersionTsNs = ackVersionTsNs(createResp)
			createEvent := createResp.GetMetadataEvent()
			if createEvent == nil {
				createEvent = metadataCreateEvent(string(newParentPath), request.Entry)
				if createEvent != nil {
					createEvent.TsNs = linkVersionTsNs
				}
			}
			if applyErr := wfs.applyLocalMetadataEvent(context.Background(), createEvent); applyErr != nil {
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

	wfs.inodeToPath.AddPath(sourceInode, newEntryPath)

	// Propagate the new HardLinkCounter to sibling cache entries and
	// invalidate the kernel's inode attr cache. Without this, `stat` on any
	// existing sibling link (other than the source we just wrote) returns
	// the old nlink from the local metacache — pjdfstest link/00.t catches
	// this after `link n1 n2` when it stats n0.
	wfs.syncHardLinkSiblings(sourceInode, oldEntry, linkVersionTsNs, oldEntryPath, newEntryPath)

	wfs.outputPbEntry(out, sourceInode, request.Entry)

	return fuse.OK
}

// syncHardLinkSiblings refreshes every sibling link of the given inode from
// the filer, so their cached HardLinkCounter (and any other fields) reflect
// the mutation. `authoritativeEntry` identifies the hard-link group;
// `versionTsNs` is the log position the mutation committed at — the fetch
// runs after the commit, so the fetched state already covers it, and the
// recorded position is the higher of the two. `skipPaths` are the link paths
// already updated by the caller (typically the source and/or the newly
// created/removed link).
func (wfs *WFS) syncHardLinkSiblings(inode uint64, authoritativeEntry *filer_pb.Entry, versionTsNs int64, skipPaths ...util.FullPath) {
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
		sibling, _, err := wfs.metaCache.FindEntry(ctx, p)
		if err != nil || sibling == nil {
			continue
		}
		// Refresh the sibling from the filer rather than stamping the cached
		// copy: the cache may predate an in-flight event of its own, and
		// claiming this mutation's version for that stale content would
		// fence the pending event out at the version gate, leaving old
		// chunks and attributes until a refresh.
		pbEntry, entryTsNs, _, fetchErr := filer_pb.GetEntry(ctx, wfs, p)
		if fetchErr == nil {
			// A path the filer no longer knows is on its way out — leave the
			// cache alone. Only touch entries that genuinely share the same
			// hard-link id: inodeToPath's shared-inode invariant should
			// already guarantee this, but a mismatch can occur transiently
			// (e.g. a rename replaced one of the paths).
			if pbEntry == nil || !bytes.Equal(pbEntry.HardLinkId, authoritativeEntry.HardLinkId) {
				continue
			}
			if entryTsNs < versionTsNs {
				entryTsNs = versionTsNs
			}
			dir, _ := p.DirAndName()
			if err := wfs.metaCache.UpdateEntryChecked(ctx, filer.FromPbEntry(dir, pbEntry), entryTsNs); err != nil {
				glog.V(4).Infof("syncHardLinkSiblings update %s: %v", p, err)
			}
			continue
		}
		// The fetch failed: patch the counter in place so the sibling is
		// not left stale, accepting that this mutation's version is claimed
		// for content that may predate it.
		if !bytes.Equal(sibling.HardLinkId, authoritativeEntry.HardLinkId) {
			continue
		}
		sibling.HardLinkCounter = authoritativeEntry.HardLinkCounter
		if authoritativeEntry.Attributes != nil {
			sibling.Attr.Ctime = time.Unix(authoritativeEntry.Attributes.Ctime, int64(authoritativeEntry.Attributes.CtimeNs))
		}
		if err := wfs.metaCache.UpdateEntry(ctx, sibling, versionTsNs); err != nil {
			glog.V(4).Infof("syncHardLinkSiblings update %s: %v", p, err)
		}
	}
	// Note: we deliberately do NOT call fuseServer.InodeNotify here. That
	// call would be made from the FUSE Link request handler goroutine, and
	// writes onto the same /dev/fuse fd that the kernel is still waiting to
	// read the Link reply from — causing a self-notify deadlock. The kernel
	// will re-stat siblings once its attr-cache TTL expires.
}
