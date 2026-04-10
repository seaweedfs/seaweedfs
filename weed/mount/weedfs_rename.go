package mount

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"syscall"

	"github.com/seaweedfs/go-fuse/v2/fs"
	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/cluster/lock_manager"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// doRename tries the streaming mux first, falling back to unary on transport errors.
func (wfs *WFS) doRename(ctx context.Context, request *filer_pb.StreamRenameEntryRequest, oldPath, newPath util.FullPath) error {
	if wfs.streamMutate != nil && wfs.streamMutate.IsAvailable() {
		err := wfs.streamMutate.Rename(ctx, request, func(resp *filer_pb.StreamRenameEntryResponse) error {
			return wfs.handleRenameResponse(ctx, resp)
		})
		if err == nil || !errors.Is(err, ErrStreamTransport) {
			return err // success or application error
		}
		glog.V(1).Infof("Rename %s => %s: stream failed, falling back to unary: %v", oldPath, newPath, err)
	}
	return wfs.WithFilerClient(true, func(client filer_pb.SeaweedFilerClient) error {
		stream, streamErr := client.StreamRenameEntry(ctx, request)
		if streamErr != nil {
			return fmt.Errorf("dir AtomicRenameEntry %s => %s : %v", oldPath, newPath, streamErr)
		}
		for {
			resp, recvErr := stream.Recv()
			if recvErr != nil {
				if recvErr == io.EOF {
					break
				}
				return fmt.Errorf("dir Rename %s => %s receive: %v", oldPath, newPath, recvErr)
			}
			if err := wfs.handleRenameResponse(ctx, resp); err != nil {
				return err
			}
		}
		return nil
	})
}

/** Rename a file
 *
 * If the target exists it should be atomically replaced. If
 * the target's inode's lookup count is non-zero, the file
 * system is expected to postpone any removal of the inode
 * until the lookup count reaches zero (see description of the
 * forget function).
 *
 * If this request is answered with an error code of ENOSYS, this is
 * treated as a permanent failure with error code EINVAL, i.e. all
 * future bmap requests will fail with EINVAL without being
 * send to the filesystem process.
 *
 * *flags* may be `RENAME_EXCHANGE` or `RENAME_NOREPLACE`. If
 * RENAME_NOREPLACE is specified, the filesystem must not
 * overwrite *newname* if it exists and return an error
 * instead. If `RENAME_EXCHANGE` is specified, the filesystem
 * must atomically exchange the two files, i.e. both must
 * exist and neither may be deleted.
 *
 * Valid replies:
 *   fuse_reply_err
 *
 * @param req request handle
 * @param parent inode number of the old parent directory
 * @param name old name
 * @param newparent inode number of the new parent directory
 * @param newname new name
 */
/*
renameat2()
       renameat2() has an additional flags argument.  A renameat2() call
       with a zero flags argument is equivalent to renameat().

       The flags argument is a bit mask consisting of zero or more of
       the following flags:

       RENAME_EXCHANGE
              Atomically exchange oldpath and newpath.  Both pathnames
              must exist but may be of different types (e.g., one could
              be a non-empty directory and the other a symbolic link).

       RENAME_NOREPLACE
              Don't overwrite newpath of the rename.  Return an error if
              newpath already exists.

              RENAME_NOREPLACE can't be employed together with
              RENAME_EXCHANGE.

              RENAME_NOREPLACE requires support from the underlying
              filesystem.  Support for various filesystems was added as
              follows:

              *  ext4 (Linux 3.15);

              *  btrfs, tmpfs, and cifs (Linux 3.17);

              *  xfs (Linux 4.0);

              *  Support for many other filesystems was added in Linux
                 4.9, including ext2, minix, reiserfs, jfs, vfat, and
                 bpf.

       RENAME_WHITEOUT (since Linux 3.18)
              This operation makes sense only for overlay/union
              filesystem implementations.

              Specifying RENAME_WHITEOUT creates a "whiteout" object at
              the source of the rename at the same time as performing
              the rename.  The whole operation is atomic, so that if the
              rename succeeds then the whiteout will also have been
              created.

              A "whiteout" is an object that has special meaning in
              union/overlay filesystem constructs.  In these constructs,
              multiple layers exist and only the top one is ever
              modified.  A whiteout on an upper layer will effectively
              hide a matching file in the lower layer, making it appear
              as if the file didn't exist.

              When a file that exists on the lower layer is renamed, the
              file is first copied up (if not already on the upper
              layer) and then renamed on the upper, read-write layer.
              At the same time, the source file needs to be "whiteouted"
              (so that the version of the source file in the lower layer
              is rendered invisible).  The whole operation needs to be
              done atomically.

              When not part of a union/overlay, the whiteout appears as
              a character device with a {0,0} device number.  (Note that
              other union/overlay implementations may employ different
              methods for storing whiteout entries; specifically, BSD
              union mount employs a separate inode type, DT_WHT, which,
              while supported by some filesystems available in Linux,
              such as CODA and XFS, is ignored by the kernel's whiteout
              support code, as of Linux 4.19, at least.)

              RENAME_WHITEOUT requires the same privileges as creating a
              device node (i.e., the CAP_MKNOD capability).

              RENAME_WHITEOUT can't be employed together with
              RENAME_EXCHANGE.

              RENAME_WHITEOUT requires support from the underlying
              filesystem.  Among the filesystems that support it are
              tmpfs (since Linux 3.18), ext4 (since Linux 3.18), XFS
              (since Linux 4.1), f2fs (since Linux 4.2), btrfs (since
              Linux 4.7), and ubifs (since Linux 4.9).
*/
const (
	RenameEmptyFlag = 0
	RenameNoReplace = 1
	RenameExchange  = fs.RENAME_EXCHANGE
	RenameWhiteout  = 3
)

func (wfs *WFS) Rename(cancel <-chan struct{}, in *fuse.RenameIn, oldName string, newName string) (code fuse.Status) {
	if wfs.IsOverQuotaWithUncommitted() {
		return fuse.Status(syscall.ENOSPC)
	}

	if s := checkName(newName); s != fuse.OK {
		return s
	}

	switch in.Flags {
	case RenameEmptyFlag:
	case RenameNoReplace:
	case RenameExchange:
	case RenameWhiteout:
		return fuse.ENOTSUP
	default:
		return fuse.EINVAL
	}

	oldDir, code := wfs.inodeToPath.GetPath(in.NodeId)
	if code != fuse.OK {
		return
	}
	oldPath := oldDir.Child(oldName)
	newDir, code := wfs.inodeToPath.GetPath(in.Newdir)
	if code != fuse.OK {
		return
	}
	newPath := newDir.Child(newName)

	oldEntry, status := wfs.maybeLoadEntry(oldPath)
	if status != fuse.OK {
		return status
	}

	// POSIX: enforce sticky bit on the source directory.
	if oldDirEntry, dirCode := wfs.maybeLoadEntry(oldDir); dirCode == fuse.OK && oldDirEntry != nil && oldDirEntry.Attributes != nil {
		targetUid := uint32(0)
		if oldEntry != nil && oldEntry.Attributes != nil {
			targetUid = oldEntry.Attributes.Uid
		}
		if code := checkStickyBit(oldDirEntry.Attributes.FileMode, oldDirEntry.Attributes.Uid, targetUid, in.Uid); code != fuse.OK {
			return code
		}
	}

	if wormEnforced, _ := wfs.wormEnforcedForEntry(oldPath, oldEntry); wormEnforced {
		return fuse.EPERM
	}

	glog.V(4).Infof("dir Rename %s => %s", oldPath, newPath)

	// Ensure the source file's metadata exists on the filer before renaming.
	// Two cases can leave the entry only in the local cache:
	//   1. deferFilerCreate=true — file handle still open, dirtyMetadata set.
	//   2. writebackCache — close() triggered async flush, handle released.
	// The filer rename will fail with ENOENT unless we flush/wait first.
	if inode, found := wfs.inodeToPath.GetInode(oldPath); found {
		// Case 1: handle still open with deferred metadata — flush synchronously
		// BEFORE any async flush interference.
		if fh, ok := wfs.fhMap.FindFileHandle(inode); ok && fh.dirtyMetadata {
			glog.V(4).Infof("dir Rename %s: flushing deferred metadata before rename", oldPath)
			if flushStatus := wfs.doFlush(fh, oldEntry.Attributes.Uid, oldEntry.Attributes.Gid, false); flushStatus != fuse.OK {
				glog.Warningf("dir Rename %s: flush before rename failed: %v", oldPath, flushStatus)
				return flushStatus
			}
		}
		// Case 2: handle already released, async flush may be in flight.
		// Mark ALL handles for this inode as renamed so the async flush
		// skips old-path metadata creation (which would re-insert the
		// renamed entry into the meta cache after rename events clean it up).
		wfs.fhMap.MarkInodeRenamed(inode)
		wfs.waitForPendingAsyncFlush(inode)
	} else if oldEntry != nil && oldEntry.Attributes != nil && oldEntry.Attributes.Inode != 0 {
		// GetInode failed (Forget already removed the mapping), but the
		// entry's stored inode can still identify pending async flushes.
		inode = oldEntry.Attributes.Inode
		wfs.fhMap.MarkInodeRenamed(inode)
		wfs.waitForPendingAsyncFlush(inode)
	}

	// Acquire DLM locks on both old and new paths to prevent another mount
	// from opening either path for writing during the rename. Lock in
	// sorted order to prevent deadlocks when two mounts rename in opposite
	// directions (A→B vs B→A).
	//
	// Skip the old-path lock if this mount already holds it via an open
	// file handle (otherwise we'd deadlock trying to re-acquire our own lock).
	if wfs.lockClient != nil {
		owner := fmt.Sprintf("mount-%d", wfs.signature)

		// Check if the source file handle already holds a DLM lock on oldPath
		oldPathAlreadyLocked := false
		if sourceInode, found := wfs.inodeToPath.GetInode(oldPath); found {
			if fh, ok := wfs.fhMap.FindFileHandle(sourceInode); ok && fh.dlmLock != nil {
				oldPathAlreadyLocked = true
			}
		}

		// Determine which paths need new DLM locks
		pathsToLock := []string{string(newPath)}
		if !oldPathAlreadyLocked {
			pathsToLock = append(pathsToLock, string(oldPath))
		}
		// Sort for consistent lock ordering
		if len(pathsToLock) == 2 && pathsToLock[0] > pathsToLock[1] {
			pathsToLock[0], pathsToLock[1] = pathsToLock[1], pathsToLock[0]
		}

		for _, p := range pathsToLock {
			dlmLock := wfs.lockClient.NewBlockingLongLivedLock(p, owner, lock_manager.LiveLockTTL)
			defer dlmLock.Stop()
		}
		glog.V(1).Infof("DLM locks acquired for rename %s => %s (oldPathAlreadyLocked=%v)", oldPath, newPath, oldPathAlreadyLocked)
	}

	// update remote filer
	request := &filer_pb.StreamRenameEntryRequest{
		OldDirectory: string(oldDir),
		OldName:      oldName,
		NewDirectory: string(newDir),
		NewName:      newName,
		Signatures:   []int32{wfs.signature},
	}

	ctx := context.Background()
	err := wfs.doRename(ctx, request, oldPath, newPath)
	if err != nil {
		glog.V(0).Infof("Rename %s => %s: %v", oldPath, newPath, err)
		// Map error strings to FUSE status codes. String matching is used
		// instead of raw errno to stay portable across platforms (errno
		// numeric values differ between Linux and macOS).
		msg := err.Error()
		if strings.Contains(msg, "not found") {
			return fuse.Status(syscall.ENOENT)
		} else if strings.Contains(msg, "not empty") {
			return fuse.Status(syscall.ENOTEMPTY)
		} else if strings.Contains(msg, "not directory") {
			return fuse.ENOTDIR
		}
		return fuse.EIO
	}
	wfs.inodeToPath.TouchDirectory(oldDir)
	wfs.inodeToPath.TouchDirectory(newDir)

	return fuse.OK

}

func (wfs *WFS) handleRenameResponse(ctx context.Context, resp *filer_pb.StreamRenameEntryResponse) error {
	// comes from filer StreamRenameEntry, can only be create or delete entry

	glog.V(4).Infof("dir Rename %+v", resp.EventNotification)

	if resp.EventNotification.NewEntry != nil {
		if err := wfs.applyLocalMetadataEvent(ctx, metadataEventFromRenameResponse(resp)); err != nil {
			glog.Warningf("rename apply metadata event: %v", err)
			wfs.inodeToPath.InvalidateChildrenCache(util.FullPath(resp.Directory))
			if resp.EventNotification.NewParentPath != "" {
				wfs.inodeToPath.InvalidateChildrenCache(util.FullPath(resp.EventNotification.NewParentPath))
			}
		}

		oldParent, newParent := util.FullPath(resp.Directory), util.FullPath(resp.EventNotification.NewParentPath)
		oldName, newName := resp.EventNotification.OldEntry.Name, resp.EventNotification.NewEntry.Name

		oldPath := oldParent.Child(oldName)
		newPath := newParent.Child(newName)

		sourceInode, targetInode := wfs.inodeToPath.MovePath(oldPath, newPath)
		if sourceInode != 0 {
			fh, foundFh := wfs.fhMap.FindFileHandle(sourceInode)
			if foundFh {
				if entry := fh.GetEntry(); entry != nil {
					entry.Name = newName
				}
				// Keep the saved handle path current so any flush fallback
				// after Forget uses the post-rename location, not the old one.
				fh.RememberPath(newPath)

				// Migrate the DLM lock from old path to new path so the
				// lock key matches the current file location. Hold the
				// fhLockTable to prevent ReleaseHandle from concurrently
				// stopping the lock during migration.
				if wfs.lockClient != nil {
					fhActiveLock := wfs.fhLockTable.AcquireLock("renameDLM", fh.fh, util.ExclusiveLock)
					if fh.dlmLock != nil {
						owner := fmt.Sprintf("mount-%d", wfs.signature)
						fh.dlmLock.Stop()
						fh.dlmLock = wfs.lockClient.NewBlockingLongLivedLock(
							string(newPath), owner, lock_manager.LiveLockTTL,
						)
						glog.V(1).Infof("DLM lock migrated from %s to %s", oldPath, newPath)
					}
					wfs.fhLockTable.ReleaseLock(fh.fh, fhActiveLock)
				}
			}
			// invalidate attr and data
			// wfs.fuseServer.InodeNotify(sourceInode, 0, -1)
		}
		if targetInode != 0 {
			// invalidate attr and data
			// wfs.fuseServer.InodeNotify(targetInode, 0, -1)
		}

	} else if resp.EventNotification.OldEntry != nil {
		// without new entry, only old entry name exists. This is the second step to delete old entry
		if err := wfs.applyLocalMetadataEvent(ctx, metadataEventFromRenameResponse(resp)); err != nil {
			glog.Warningf("rename apply delete event: %v", err)
			wfs.inodeToPath.InvalidateChildrenCache(util.FullPath(resp.Directory))
		}
	}

	return nil

}
