package mount

import (
	"context"
	"fmt"
	"io"
	"strings"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

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
	if wfs.IsOverQuota {
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

	if wormEnforced, _ := wfs.wormEnforcedForEntry(oldPath, oldEntry); wormEnforced {
		return fuse.EPERM
	}

	glog.V(4).Infof("dir Rename %s => %s", oldPath, newPath)

	// update remote filer
	err := wfs.WithFilerClient(true, func(client filer_pb.SeaweedFilerClient) error {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		request := &filer_pb.StreamRenameEntryRequest{
			OldDirectory: string(oldDir),
			OldName:      oldName,
			NewDirectory: string(newDir),
			NewName:      newName,
			Signatures:   []int32{wfs.signature},
		}

		stream, err := client.StreamRenameEntry(ctx, request)
		if err != nil {
			code = fuse.EIO
			return fmt.Errorf("dir AtomicRenameEntry %s => %s : %v", oldPath, newPath, err)
		}

		for {
			resp, recvErr := stream.Recv()
			if recvErr != nil {
				if recvErr == io.EOF {
					break
				} else {
					if strings.Contains(recvErr.Error(), "not empty") {
						code = fuse.Status(syscall.ENOTEMPTY)
					} else if strings.Contains(recvErr.Error(), "not directory") {
						code = fuse.ENOTDIR
					}
					return fmt.Errorf("dir Rename %s => %s receive: %v", oldPath, newPath, recvErr)
				}
			}

			if err = wfs.handleRenameResponse(ctx, resp); err != nil {
				glog.V(0).Infof("dir Rename %s => %s : %v", oldPath, newPath, err)
				return err
			}

		}

		return nil

	})
	if err != nil {
		glog.V(0).Infof("Link: %v", err)
		return
	}

	return fuse.OK

}

func (wfs *WFS) handleRenameResponse(ctx context.Context, resp *filer_pb.StreamRenameEntryResponse) error {
	// comes from filer StreamRenameEntry, can only be create or delete entry

	glog.V(4).Infof("dir Rename %+v", resp.EventNotification)

	if resp.EventNotification.NewEntry != nil {
		// with new entry, the old entry name also exists. This is the first step to create new entry
		newEntry := filer.FromPbEntry(resp.EventNotification.NewParentPath, resp.EventNotification.NewEntry)
		if err := wfs.metaCache.AtomicUpdateEntryFromFiler(ctx, "", newEntry); err != nil {
			return err
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
		if err := wfs.metaCache.AtomicUpdateEntryFromFiler(ctx, util.NewFullPath(resp.Directory, resp.EventNotification.OldEntry.Name), nil); err != nil {
			return err
		}
	}

	return nil

}
