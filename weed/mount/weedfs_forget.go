package mount

import (
	"context"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// Forget is called when the kernel discards entries from its
// dentry cache. This happens on unmount, and when the kernel
// is short on memory. Since it is not guaranteed to occur at
// any moment, and since there is no return value, Forget
// should not do I/O, as there is no channel to report back
// I/O errors.
// from https://github.com/libfuse/libfuse/blob/master/include/fuse_lowlevel.h
/**
 * Forget about an inode
 *
 * This function is called when the kernel removes an inode
 * from its internal caches.
 *
 * The inode's lookup count increases by one for every call to
 * fuse_reply_entry and fuse_reply_create. The nlookup parameter
 * indicates by how much the lookup count should be decreased.
 *
 * Inodes with a non-zero lookup count may receive request from
 * the kernel even after calls to unlink, rmdir or (when
 * overwriting an existing file) rename. Filesystems must handle
 * such requests properly and it is recommended to defer removal
 * of the inode until the lookup count reaches zero. Calls to
 * unlink, rmdir or rename will be followed closely by forget
 * unless the file or directory is open, in which case the
 * kernel issues forget only after the release or releasedir
 * calls.
 *
 * Note that if a file system will be exported over NFS the
 * inodes lifetime must extend even beyond forget. See the
 * generation field in struct fuse_entry_param above.
 *
 * On unmount the lookup count for all inodes implicitly drops
 * to zero. It is not guaranteed that the file system will
 * receive corresponding forget messages for the affected
 * inodes.
 *
 * Valid replies:
 *   fuse_reply_none
 *
 * @param req request handle
 * @param ino the inode number
 * @param nlookup the number of lookups to forget
 */
/*
https://libfuse.github.io/doxygen/include_2fuse__lowlevel_8h.html

int fuse_reply_entry	(	fuse_req_t 	req,
const struct fuse_entry_param * 	e
)
Reply with a directory entry

Possible requests: lookup, mknod, mkdir, symlink, link

Side effects: increments the lookup count on success

*/
func (wfs *WFS) Forget(nodeid, nlookup uint64) {
	wfs.inodeToPath.Forget(nodeid, nlookup, func(dir util.FullPath) {
		wfs.metaCache.DeleteFolderChildren(context.Background(), dir)
	})
	wfs.fhMap.ReleaseByInode(nodeid)
}
