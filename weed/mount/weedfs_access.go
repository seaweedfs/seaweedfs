package mount

import (
	"os/user"
	"strconv"

	"github.com/seaweedfs/go-fuse/v2/fuse"
)

/**
 * Check file access permissions
 *
 * This will be called for the access() system call.  If the
 * 'default_permissions' mount option is given, this method is not
 * called.
 *
 * This method is not called under Linux kernel versions 2.4.x
 */
func (wfs *WFS) Access(cancel <-chan struct{}, input *fuse.AccessIn) (code fuse.Status) {
	_, _, entry, code := wfs.maybeReadEntry(input.NodeId)
	if code != fuse.OK {
		return code
	}
	if entry == nil || entry.Attributes == nil {
		return fuse.EIO
	}
	if hasAccess(input.Uid, input.Gid, entry.Attributes.Uid, entry.Attributes.Gid, entry.Attributes.FileMode, input.Mask) {
		return fuse.OK
	}
	return fuse.EACCES
}

func hasAccess(callerUid, callerGid, fileUid, fileGid uint32, perm uint32, mask uint32) bool {
	if callerUid == 0 {
		return true
	}

	mask &= 7
	if mask == 0 {
		return true
	}

	if callerUid == fileUid && perm&(mask<<6) != 0 {
		return true
	}
	if callerGid == fileGid && perm&(mask<<3) != 0 {
		return true
	}
	if perm&mask != 0 {
		return true
	}
	if perm&(mask<<3) == 0 {
		return false
	}

	u, err := user.LookupId(strconv.Itoa(int(callerUid)))
	if err != nil {
		return false
	}
	groupIDs, err := u.GroupIds()
	if err != nil {
		return false
	}

	fileGidStr := strconv.Itoa(int(fileGid))
	for _, gidStr := range groupIDs {
		if gidStr == fileGidStr {
			return true
		}
	}
	return false
}
