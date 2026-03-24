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

	mask &= fuse.R_OK | fuse.W_OK | fuse.X_OK
	if mask == 0 {
		return true
	}

	if callerUid == fileUid {
		return (perm>>6)&mask == mask
	}

	isMember := callerGid == fileGid
	if !isMember {
		u, err := user.LookupId(strconv.Itoa(int(callerUid)))
		if err != nil {
			return (perm & mask) == mask
		}
		groupIDs, err := u.GroupIds()
		if err != nil {
			return (perm & mask) == mask
		}

		fileGidStr := strconv.Itoa(int(fileGid))
		for _, gidStr := range groupIDs {
			if gidStr == fileGidStr {
				isMember = true
				break
			}
		}
	}

	if isMember {
		return (perm>>3)&mask == mask
	}

	return (perm & mask) == mask
}
