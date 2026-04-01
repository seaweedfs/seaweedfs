package mount

import (
	"os/user"
	"strconv"
	"syscall"

	"github.com/seaweedfs/go-fuse/v2/fuse"
	"github.com/seaweedfs/seaweedfs/weed/glog"
)

var lookupSupplementaryGroupIDs = func(callerUid uint32) ([]string, error) {
	u, err := user.LookupId(strconv.Itoa(int(callerUid)))
	if err != nil {
		glog.Warningf("hasAccess: user.LookupId for uid %d failed: %v", callerUid, err)
		return nil, err
	}
	groupIDs, err := u.GroupIds()
	if err != nil {
		glog.Warningf("hasAccess: u.GroupIds for uid %d failed: %v", callerUid, err)
		return nil, err
	}
	return groupIDs, nil
}

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
	// Map entry uid/gid from filer-space to local-space so the permission
	// check compares like with like (caller uid/gid from FUSE are local).
	fileUid, fileGid := entry.Attributes.Uid, entry.Attributes.Gid
	if wfs.option.UidGidMapper != nil {
		fileUid, fileGid = wfs.option.UidGidMapper.FilerToLocal(fileUid, fileGid)
	}
	if hasAccess(input.Uid, input.Gid, fileUid, fileGid, entry.Attributes.FileMode, input.Mask) {
		return fuse.OK
	}
	return fuse.EACCES
}

func hasAccess(callerUid, callerGid, fileUid, fileGid uint32, perm uint32, mask uint32) bool {
	mask &= fuse.R_OK | fuse.W_OK | fuse.X_OK
	if mask == 0 {
		return true
	}
	if callerUid == 0 {
		return mask&fuse.X_OK == 0 || perm&0o111 != 0
	}

	if callerUid == fileUid {
		return (perm>>6)&mask == mask
	}

	isMember := callerGid == fileGid
	if !isMember {
		groupIDs, err := lookupSupplementaryGroupIDs(callerUid)
		if err != nil {
			// Cannot determine group membership; require both group and
			// other permission classes to satisfy the mask so we never
			// overgrant when the lookup fails.
			groupMatch := ((perm >> 3) & mask) == mask
			otherMatch := (perm & mask) == mask
			return groupMatch && otherMatch
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

// openFlagsToAccessMask converts open(2) flags to an access permission mask.
func openFlagsToAccessMask(flags uint32) uint32 {
	switch flags & uint32(syscall.O_ACCMODE) {
	case syscall.O_WRONLY:
		return fuse.W_OK
	case syscall.O_RDWR:
		return fuse.R_OK | fuse.W_OK
	default: // O_RDONLY
		return fuse.R_OK
	}
}
