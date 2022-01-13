package filesys

import (
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/fuse"
)

func checkPermission(entry *filer_pb.Entry, uid, gid uint32, isWrite bool) error {
	if uid == 0 || gid == 0 {
		return nil
	}
	if entry == nil {
		return nil
	}
	if entry.Attributes == nil {
		return nil
	}
	attr := entry.Attributes
	if attr.Uid == uid {
		if isWrite {
			if attr.FileMode&0200 > 0 {
				return nil
			} else {
				return fuse.EPERM
			}
		} else {
			if attr.FileMode&0400 > 0 {
				return nil
			} else {
				return fuse.EPERM
			}
		}
	} else if attr.Gid == gid {
		if isWrite {
			if attr.FileMode&0020 > 0 {
				return nil
			} else {
				return fuse.EPERM
			}
		} else {
			if attr.FileMode&0040 > 0 {
				return nil
			} else {
				return fuse.EPERM
			}
		}
	} else {
		if isWrite {
			if attr.FileMode&0002 > 0 {
				return nil
			} else {
				return fuse.EPERM
			}
		} else {
			if attr.FileMode&0004 > 0 {
				return nil
			} else {
				return fuse.EPERM
			}
		}
	}

}
