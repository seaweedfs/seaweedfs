package filer

import (
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

// ensureEntryInode derives a stable inode the same way the FUSE mount does, so
// the filer-stored value matches what a mount would otherwise compute and no
// per-object reverse index is required. Hard links hash their shared
// HardLinkId, so every link resolves to one inode; other entries hash the path
// and creation time.
func (f *Filer) ensureEntryInode(entry *Entry) {
	if entry == nil || entry.Attr.Inode != 0 {
		return
	}
	if entry.Attr.Crtime.IsZero() {
		entry.Attr.Crtime = time.Now()
	}
	if len(entry.HardLinkId) > 0 {
		entry.Attr.Inode = uint64(util.HashStringToLong(string(entry.HardLinkId)))
		return
	}
	entry.Attr.Inode = entry.FullPath.AsInode(entry.Attr.Crtime.Unix())
}
