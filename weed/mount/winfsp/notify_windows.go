package winfsp

import (
	cgofuse "github.com/winfsp/cgofuse/fuse"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mount/meta_cache"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// notifier turns the mount's metadata events into the notifications Windows
// listens for. Nothing invalidates a Windows client's cache from this side, so
// without it a change made by another mount, the S3 gateway or the filer API
// stays invisible until the user refreshes by hand.
type notifier struct {
	host      *cgofuse.FileSystemHost
	mountRoot util.FullPath
}

// notify reports one applied event. Windows wants the path relative to the
// mount, and an action describing what happened to it.
func (n *notifier) notify(invalidation meta_cache.EntryInvalidation) {
	if n.host == nil {
		return
	}
	// A rename arrives as two invalidations, one vacating the old path and one
	// describing the new, so reporting RenamedTo here as well would send the
	// destination twice — and as a create even when a directory moved.
	if path, ok := relativeToMount(n.mountRoot, invalidation.Path); ok {
		n.send(path, n.action(invalidation))
	}
}

func (n *notifier) send(path string, action uint32) {
	if !n.host.Notify(path, action) {
		// A rejected notification only costs a stale view until the client
		// looks again, so it is not worth failing an operation over.
		glog.V(4).Infof("winfsp notify %s action %d rejected", path, action)
	}
}

// action picks what to tell Windows happened. It cannot distinguish a created
// entry from a modified one, and Windows treats an unexpected create on an
// existing name as a refresh, so create is the safe report.
func (n *notifier) action(invalidation meta_cache.EntryInvalidation) uint32 {
	if invalidation.Deleted || invalidation.Entry == nil {
		// The entry is gone, so only WasDirectory still says what it was, and
		// Windows watches directory and file removals through different
		// filters.
		if invalidation.WasDirectory {
			return cgofuse.NOTIFY_RMDIR
		}
		return cgofuse.NOTIFY_UNLINK
	}
	if invalidation.Entry.GetIsDirectory() {
		return cgofuse.NOTIFY_MKDIR
	}
	return cgofuse.NOTIFY_CREATE
}
