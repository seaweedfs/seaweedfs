package filer

import (
	"context"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// Replay applies the delete and the insert as separate, non-transactional
// store calls, so retrying it after a partial failure is not atomic.
//
// A replayed event can be older than local state: a peer resuming from a
// stale offset replays everything since the offset, and two writes through
// different filers race within the replication lag. Apply each path's change
// only when no newer local entry exists, so a late event cannot revert or
// resurrect entries. Entries carry no commit timestamp of their own; mtime
// is set to write time on local commits and is the closest proxy.
func Replay(filerStore FilerStore, resp *filer_pb.SubscribeMetadataResponse) error {
	message := resp.EventNotification
	dir := resp.Directory
	if message.NewParentPath != "" {
		dir = message.NewParentPath
	}
	var oldPath, newPath util.FullPath
	if message.OldEntry != nil {
		oldPath = util.NewFullPath(resp.Directory, message.OldEntry.Name)
	}
	if message.NewEntry != nil {
		newPath = util.NewFullPath(dir, message.NewEntry.Name)
	}
	samePath := oldPath != "" && oldPath == newPath
	if samePath && replayLocalNewer(filerStore, oldPath, resp.TsNs) {
		return nil
	}

	if message.OldEntry != nil && (samePath || !replayLocalNewer(filerStore, oldPath, resp.TsNs)) {
		glog.V(4).Infof("deleting %v", oldPath)
		if err := filerStore.DeleteEntry(context.Background(), oldPath); err != nil {
			return err
		}
	}

	if message.NewEntry != nil && (samePath || !replayLocalNewer(filerStore, newPath, resp.TsNs)) {
		glog.V(4).Infof("creating %v", newPath)
		newEntry := FromPbEntry(dir, message.NewEntry)
		if err := filerStore.InsertEntry(context.Background(), newEntry); err != nil {
			return err
		}
	}

	return nil
}

// replayLocalNewer reports whether the local store already holds a version of
// path committed after the event's timestamp.
func replayLocalNewer(store FilerStore, path util.FullPath, tsNs int64) bool {
	if tsNs <= 0 {
		return false
	}
	existing, err := store.FindEntry(context.Background(), path)
	return err == nil && existing != nil && existing.Mtime.UnixNano() > tsNs
}
