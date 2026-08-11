package filer

import (
	"context"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// Replay applies a delete and an insert as two separate, non-transactional
// store calls, so a caller that retries Replay after a failure is not
// retrying an atomic unit. This is usually harmless: FilerStoreWrapper's
// InsertEntry has no pre-check, so a retried insert just reapplies both of
// its own sub-steps. DeleteEntry is the exception — FilerStoreWrapper skips
// the call once FindEntry reports the path already gone, so a store whose
// delete removes the primary key before a secondary mutation (e.g. the redis
// families drop the entry key before the parent-directory membership) can
// fail between the two, and a subsequent retry will see "already deleted"
// and never revisit the secondary mutation. The retry then reports overall
// success and the stale membership goes unrepaired and uncounted. This is a
// property of FilerStoreWrapper.DeleteEntry and the affected stores, not
// something a caller of Replay can fix by retrying differently.
func Replay(filerStore FilerStore, resp *filer_pb.SubscribeMetadataResponse) error {
	message := resp.EventNotification
	var oldPath util.FullPath
	var newEntry *Entry
	if message.OldEntry != nil {
		oldPath = util.NewFullPath(resp.Directory, message.OldEntry.Name)
		glog.V(4).Infof("deleting %v", oldPath)
		if err := filerStore.DeleteEntry(context.Background(), oldPath); err != nil {
			return err
		}
	}

	if message.NewEntry != nil {
		dir := resp.Directory
		if message.NewParentPath != "" {
			dir = message.NewParentPath
		}
		key := util.NewFullPath(dir, message.NewEntry.Name)
		glog.V(4).Infof("creating %v", key)
		newEntry = FromPbEntry(dir, message.NewEntry)
		if err := filerStore.InsertEntry(context.Background(), newEntry); err != nil {
			return err
		}
	}

	return nil
}
