package filer

import (
	"context"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

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

// ParallelProcessDirectoryStructure processes each entry in parallel, and also ensure parent directories are processed first.
// This also assumes the parent directories are in the entryChan already.
func ParallelProcessDirectoryStructure(entryChan chan *Entry, concurrency int, eachEntryFn func(entry *Entry) error) (firstErr error) {

	executors := util.NewLimitedConcurrentExecutor(concurrency)

	var wg sync.WaitGroup
	for entry := range entryChan {
		wg.Add(1)
		if entry.IsDirectory() {
			func() {
				defer wg.Done()
				if err := eachEntryFn(entry); err != nil {
					if firstErr == nil {
						firstErr = err
					}
				}
			}()
		} else {
			executors.Execute(func() {
				defer wg.Done()
				if err := eachEntryFn(entry); err != nil {
					if firstErr == nil {
						firstErr = err
					}
				}
			})
		}
		if firstErr != nil {
			break
		}
	}
	wg.Wait()
	return
}
