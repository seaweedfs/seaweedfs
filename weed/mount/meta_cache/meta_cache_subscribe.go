package meta_cache

import (
	"context"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"strings"
)

type MetadataFollower struct {
	PathPrefixToWatch string
	ProcessEventFn    func(resp *filer_pb.SubscribeMetadataResponse) error
}

func mergeProcessors(mainProcessor func(resp *filer_pb.SubscribeMetadataResponse) error, followers ...*MetadataFollower) func(resp *filer_pb.SubscribeMetadataResponse) error {
	return func(resp *filer_pb.SubscribeMetadataResponse) error {

		// build the full path
		entry := resp.EventNotification.NewEntry
		if entry == nil {
			entry = resp.EventNotification.OldEntry
		}
		if entry != nil {
			dir := resp.Directory
			if resp.EventNotification.NewParentPath != "" {
				dir = resp.EventNotification.NewParentPath
			}
			fp := util.NewFullPath(dir, entry.Name)

			for _, follower := range followers {
				if strings.HasPrefix(string(fp), follower.PathPrefixToWatch) {
					if err := follower.ProcessEventFn(resp); err != nil {
						return err
					}
				}
			}
		}
		return mainProcessor(resp)
	}
}

func SubscribeMetaEvents(mc *MetaCache, selfSignature int32, client filer_pb.FilerClient, dir string, lastTsNs int64, followers ...*MetadataFollower) error {

	var prefixes []string
	for _, follower := range followers {
		prefixes = append(prefixes, follower.PathPrefixToWatch)
	}

	processEventFn := func(resp *filer_pb.SubscribeMetadataResponse) error {
		message := resp.EventNotification

		for _, sig := range message.Signatures {
			if sig == selfSignature && selfSignature != 0 {
				return nil
			}
		}

		dir := resp.Directory
		var oldPath util.FullPath
		var newEntry *filer.Entry
		if message.OldEntry != nil {
			oldPath = util.NewFullPath(dir, message.OldEntry.Name)
			glog.V(4).Infof("deleting %v", oldPath)
		}

		if message.NewEntry != nil {
			if message.NewParentPath != "" {
				dir = message.NewParentPath
			}
			key := util.NewFullPath(dir, message.NewEntry.Name)
			glog.V(4).Infof("creating %v", key)
			newEntry = filer.FromPbEntry(dir, message.NewEntry)
		}
		err := mc.AtomicUpdateEntryFromFiler(context.Background(), oldPath, newEntry)
		if err == nil {
			if message.OldEntry != nil && message.NewEntry != nil {
				oldKey := util.NewFullPath(resp.Directory, message.OldEntry.Name)
				mc.invalidateFunc(oldKey, message.OldEntry)
				if message.OldEntry.Name != message.NewEntry.Name {
					newKey := util.NewFullPath(dir, message.NewEntry.Name)
					mc.invalidateFunc(newKey, message.NewEntry)
				}
			} else if filer_pb.IsCreate(resp) {
				// no need to invalidate
			} else if filer_pb.IsDelete(resp) {
				oldKey := util.NewFullPath(resp.Directory, message.OldEntry.Name)
				mc.invalidateFunc(oldKey, message.OldEntry)
			}
		}

		return err

	}

	prefix := dir
	if !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}

	metadataFollowOption := &pb.MetadataFollowOption{
		ClientName:             "mount",
		ClientId:               selfSignature,
		ClientEpoch:            1,
		SelfSignature:          selfSignature,
		PathPrefix:             prefix,
		AdditionalPathPrefixes: prefixes,
		DirectoriesToWatch:     nil,
		StartTsNs:              lastTsNs,
		StopTsNs:               0,
		EventErrorType:         pb.FatalOnError,
	}
	util.RetryUntil("followMetaUpdates", func() error {
		metadataFollowOption.ClientEpoch++
		return pb.WithFilerClientFollowMetadata(client, metadataFollowOption, mergeProcessors(processEventFn, followers...))
	}, func(err error) bool {
		glog.Errorf("follow metadata updates: %v", err)
		return true
	})

	return nil
}
