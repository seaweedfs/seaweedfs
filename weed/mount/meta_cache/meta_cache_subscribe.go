package meta_cache

import (
	"context"
	"io"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
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

func SubscribeMetaEvents(mc *MetaCache, selfSignature int32, client filer_pb.FilerClient, dir string, lastTsNs int64, skipSelfEvents bool, onRetry func(lastTsNs int64, err error), followers ...*MetadataFollower) error {

	var prefixes []string
	for _, follower := range followers {
		prefixes = append(prefixes, follower.PathPrefixToWatch)
	}

	processEventFn := func(resp *filer_pb.SubscribeMetadataResponse) error {
		if skipSelfEvents && resp.EventNotification != nil {
			for _, sig := range resp.EventNotification.Signatures {
				if sig == selfSignature {
					glog.V(4).Infof("skip self-originated event %s", resp.Directory)
					return nil
				}
			}
		}
		return mc.ApplyMetadataResponse(context.Background(), resp, SubscriberMetadataResponseApplyOptions)
	}

	prefix := dir
	if !strings.HasSuffix(prefix, "/") {
		prefix = prefix + "/"
	}

	// Read persisted log chunks directly from volume servers, keeping the replay
	// cost off the filer's heap (see LogFileReaderFn below).
	lookupFn := filer.LookupFn(client)
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
		LogFileReaderFn: func(chunks []*filer_pb.FileChunk) (io.ReadCloser, error) {
			return filer.NewChunkStreamReaderFromLookup(context.Background(), lookupFn, chunks), nil
		},
	}
	util.RetryUntil("followMetaUpdates", func() error {
		metadataFollowOption.ClientEpoch++
		return pb.WithFilerClientFollowMetadata(client, metadataFollowOption, mergeProcessors(processEventFn, followers...))
	}, func(err error) bool {
		if onRetry != nil {
			onRetry(metadataFollowOption.StartTsNs, err)
		}
		glog.Errorf("follow metadata updates: %v", err)
		return true
	})

	return nil
}
