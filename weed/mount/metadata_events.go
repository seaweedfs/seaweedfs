package mount

import (
	"context"

	"github.com/seaweedfs/seaweedfs/weed/mount/meta_cache"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/protobuf/proto"
)

func (wfs *WFS) applyLocalMetadataEvent(ctx context.Context, event *filer_pb.SubscribeMetadataResponse) error {
	if ctx == nil {
		ctx = context.Background()
	}
	return wfs.metaCache.ApplyMetadataResponseOwned(ctx, event, meta_cache.LocalMetadataResponseApplyOptions)
}

func metadataDeleteEvent(directory, name string) *filer_pb.SubscribeMetadataResponse {
	if name == "" {
		return nil
	}
	return &filer_pb.SubscribeMetadataResponse{
		Directory: directory,
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{Name: name},
		},
	}
}

func metadataUpdateEvent(directory string, entry *filer_pb.Entry) *filer_pb.SubscribeMetadataResponse {
	if entry == nil {
		return nil
	}
	return &filer_pb.SubscribeMetadataResponse{
		Directory: directory,
		EventNotification: &filer_pb.EventNotification{
			OldEntry:      &filer_pb.Entry{Name: entry.Name},
			NewEntry:      proto.Clone(entry).(*filer_pb.Entry),
			NewParentPath: directory,
		},
	}
}

func metadataEventFromRenameResponse(resp *filer_pb.StreamRenameEntryResponse) *filer_pb.SubscribeMetadataResponse {
	if resp == nil || resp.EventNotification == nil {
		return nil
	}
	return &filer_pb.SubscribeMetadataResponse{
		Directory:         resp.Directory,
		EventNotification: proto.Clone(resp.EventNotification).(*filer_pb.EventNotification),
		TsNs:              resp.TsNs,
	}
}
