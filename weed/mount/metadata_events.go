package mount

import (
	"context"

	"github.com/seaweedfs/seaweedfs/weed/mount/meta_cache"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/protobuf/proto"
)

// filerAckResponse is any mutation response carrying the log position of the
// state it acknowledged.
type filerAckResponse interface {
	GetMetadataEvent() *filer_pb.SubscribeMetadataResponse
	GetLogTsNs() int64
}

// ackVersionTsNs is the log position an ack confirmed: its event's timestamp,
// or the response's fence when the ack carried no event.
func ackVersionTsNs(resp filerAckResponse) int64 {
	if ts := resp.GetMetadataEvent().GetTsNs(); ts != 0 {
		return ts
	}
	return resp.GetLogTsNs()
}

func (wfs *WFS) applyLocalMetadataEvent(ctx context.Context, event *filer_pb.SubscribeMetadataResponse) error {
	if ctx == nil {
		ctx = context.Background()
	}
	return wfs.metaCache.ApplyMetadataResponseOwned(ctx, event, meta_cache.LocalMetadataResponseApplyOptions)
}

func (wfs *WFS) applyLocalMetadataEventAsync(event *filer_pb.SubscribeMetadataResponse) {
	wfs.metaCache.ApplyMetadataResponseOwnedAsync(event, meta_cache.LocalMetadataResponseApplyOptions)
}

func metadataDeleteEvent(directory, name string, isDirectory bool) *filer_pb.SubscribeMetadataResponse {
	if name == "" {
		return nil
	}
	return &filer_pb.SubscribeMetadataResponse{
		Directory: directory,
		EventNotification: &filer_pb.EventNotification{
			OldEntry: &filer_pb.Entry{Name: name, IsDirectory: isDirectory},
		},
	}
}

func metadataCreateEvent(directory string, entry *filer_pb.Entry) *filer_pb.SubscribeMetadataResponse {
	if entry == nil {
		return nil
	}
	return &filer_pb.SubscribeMetadataResponse{
		Directory: directory,
		EventNotification: &filer_pb.EventNotification{
			NewEntry:      proto.Clone(entry).(*filer_pb.Entry),
			NewParentPath: directory,
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
