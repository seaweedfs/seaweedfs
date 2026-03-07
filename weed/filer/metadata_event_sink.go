package filer

import (
	"context"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

type metadataEventSinkKey struct{}

// MetadataEventSink captures the last metadata event emitted while serving a
// request. It is request-scoped and accessed only by the goroutine handling
// the gRPC call, so no mutex is needed.
type MetadataEventSink struct {
	last *filer_pb.SubscribeMetadataResponse
}

func WithMetadataEventSink(ctx context.Context) (context.Context, *MetadataEventSink) {
	sink := &MetadataEventSink{}
	return context.WithValue(ctx, metadataEventSinkKey{}, sink), sink
}

func metadataEventSinkFromContext(ctx context.Context) *MetadataEventSink {
	if ctx == nil {
		return nil
	}
	sink, _ := ctx.Value(metadataEventSinkKey{}).(*MetadataEventSink)
	return sink
}

// Record stores the event, replacing any previously recorded one.
// Each filer RPC emits at most one NotifyUpdateEvent, so only the last
// event is retained. If an RPC were to emit multiple events, only the
// final one would be returned to the caller.
func (s *MetadataEventSink) Record(event *filer_pb.SubscribeMetadataResponse) {
	if s == nil || event == nil {
		return
	}
	s.last = event
}

func (s *MetadataEventSink) Last() *filer_pb.SubscribeMetadataResponse {
	if s == nil {
		return nil
	}
	return s.last
}
