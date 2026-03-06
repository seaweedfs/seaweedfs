package filer

import (
	"context"
	"sync"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/protobuf/proto"
)

type metadataEventSinkKey struct{}

// MetadataEventSink captures the last metadata event emitted while serving a request.
type MetadataEventSink struct {
	mu   sync.Mutex
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

func (s *MetadataEventSink) Record(event *filer_pb.SubscribeMetadataResponse) {
	if s == nil || event == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.last = proto.Clone(event).(*filer_pb.SubscribeMetadataResponse)
}

func (s *MetadataEventSink) Last() *filer_pb.SubscribeMetadataResponse {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.last == nil {
		return nil
	}
	return proto.Clone(s.last).(*filer_pb.SubscribeMetadataResponse)
}
