package pb

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"google.golang.org/grpc"
)

// fakeSubscribeStream plays back a scripted sequence of responses, one per Recv.
type fakeSubscribeStream struct {
	grpc.ClientStream
	responses []*filer_pb.SubscribeMetadataResponse
	delay     time.Duration
	idx       int
}

func (s *fakeSubscribeStream) Recv() (*filer_pb.SubscribeMetadataResponse, error) {
	if s.idx >= len(s.responses) {
		return nil, io.EOF
	}
	if s.idx > 0 && s.delay > 0 {
		// advance the wall clock so AddOffsetFunc's interval gate opens
		time.Sleep(s.delay)
	}
	r := s.responses[s.idx]
	s.idx++
	return r, nil
}

// fakeFilerClient only needs SubscribeMetadata; the embedded nil interface
// covers the rest, which makeSubscribeMetadataFunc never calls.
type fakeFilerClient struct {
	filer_pb.SeaweedFilerClient
	stream *fakeSubscribeStream
}

func (c *fakeFilerClient) SubscribeMetadata(ctx context.Context, in *filer_pb.SubscribeMetadataRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[filer_pb.SubscribeMetadataResponse], error) {
	return c.stream, nil
}

// TestFilerSyncOffsetStaysFreshOnFilteredMarker: on a read-only watched path with
// a busy source, the MaxUnsyncedEvents marker (empty EventNotification, fresh
// timestamp) must be treated as a freshness signal, not fed to the offset path
// where it would pin the gauge to the stale watermark. Wiring mirrors filer.sync.
func TestFilerSyncOffsetStaysFreshOnFilteredMarker(t *testing.T) {
	const oldEventTs = int64(1_000_000_000) // t0: last real synced event (stale)
	nowTs := time.Now().UnixNano()          // current source time
	markerTs := nowTs + int64(time.Second)

	var watermark = oldEventTs // MetadataProcessor.processedTsWatermark

	type gaugeWrite struct {
		src string
		ts  int64
	}
	var timeline []gaugeWrite
	var heartbeatCalls, markerToProcessFn int

	// AddSyncJob drops empty events and does not advance the watermark; the real
	// processor is checked in command.TestMetadataProcessorEmptyMarkerKeepsWatermarkStale.
	realProcessFn := func(resp *filer_pb.SubscribeMetadataResponse) error {
		if filer_pb.IsEmpty(resp) {
			markerToProcessFn++
			return nil
		}
		watermark = resp.TsNs
		return nil
	}
	// offsetFunc publishes the watermark to the gauge (filer_sync.go).
	processEventFn := AddOffsetFunc(realProcessFn, 0, func(counter, lastTsNs int64) error {
		timeline = append(timeline, gaugeWrite{"offset", watermark})
		return nil
	})

	option := &MetadataFollowOption{
		ClientName:     "syncFrom_A_To_B",
		StartTsNs:      oldEventTs,
		EventErrorType: DontLogError,
		OnIdleHeartbeat: func(tsNs int64) {
			heartbeatCalls++
			timeline = append(timeline, gaugeWrite{"heartbeat", tsNs})
		},
	}

	stream := &fakeSubscribeStream{
		delay: 2 * time.Millisecond,
		responses: []*filer_pb.SubscribeMetadataResponse{
			// real create on the watched path, long ago -> watermark = t0
			{Directory: "/watched", TsNs: oldEventTs, EventNotification: &filer_pb.EventNotification{
				NewEntry: &filer_pb.Entry{Name: "file"},
			}},
			// genuine idle heartbeat
			{TsNs: nowTs},
			// MaxUnsyncedEvents marker: empty EventNotification, fresh timestamp
			{TsNs: markerTs, EventNotification: &filer_pb.EventNotification{}},
		},
	}

	if err := makeSubscribeMetadataFunc(option, processEventFn)(&fakeFilerClient{stream: stream}); err != nil {
		t.Fatalf("follow: %v", err)
	}

	t.Logf("gauge timeline: %+v", timeline)

	// the marker fires OnIdleHeartbeat instead of reaching processEventFn
	if markerToProcessFn != 0 {
		t.Errorf("empty marker must not reach processEventFn, got %d calls", markerToProcessFn)
	}
	if heartbeatCalls != 2 {
		t.Errorf("expected OnIdleHeartbeat for both the heartbeat and the marker (2), got %d", heartbeatCalls)
	}
	if option.StartTsNs != markerTs {
		t.Errorf("marker should advance StartTsNs to %d, got %d", markerTs, option.StartTsNs)
	}

	// gauge stays fresh: last write is the marker's timestamp, not the stale watermark
	last := timeline[len(timeline)-1]
	if last.src != "heartbeat" || last.ts != markerTs {
		t.Fatalf("expected final gauge write fresh at %d, got %+v (spike is back if stale %d)", markerTs, last, oldEventTs)
	}
}
