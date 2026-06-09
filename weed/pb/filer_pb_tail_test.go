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

// TestFilerSyncBatchedFreshnessSignalDoesNotCrash: while catching up after a
// peer outage the server folds a backlog into one batched response: the first
// event in the top-level fields, the rest in resp.Events. The drain can pull an
// idle heartbeat (nil EventNotification) into that tail. The batched tail must
// get the same freshness-signal handling as the envelope, else processEventFn
// (filer.sync's AddSyncJob) nil-derefs in IsEmpty. The processEventFn here
// mirrors that first call.
func TestFilerSyncBatchedFreshnessSignalDoesNotCrash(t *testing.T) {
	const ts1 = int64(1_000_000_000)      // envelope: real create
	const ts2 = int64(1_000_000_001)      // batched: real create
	const markerTs = int64(1_000_000_002) // batched: MaxUnsyncedEvents marker (empty entry)
	hbTs := time.Now().UnixNano()         // batched: idle heartbeat (nil EventNotification), fresh

	var realEvents []int64
	var heartbeatTs []int64

	// Mirrors AddSyncJob: IsEmpty nil-derefs on a nil EventNotification.
	processEventFn := func(resp *filer_pb.SubscribeMetadataResponse) error {
		if filer_pb.IsEmpty(resp) {
			return nil
		}
		realEvents = append(realEvents, resp.TsNs)
		return nil
	}

	option := &MetadataFollowOption{
		ClientName:     "syncFrom_A_To_B",
		StartTsNs:      ts1,
		EventErrorType: DontLogError,
		OnIdleHeartbeat: func(tsNs int64) {
			heartbeatTs = append(heartbeatTs, tsNs)
		},
	}

	// One batched response: envelope plus a tail mixing a real event, a marker,
	// and a fresh heartbeat, as the server emits while draining a backlog. The
	// heartbeat is last and carries the largest timestamp on purpose.
	stream := &fakeSubscribeStream{
		responses: []*filer_pb.SubscribeMetadataResponse{{
			Directory:         "/watched",
			TsNs:              ts1,
			EventNotification: &filer_pb.EventNotification{NewEntry: &filer_pb.Entry{Name: "a"}},
			Events: []*filer_pb.SubscribeMetadataResponse{
				{Directory: "/watched", TsNs: ts2, EventNotification: &filer_pb.EventNotification{NewEntry: &filer_pb.Entry{Name: "b"}}},
				{TsNs: markerTs, EventNotification: &filer_pb.EventNotification{}}, // marker: empty entry
				{TsNs: hbTs}, // idle heartbeat: nil EventNotification
			},
		}},
	}

	if err := makeSubscribeMetadataFunc(option, processEventFn)(&fakeFilerClient{stream: stream}); err != nil {
		t.Fatalf("follow: %v", err)
	}

	// Both real events reached processEventFn; neither freshness signal did.
	if len(realEvents) != 2 || realEvents[0] != ts1 || realEvents[1] != ts2 {
		t.Errorf("expected real events [%d %d], got %v", ts1, ts2, realEvents)
	}
	// The batched marker and heartbeat both fired OnIdleHeartbeat.
	if len(heartbeatTs) != 2 || heartbeatTs[0] != markerTs || heartbeatTs[1] != hbTs {
		t.Errorf("expected heartbeats [%d %d], got %v", markerTs, hbTs, heartbeatTs)
	}
	// The marker advances the resume cursor; the heartbeat does not, even though
	// it is last and carries the largest timestamp. StartTsNs ends at the marker.
	if option.StartTsNs != markerTs {
		t.Errorf("expected StartTsNs %d (marker), got %d (heartbeat must not advance the cursor)", markerTs, option.StartTsNs)
	}
}
