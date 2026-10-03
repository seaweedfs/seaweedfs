package pb

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
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

	// This consumer sets no resume callback, so markers keep moving StartTsNs
	// and never reach processEventFn; the callback path is checked in
	// TestFilerSyncMarkerReachesCallbackConsumer.
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

// TestFilerSyncResumeFromProcessedWatermarkOnReconnect verifies that when GetResumeTsNs is
// configured, reconnection uses the processed watermark instead of skipping ahead to the
// latest received timestamp.
func TestFilerSyncResumeFromProcessedWatermarkOnReconnect(t *testing.T) {
	const initialTs = int64(100)
	const watermarkTs = int64(200)
	const latestStreamTs = int64(500)

	var capturedSinceNs int64
	recordingClient := &recordingFilerClient{
		onSubscribe: func(req *filer_pb.SubscribeMetadataRequest) {
			capturedSinceNs = req.SinceNs
		},
		stream: &fakeSubscribeStream{
			responses: []*filer_pb.SubscribeMetadataResponse{
				{
					Directory:         "/watched",
					TsNs:              latestStreamTs,
					EventNotification: &filer_pb.EventNotification{NewEntry: &filer_pb.Entry{Name: "file"}},
				},
			},
		},
	}

	option := &MetadataFollowOption{
		ClientName: "syncFrom_A_To_B",
		StartTsNs:  initialTs,
		GetResumeTsNs: func() int64 {
			return watermarkTs
		},
	}

	processFn := func(resp *filer_pb.SubscribeMetadataResponse) error {
		return nil
	}

	fn := makeSubscribeMetadataFunc(option, processFn)
	if err := fn(recordingClient); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if capturedSinceNs != watermarkTs {
		t.Fatalf("expected subscribe SinceNs to be watermark %d, got %d", watermarkTs, capturedSinceNs)
	}
	// StartTsNs must not be mutated when GetResumeTsNs is set
	if option.StartTsNs != initialTs {
		t.Fatalf("expected option.StartTsNs to remain %d, got %d", initialTs, option.StartTsNs)
	}
}

// TestFilerSyncDoesNotAdvanceStartTsNsOnProcessError verifies that a failing synchronous
// processEventFn does not advance option.StartTsNs past the failed event.
func TestFilerSyncDoesNotAdvanceStartTsNsOnProcessError(t *testing.T) {
	const initialTs = int64(100)
	const failedTs = int64(200)

	option := &MetadataFollowOption{
		ClientName:     "syncFrom_A_To_B",
		StartTsNs:      initialTs,
		EventErrorType: TrivialOnError,
	}

	stream := &fakeSubscribeStream{
		responses: []*filer_pb.SubscribeMetadataResponse{
			{
				Directory:         "/watched",
				TsNs:              failedTs,
				EventNotification: &filer_pb.EventNotification{NewEntry: &filer_pb.Entry{Name: "bad"}},
			},
		},
	}

	processFn := func(resp *filer_pb.SubscribeMetadataResponse) error {
		return io.ErrUnexpectedEOF
	}

	fn := makeSubscribeMetadataFunc(option, processFn)
	_ = fn(&fakeFilerClient{stream: stream})

	if option.StartTsNs != initialTs {
		t.Fatalf("expected StartTsNs to stay at %d on error, got %d", initialTs, option.StartTsNs)
	}
}

type recordingFilerClient struct {
	filer_pb.SeaweedFilerClient
	onSubscribe func(req *filer_pb.SubscribeMetadataRequest)
	stream      *fakeSubscribeStream
}

func (c *recordingFilerClient) SubscribeMetadata(ctx context.Context, in *filer_pb.SubscribeMetadataRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[filer_pb.SubscribeMetadataResponse], error) {
	if c.onSubscribe != nil {
		c.onSubscribe(in)
	}
	return c.stream, nil
}

// RetryForeverOnError resolves a failure inside handleErr, so the cursor must
// still move past the recovered event instead of replaying it on reconnect.
func TestFilerSyncRecoveredEventAdvancesCursor(t *testing.T) {
	var calls int
	processFn := func(resp *filer_pb.SubscribeMetadataResponse) error {
		calls++
		if calls == 1 {
			return io.ErrUnexpectedEOF
		}
		return nil
	}
	option := &MetadataFollowOption{
		ClientName:     "syncFrom_A_To_B",
		StartTsNs:      100,
		EventErrorType: RetryForeverOnError,
	}
	stream := &fakeSubscribeStream{
		responses: []*filer_pb.SubscribeMetadataResponse{
			{Directory: "/watched", TsNs: 300, EventNotification: &filer_pb.EventNotification{
				NewEntry: &filer_pb.Entry{Name: "file"},
			}},
		},
	}
	if err := makeSubscribeMetadataFunc(option, processFn)(&fakeFilerClient{stream: stream}); err != nil {
		t.Fatalf("follow: %v", err)
	}
	if calls != 2 {
		t.Fatalf("expected the event retried once (2 calls), got %d", calls)
	}
	if option.StartTsNs != 300 {
		t.Fatalf("expected StartTsNs 300 after the retry succeeded, got %d", option.StartTsNs)
	}
}

// A consumer with a resume callback keeps its cursor in its processed
// watermark, so a filtered-progress marker is handed to processEventFn (which
// can count it as processed) instead of mutating StartTsNs.
func TestFilerSyncMarkerReachesCallbackConsumer(t *testing.T) {
	var markers, events int
	option := &MetadataFollowOption{
		ClientName: "syncFrom_A_To_B",
		StartTsNs:  100,
		GetResumeTsNs: func() int64 {
			return 100
		},
	}
	stream := &fakeSubscribeStream{
		responses: []*filer_pb.SubscribeMetadataResponse{
			{Directory: "/watched", TsNs: 300, EventNotification: &filer_pb.EventNotification{
				NewEntry: &filer_pb.Entry{Name: "file"},
			}},
			{TsNs: 500, EventNotification: &filer_pb.EventNotification{}},
		},
	}
	fn := makeSubscribeMetadataFunc(option, func(resp *filer_pb.SubscribeMetadataResponse) error {
		if filer_pb.IsEmpty(resp) {
			markers++
		} else {
			events++
		}
		return nil
	})
	if err := fn(&fakeFilerClient{stream: stream}); err != nil {
		t.Fatalf("follow: %v", err)
	}
	if markers != 1 || events != 1 {
		t.Fatalf("expected 1 marker and 1 event at processEventFn, got %d and %d", markers, events)
	}
	if option.StartTsNs != 100 {
		t.Fatalf("callback consumer must not mutate StartTsNs, got %d", option.StartTsNs)
	}
}

func TestFilerSyncMarkerCallbackRetries(t *testing.T) {
	var calls int
	option := &MetadataFollowOption{
		StartTsNs:      100,
		EventErrorType: RetryForeverOnError,
		GetResumeTsNs:  func() int64 { return 100 },
	}
	stream := &fakeSubscribeStream{responses: []*filer_pb.SubscribeMetadataResponse{
		{TsNs: 500, EventNotification: &filer_pb.EventNotification{}},
	}}
	fn := makeSubscribeMetadataFunc(option, func(resp *filer_pb.SubscribeMetadataResponse) error {
		calls++
		if calls == 1 {
			return io.ErrUnexpectedEOF
		}
		return nil
	})
	if err := fn(&fakeFilerClient{stream: stream}); err != nil {
		t.Fatalf("follow: %v", err)
	}
	if calls != 2 || option.StartTsNs != 100 {
		t.Fatalf("calls = %d, cursor = %d; want 2 calls and unchanged cursor 100", calls, option.StartTsNs)
	}
}

// Each subscribe call re-reads the callback, so a reconnect after the consumer
// made progress resumes from the newer watermark.
func TestFilerSyncReconnectReadsWatermarkEachSubscribe(t *testing.T) {
	var watermark atomic.Int64
	watermark.Store(100)
	var sinceNs []int64
	var mu sync.Mutex
	stream := &fakeSubscribeStream{
		responses: []*filer_pb.SubscribeMetadataResponse{
			{Directory: "/watched", TsNs: 300, EventNotification: &filer_pb.EventNotification{
				NewEntry: &filer_pb.Entry{Name: "file"},
			}},
		},
	}
	client := &recordingFilerClient{
		onSubscribe: func(req *filer_pb.SubscribeMetadataRequest) {
			mu.Lock()
			sinceNs = append(sinceNs, req.SinceNs)
			mu.Unlock()
		},
		stream: stream,
	}
	option := &MetadataFollowOption{
		ClientName: "syncFrom_A_To_B",
		StartTsNs:  100,
		GetResumeTsNs: func() int64 {
			return watermark.Load()
		},
	}
	fn := makeSubscribeMetadataFunc(option, func(resp *filer_pb.SubscribeMetadataResponse) error {
		watermark.Store(resp.TsNs)
		return nil
	})
	if err := fn(client); err != nil {
		t.Fatalf("first subscribe: %v", err)
	}
	client.stream = &fakeSubscribeStream{}
	if err := fn(client); err != nil {
		t.Fatalf("resubscribe: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(sinceNs) != 2 || sinceNs[0] != 100 || sinceNs[1] != 300 {
		t.Fatalf("expected subscribes at 100 then 300, got %v", sinceNs)
	}
}
