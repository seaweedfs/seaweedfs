package weed_server

import (
	"context"
	"sync/atomic"

	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
)

type recordingStream struct {
	mu   sync.Mutex
	slow time.Duration
	msgs []*filer_pb.SubscribeMetadataResponse
}

func (s *recordingStream) Send(m *filer_pb.SubscribeMetadataResponse) error {
	if s.slow > 0 {
		time.Sleep(s.slow)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// Clone-ish: the sender clears Events after sending, so keep our own view.
	copied := &filer_pb.SubscribeMetadataResponse{
		TsNs:              m.TsNs,
		Directory:         m.Directory,
		EventNotification: m.EventNotification,
		LogFileRefs:       m.LogFileRefs,
		Events:            append([]*filer_pb.SubscribeMetadataResponse(nil), m.Events...),
	}
	s.msgs = append(s.msgs, copied)
	return nil
}

func (s *recordingStream) snapshot() []*filer_pb.SubscribeMetadataResponse {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*filer_pb.SubscribeMetadataResponse(nil), s.msgs...)
}

// TestPipelinedSenderRefsNeverBatched pins the wire rules the client depends
// on: a refs message must arrive solo - the client recognizes refs by the
// top-level field and skips the rest of the response, so a refs envelope would
// drop its Events tail, and refs inside Events would be applied as an empty
// event. Everything must arrive, in order, whatever the batcher does.
func TestPipelinedSenderRefsNeverBatched(t *testing.T) {
	stream := &recordingStream{slow: 2 * time.Millisecond} // let the queue back up so batching engages
	sender := newPipelinedSender(context.Background(), stream, 64, true, "local")

	oldTs := time.Now().Add(-time.Hour).UnixNano() // far behind: the batch heuristic fires
	var wantOrder []string
	send := func(kind string, m *filer_pb.SubscribeMetadataResponse) {
		wantOrder = append(wantOrder, kind)
		if err := sender.Send(m); err != nil {
			t.Fatalf("send %s: %v", kind, err)
		}
	}
	event := func(i int) *filer_pb.SubscribeMetadataResponse {
		return &filer_pb.SubscribeMetadataResponse{
			TsNs:              oldTs + int64(i),
			EventNotification: &filer_pb.EventNotification{NewEntry: &filer_pb.Entry{Name: fmt.Sprintf("e%d", i)}},
		}
	}
	refs := func() *filer_pb.SubscribeMetadataResponse {
		return &filer_pb.SubscribeMetadataResponse{LogFileRefs: []*filer_pb.LogFileChunkRef{{FilerId: "a"}}}
	}

	// Interleave backlog events with refs so refs land both between batches
	// and mid-drain.
	for i := 0; i < 30; i++ {
		send("event", event(i))
		if i%7 == 3 {
			send("refs", refs())
		}
	}
	if err := sender.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	var gotOrder []string
	for _, m := range stream.snapshot() {
		if len(m.LogFileRefs) > 0 {
			if len(m.Events) > 0 {
				t.Fatal("a refs envelope carried an Events tail; the client drops that tail")
			}
			if m.EventNotification != nil {
				t.Fatal("a refs message doubled as an event envelope")
			}
			gotOrder = append(gotOrder, "refs")
			continue
		}
		gotOrder = append(gotOrder, "event")
		for _, e := range m.Events {
			if len(e.LogFileRefs) > 0 {
				t.Fatal("refs packed inside Events; the client applies that as an empty event")
			}
			gotOrder = append(gotOrder, "event")
		}
	}
	if fmt.Sprint(gotOrder) != fmt.Sprint(wantOrder) {
		t.Fatalf("delivery order/count changed:\n got %v\nwant %v", gotOrder, wantOrder)
	}
}

type blockedStream struct {
	release chan struct{}
	entered chan struct{}
	sends   atomic.Int32
}

func (s *blockedStream) Send(*filer_pb.SubscribeMetadataResponse) error {
	s.sends.Add(1)
	select {
	case s.entered <- struct{}{}:
	default:
	}
	<-s.release
	return nil
}

// A subscriber that stops reading wedges stream.Send. The subscription
// context must release Send and Close, so the handler can return and end the
// stream during filer shutdown.
func TestPipelinedSenderUnblocksOnCancel(t *testing.T) {
	stream := &blockedStream{release: make(chan struct{})}
	defer close(stream.release)
	ctx, cancel := context.WithCancel(context.Background())
	sender := newPipelinedSender(ctx, stream, 1, false, "local")

	msg := &filer_pb.SubscribeMetadataResponse{EventNotification: &filer_pb.EventNotification{}}
	if err := sender.Send(msg); err != nil { // consumed by the blocked sendLoop
		t.Fatal(err)
	}
	if err := sender.Send(msg); err != nil { // fills the one-deep queue
		t.Fatal(err)
	}

	cancel()

	done := make(chan error, 1)
	go func() { done <- sender.Send(msg) }()
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("Send returned %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Send still blocked after cancel")
	}
	if err := sender.Close(); err != nil {
		t.Fatalf("Close returned %v", err)
	}
}

// Once the subscription context ends, sendLoop must not start another
// stream.Send: the handler returns and gRPC ends the stream behind it, so a
// send started then would race the stream's teardown. The one Send already
// in flight is released by that teardown.
func TestPipelinedSenderStopsSendingOnCancel(t *testing.T) {
	stream := &blockedStream{release: make(chan struct{}), entered: make(chan struct{}, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	sender := newPipelinedSender(ctx, stream, 4, false, "local")

	msg := &filer_pb.SubscribeMetadataResponse{EventNotification: &filer_pb.EventNotification{}}
	if err := sender.Send(msg); err != nil {
		t.Fatal(err)
	}
	<-stream.entered         // sendLoop is inside stream.Send
	for i := 0; i < 2; i++ { // queued behind the blocked send
		if err := sender.Send(msg); err != nil {
			t.Fatal(err)
		}
	}

	cancel()
	close(stream.release)

	if err := sender.Close(); err != nil {
		t.Fatalf("Close returned %v", err)
	}
	select {
	case <-sender.done:
	case <-time.After(2 * time.Second):
		t.Fatal("sendLoop did not exit after cancel")
	}
	if n := stream.sends.Load(); n != 1 {
		t.Fatalf("stream.Send ran %d times, want 1; a new send started after cancel", n)
	}
}

// deliveryLagSnapshot returns the delivery-lag histogram's sample count and sum
// for a scope, so a test can assert what one send added.
func deliveryLagSnapshot(t *testing.T, scope string) (uint64, float64) {
	t.Helper()
	var m dto.Metric
	if err := stats.FilerSubscribeDeliveryLagSeconds.WithLabelValues(scope).(prometheus.Metric).Write(&m); err != nil {
		t.Fatalf("read delivery-lag metric: %v", err)
	}
	return m.GetHistogram().GetSampleCount(), m.GetHistogram().GetSampleSum()
}

// gatedStream blocks Send until release is closed, signalling on entered the
// first time Send is called, so a test can inspect state while a send is in
// flight (i.e. before it has succeeded).
type gatedStream struct {
	recordingStream
	entered chan struct{}
	release chan struct{}
}

func (s *gatedStream) Send(m *filer_pb.SubscribeMetadataResponse) error {
	select {
	case s.entered <- struct{}{}:
	default:
	}
	<-s.release
	return s.recordingStream.Send(m)
}

// TestPipelinedSenderObservesDeliveryLagAfterSend proves delivery lag is sampled
// after the real stream send (through the queue + Close), measured from the
// event's mutation stamp (not the moved stream position), and that control
// messages and empty progress markers are not sampled.
func TestPipelinedSenderObservesDeliveryLagAfterSend(t *testing.T) {
	const scope = "delivery-lag-test"
	beforeCount, beforeSum := deliveryLagSnapshot(t, scope)

	stream := &gatedStream{entered: make(chan struct{}, 1), release: make(chan struct{})}
	sender := newPipelinedSender(context.Background(), stream, 64, false, scope)

	mutationNs := time.Now().Add(-5 * time.Second).UnixNano()
	// A real event whose stream position (TsNs) has moved well past its original
	// mutation stamp; lag must be measured from the mutation stamp.
	event := &filer_pb.SubscribeMetadataResponse{
		TsNs:              time.Now().UnixNano(),
		MutationTsNs:      mutationNs,
		EventNotification: &filer_pb.EventNotification{NewEntry: &filer_pb.Entry{Name: "e"}},
	}
	// A control message (heartbeat): no EventNotification -> must not be sampled.
	control := &filer_pb.SubscribeMetadataResponse{TsNs: time.Now().UnixNano()}
	// A progress marker (sent after MaxUnsyncedEvents filtered entries): a
	// non-nil but empty EventNotification carrying the filtered entry's TsNs ->
	// must not be sampled either.
	marker := &filer_pb.SubscribeMetadataResponse{
		TsNs:              time.Now().UnixNano(),
		EventNotification: &filer_pb.EventNotification{},
	}
	if err := sender.Send(event); err != nil {
		t.Fatalf("send event: %v", err)
	}
	// Wait until the send loop is blocked inside stream.Send for the event, then
	// confirm nothing was sampled yet: the observation must come after the send
	// succeeds, not when the event entered the queue.
	<-stream.entered
	if midCount, _ := deliveryLagSnapshot(t, scope); midCount != beforeCount {
		t.Fatalf("delivery-lag sampled %d while the send was still blocked; it must observe only after stream.Send succeeds", midCount-beforeCount)
	}
	close(stream.release)

	if err := sender.Send(control); err != nil {
		t.Fatalf("send control: %v", err)
	}
	if err := sender.Send(marker); err != nil {
		t.Fatalf("send marker: %v", err)
	}
	if err := sender.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	afterCount, afterSum := deliveryLagSnapshot(t, scope)
	if afterCount != beforeCount+1 {
		t.Fatalf("delivery-lag samples added = %d, want 1 (only the real event; control and progress markers skipped)", afterCount-beforeCount)
	}
	if lag := afterSum - beforeSum; lag < 4 || lag > 60 {
		t.Fatalf("delivery lag = %.2fs, want ~5s from the mutation stamp (not ~0 from the moved TsNs)", lag)
	}
}
