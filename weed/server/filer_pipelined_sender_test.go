package weed_server

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
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
	sender := newPipelinedSender(stream, 64, true)

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
