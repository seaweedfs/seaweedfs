package weed_server

import (
	"context"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

type recordingSender struct {
	sent []*filer_pb.SubscribeMetadataResponse
}

func (s *recordingSender) Send(resp *filer_pb.SubscribeMetadataResponse) error {
	s.sent = append(s.sent, resp)
	return nil
}

func metadataLogEntry(t *testing.T, dir, name string, tsNs int64) *filer_pb.LogEntry {
	t.Helper()
	data, err := proto.Marshal(&filer_pb.SubscribeMetadataResponse{
		Directory: dir,
		TsNs:      tsNs,
		EventNotification: &filer_pb.EventNotification{
			NewEntry: &filer_pb.Entry{Name: name},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return &filer_pb.LogEntry{TsNs: tsNs, Data: data}
}

func TestEachLogEntryFnPrefilterSkipsDecode(t *testing.T) {
	req := &filer_pb.SubscribeMetadataRequest{PathPrefix: "/data/pvc-7/"}
	sender := &recordingSender{}
	var decoded int
	var unsyncedEvents int64
	fn := eachLogEntryFn(context.Background(), req, sender, func(dirPath string, eventNotification *filer_pb.EventNotification, tsNs int64) error {
		decoded++
		unsyncedEvents = 0 // emulate a delivery, like the notification fn after a send
		return nil
	}, &unsyncedEvents)

	// non-matching entries skip the full decode but keep heartbeating
	for i := 0; i <= MaxUnsyncedEvents; i++ {
		if _, err := fn(metadataLogEntry(t, "/data/pvc-1", "x.log", int64(i+1))); err != nil {
			t.Fatal(err)
		}
	}
	if decoded != 0 {
		t.Fatalf("non-matching entries must not reach the notification fn, got %d", decoded)
	}
	if len(sender.sent) != 1 {
		t.Fatalf("expected one unsynced heartbeat, got %d", len(sender.sent))
	}
	if hb := sender.sent[0]; hb.TsNs != int64(MaxUnsyncedEvents+1) || len(hb.EventNotification.Signatures) != 0 {
		t.Fatalf("unexpected heartbeat %+v", hb)
	}

	// a matching entry takes the full decode path
	if _, err := fn(metadataLogEntry(t, "/data/pvc-7", "y.log", 9999)); err != nil {
		t.Fatal(err)
	}
	if decoded != 1 {
		t.Fatalf("matching entry must be decoded and delivered, got %d", decoded)
	}

	// the delivery reset the shared counter: a fresh full window passes
	// before the next heartbeat
	for i := 0; i <= MaxUnsyncedEvents; i++ {
		if _, err := fn(metadataLogEntry(t, "/data/pvc-1", "x.log", int64(20000+i))); err != nil {
			t.Fatal(err)
		}
	}
	if len(sender.sent) != 2 {
		t.Fatalf("expected a second heartbeat after a full window, got %d sends", len(sender.sent))
	}
}

func TestEachLogEntryFnNoFilterDecodesEverything(t *testing.T) {
	req := &filer_pb.SubscribeMetadataRequest{}
	var decoded int
	var unsyncedEvents int64
	fn := eachLogEntryFn(context.Background(), req, &recordingSender{}, func(dirPath string, eventNotification *filer_pb.EventNotification, tsNs int64) error {
		decoded++
		return nil
	}, &unsyncedEvents)
	if _, err := fn(metadataLogEntry(t, "/anywhere", "x", 1)); err != nil {
		t.Fatal(err)
	}
	if decoded != 1 {
		t.Fatalf("unfiltered subscriber must decode every entry, got %d", decoded)
	}
}

func TestEachLogEntryFnDeliversStreamPositionTs(t *testing.T) {
	// The event stamp is taken before the store write while the buffer
	// append lands after it, so a delayed append keeps an early stamp in
	// the payload while LogEntry.TsNs is bumped to stay monotone.
	// Subscribers must see the stream position: a cursor saved past the
	// payload stamp would filter the event out forever.
	req := &filer_pb.SubscribeMetadataRequest{}
	var unsyncedEvents int64
	var gotTsNs int64
	fn := eachLogEntryFn(context.Background(), req, &recordingSender{}, func(dirPath string, eventNotification *filer_pb.EventNotification, tsNs int64) error {
		gotTsNs = tsNs
		return nil
	}, &unsyncedEvents)

	data, err := proto.Marshal(&filer_pb.SubscribeMetadataResponse{
		Directory: "/buckets/b",
		TsNs:      99, // stamped before the store write
		EventNotification: &filer_pb.EventNotification{
			NewEntry: &filer_pb.Entry{Name: "expire1/foo"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// The buffer appended it after a ts=100 write already landed: bumped to 101.
	if _, err := fn(&filer_pb.LogEntry{TsNs: 101, Data: data}); err != nil {
		t.Fatal(err)
	}
	if gotTsNs != 101 {
		t.Fatalf("delivered ts = %d, want stream position 101", gotTsNs)
	}
}

func TestEachLogEntryFnFilteredHeartbeatUsesStreamPosition(t *testing.T) {
	req := &filer_pb.SubscribeMetadataRequest{PathPrefix: "/buckets/keep/"}
	sender := &recordingSender{}
	unsyncedEvents := int64(MaxUnsyncedEvents)
	fn := eachLogEntryFn(context.Background(), req, sender, func(dirPath string, eventNotification *filer_pb.EventNotification, tsNs int64) error {
		return nil
	}, &unsyncedEvents)

	data, err := proto.Marshal(&filer_pb.SubscribeMetadataResponse{
		Directory: "/buckets/other",
		TsNs:      99,
		EventNotification: &filer_pb.EventNotification{
			NewEntry: &filer_pb.Entry{Name: "x"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fn(&filer_pb.LogEntry{TsNs: 101, Data: data}); err != nil {
		t.Fatal(err)
	}
	if len(sender.sent) != 1 || sender.sent[0].TsNs != 101 {
		t.Fatalf("heartbeat ts = %+v, want stream position 101", sender.sent)
	}
}
