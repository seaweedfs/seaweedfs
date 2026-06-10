package weed_server

import (
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
	fn := eachLogEntryFn(req, sender, func(dirPath string, eventNotification *filer_pb.EventNotification, tsNs int64) error {
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
	fn := eachLogEntryFn(req, &recordingSender{}, func(dirPath string, eventNotification *filer_pb.EventNotification, tsNs int64) error {
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
