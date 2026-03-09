package filer

import (
	"context"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/log_buffer"
)

func TestNotifyUpdateEventRecordsRequestMetadataEvent(t *testing.T) {
	f := &Filer{
		Signature: 42,
		LocalMetaLogBuffer: log_buffer.NewLogBuffer(
			"test",
			time.Hour,
			func(*log_buffer.LogBuffer, time.Time, time.Time, []byte, int64, int64) {},
			nil,
			nil,
		),
	}

	ctx, sink := WithMetadataEventSink(context.Background())
	f.NotifyUpdateEvent(ctx, &Entry{FullPath: util.FullPath("/dir/file.txt")}, nil, true, false, []int32{7})

	event := sink.Last()
	if event == nil {
		t.Fatal("expected metadata event to be recorded")
	}
	if event.Directory != "/dir" {
		t.Fatalf("directory = %q, want /dir", event.Directory)
	}
	if event.EventNotification.OldEntry == nil || event.EventNotification.OldEntry.Name != "file.txt" {
		t.Fatalf("old entry = %+v, want file.txt", event.EventNotification.OldEntry)
	}
	if got := event.EventNotification.Signatures; len(got) != 2 || got[0] != 7 || got[1] != 42 {
		t.Fatalf("signatures = %v, want [7 42]", got)
	}
	if event.TsNs == 0 {
		t.Fatal("expected event timestamp to be set")
	}
}
