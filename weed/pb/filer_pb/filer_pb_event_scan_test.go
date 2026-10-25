package filer_pb

import (
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func marshalEvent(t *testing.T, event *SubscribeMetadataResponse) []byte {
	t.Helper()
	data, err := proto.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func bigEntry(name string) *Entry {
	chunks := make([]*FileChunk, 50)
	for i := range chunks {
		chunks[i] = &FileChunk{FileId: "3,1234567890ab", Offset: int64(i) * 100, Size: 100}
	}
	return &Entry{Name: name, Chunks: chunks, Attributes: &FuseAttributes{FileSize: 5000, Mtime: 1234567890}}
}

func TestScanMetadataEventSkeletonMatchesFullDecode(t *testing.T) {
	events := map[string]*SubscribeMetadataResponse{
		"create": {Directory: "/data/pvc-7", TsNs: 100,
			EventNotification: &EventNotification{NewEntry: bigEntry("a.log")}},
		"update": {Directory: "/data/pvc-7", TsNs: 200,
			EventNotification: &EventNotification{OldEntry: bigEntry("a.log"), NewEntry: bigEntry("a.log")}},
		"delete": {Directory: "/data/pvc-7", TsNs: 300,
			EventNotification: &EventNotification{OldEntry: bigEntry("a.log"), DeleteChunks: true}},
		"rename within dir": {Directory: "/data/pvc-7", TsNs: 400,
			EventNotification: &EventNotification{OldEntry: bigEntry("a.log"), NewEntry: bigEntry("b.log")}},
		"rename across dirs": {Directory: "/data/pvc-7", TsNs: 500,
			EventNotification: &EventNotification{OldEntry: bigEntry("a.log"), NewEntry: bigEntry("a.log"), NewParentPath: "/data/pvc-9"}},
		"empty notification": {Directory: "/data/pvc-7", TsNs: 600,
			EventNotification: &EventNotification{}},
		"no notification": {Directory: "/data/pvc-7", TsNs: 700},
	}
	filters := []struct {
		prefix   string
		prefixes []string
		dirs     []string
	}{
		{prefix: "/data/pvc-7/"},
		{prefix: "/data/pvc-9/"},
		{prefix: "/data/pvc-1/"},
		{prefix: "/"},
		{prefixes: []string{"/other/", "/data/pvc-9/"}},
		{dirs: []string{"/data/pvc-7"}},
		{dirs: []string{"/data/pvc-9"}},
	}

	for name, event := range events {
		skeleton, ok := ScanMetadataEventSkeleton(marshalEvent(t, event))
		if !ok {
			t.Fatalf("%s: scan failed", name)
		}
		if skeleton.TsNs != event.TsNs {
			t.Errorf("%s: ts %d != %d", name, skeleton.TsNs, event.TsNs)
		}
		for _, f := range filters {
			want := MetadataEventMatchesSubscription(event, f.prefix, f.prefixes, f.dirs)
			got := MetadataEventMatchesSubscription(skeleton, f.prefix, f.prefixes, f.dirs)
			if got != want {
				t.Errorf("%s with filter %+v: skeleton match %v, full match %v", name, f, got, want)
			}
		}
	}
}

func TestScanMetadataEventSkeletonFallsBack(t *testing.T) {
	if _, ok := ScanMetadataEventSkeleton([]byte{0xff, 0xff, 0xff}); ok {
		t.Error("malformed payload must not scan")
	}

	// two event_notification occurrences merge under proto semantics; the
	// scanner must punt rather than guess
	one := marshalEvent(t, &SubscribeMetadataResponse{
		EventNotification: &EventNotification{OldEntry: &Entry{Name: "a"}}})
	two := append(append([]byte{}, one...), one...)
	if _, ok := ScanMetadataEventSkeleton(two); ok {
		t.Error("duplicated message field must fall back to full decode")
	}

	// same for a duplicated entry inside the notification
	entry, err := proto.Marshal(&Entry{Name: "a"})
	if err != nil {
		t.Fatal(err)
	}
	var notification []byte
	for i := 0; i < 2; i++ {
		notification = protowire.AppendTag(notification, 1, protowire.BytesType) // old_entry
		notification = protowire.AppendBytes(notification, entry)
	}
	var response []byte
	response = protowire.AppendTag(response, 2, protowire.BytesType) // event_notification
	response = protowire.AppendBytes(response, notification)
	if _, ok := ScanMetadataEventSkeleton(response); ok {
		t.Error("duplicated nested entry field must fall back to full decode")
	}
}

func TestScanMetadataEventSkeletonEmptyPayload(t *testing.T) {
	skeleton, ok := ScanMetadataEventSkeleton(nil)
	if !ok || skeleton.Directory != "" || skeleton.EventNotification != nil {
		t.Fatalf("empty payload: %+v ok=%v", skeleton, ok)
	}
}
