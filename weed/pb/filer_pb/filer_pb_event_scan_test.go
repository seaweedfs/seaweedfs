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

func TestMetadataDirectoryMayMatchSubscriptionCoversMatches(t *testing.T) {
	dirs := []string{"/", "/data", "/data/pvc-7", "/data/pvc-70", "/data/pvc-7/sub", "/data/pvc-1", "/other", "/data/./pvc-7", "/data//pvc-7", "/data/pvc-1/../pvc-7"}
	names := []string{"pvc-7", "pvc-70", "a.log", "sub", ""}
	filters := []struct {
		prefix   string
		prefixes []string
		dirs     []string
	}{
		{prefix: "/data/pvc-7/"},
		{prefix: "/data/pvc-7"},
		{prefix: "/data/"},
		{prefix: "/"},
		{prefix: "/data/pvc-7/sub/deeper/"},
		{prefix: "/x", prefixes: []string{"/other/", "/data/pvc-7/"}},
		{prefix: "/x", dirs: []string{"/data/pvc-1"}},
	}
	for _, dir := range dirs {
		for _, target := range append([]string{""}, dirs...) {
			for _, name := range names {
				event := &SubscribeMetadataResponse{Directory: dir, EventNotification: &EventNotification{
					OldEntry: &Entry{Name: name}, NewEntry: &Entry{Name: name}, NewParentPath: target}}
				for _, f := range filters {
					if !MetadataEventMatchesSubscription(event, f.prefix, f.prefixes, f.dirs) {
						continue
					}
					if !MetadataDirectoryMayMatchSubscription([]byte(dir), f.prefix, f.prefixes, f.dirs) &&
						(target == "" || !MetadataDirectoryMayMatchSubscription([]byte(target), f.prefix, f.prefixes, f.dirs)) {
						t.Errorf("event %q/%q -> %q matches %+v but neither directory may match", dir, name, target, f)
					}
				}
			}
		}
	}

	for _, c := range []struct {
		dir, prefix string
		want        bool
	}{
		{"/data/pvc-1", "/data/pvc-7/", false},
		{"/data/pvc-70", "/data/pvc-7/", false},
		{"/other", "/data/", false},
		{"/data/pvc-7", "/data/pvc-7/", true},
		{"/data/pvc-7/sub", "/data/pvc-7/", true},
		{"/data", "/data/pvc-7", true},
		{"/", "/data/", true},
	} {
		if got := MetadataDirectoryMayMatchSubscription([]byte(c.dir), c.prefix, nil, nil); got != c.want {
			t.Errorf("dir %q prefix %q: got %v, want %v", c.dir, c.prefix, got, c.want)
		}
	}
}

func TestScanMetadataEventDirectories(t *testing.T) {
	for _, event := range []*SubscribeMetadataResponse{
		{Directory: "/data/pvc-7", EventNotification: &EventNotification{NewEntry: bigEntry("a.log")}},
		{Directory: "/data/pvc-7", EventNotification: &EventNotification{OldEntry: bigEntry("a.log"), NewEntry: bigEntry("a.log"), NewParentPath: "/data/pvc-9"}},
		{Directory: "/data/pvc-7"},
		{},
	} {
		dir, target, ok := ScanMetadataEventDirectories(marshalEvent(t, event))
		if !ok || string(dir) != event.Directory || string(target) != event.GetEventNotification().GetNewParentPath() {
			t.Errorf("scan of %+v: dir %q target %q ok %v", event, dir, target, ok)
		}
	}

	// repeated event_notification fields merge: the last new_parent_path wins
	first := marshalEvent(t, &SubscribeMetadataResponse{Directory: "/a", EventNotification: &EventNotification{NewParentPath: "/b"}})
	second := marshalEvent(t, &SubscribeMetadataResponse{EventNotification: &EventNotification{NewEntry: &Entry{Name: "x"}, NewParentPath: "/c"}})
	merged := append(append([]byte{}, first...), second...)
	full := &SubscribeMetadataResponse{}
	if err := proto.Unmarshal(merged, full); err != nil {
		t.Fatal(err)
	}
	dir, target, ok := ScanMetadataEventDirectories(merged)
	if !ok || string(dir) != full.Directory || string(target) != full.EventNotification.NewParentPath {
		t.Errorf("merged: dir %q target %q ok %v, full decode %q %q", dir, target, ok, full.Directory, full.EventNotification.NewParentPath)
	}

	if _, _, ok := ScanMetadataEventDirectories([]byte{0xff, 0xff, 0xff}); ok {
		t.Error("malformed payload must not scan")
	}
}
