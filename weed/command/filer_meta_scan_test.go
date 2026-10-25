package command

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

func TestLogicalPathResolvesVersionedLayout(t *testing.T) {
	raw := false
	scanRaw = &raw

	tests := []struct {
		name        string
		dir, entry  string
		wantPath    string
		wantVersion string
		wantKind    string
	}{
		{
			// The reason this exists: a client asks for summary.xml, but every
			// write lands on a v_<id> inside a sibling directory, so a filter on
			// the stored name never matches what the client is looking for.
			name: "version file reports the object key",
			dir:  "/buckets/b/rp/summary.xml.versions", entry: "v_abc123",
			wantPath: "/buckets/b/rp/summary.xml", wantVersion: "abc123",
		},
		{
			name: "versions directory reports the object key",
			dir:  "/buckets/b/rp", entry: "summary.xml.versions",
			wantPath: "/buckets/b/rp/summary.xml", wantKind: "versions-container",
		},
		{
			name: "unversioned object is unchanged",
			dir:  "/buckets/b/rp", entry: "summary.xml",
			wantPath: "/buckets/b/rp/summary.xml",
		},
		{
			// A v_ prefix only means a version inside a .versions directory.
			name: "v_ prefix outside a versions directory is a normal name",
			dir:  "/buckets/b/rp", entry: "v_notaversion",
			wantPath: "/buckets/b/rp/v_notaversion",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path, version, kind := logicalPath(tt.dir, tt.entry)
			if path != tt.wantPath || version != tt.wantVersion || kind != tt.wantKind {
				t.Errorf("logicalPath(%q,%q) = (%q,%q,%q), want (%q,%q,%q)",
					tt.dir, tt.entry, path, version, kind, tt.wantPath, tt.wantVersion, tt.wantKind)
			}
		})
	}
}

func TestLogicalPathRawKeepsStoredPath(t *testing.T) {
	raw := true
	scanRaw = &raw

	path, version, kind := logicalPath("/buckets/b/rp/summary.xml.versions", "v_abc123")
	if path != "/buckets/b/rp/summary.xml.versions/v_abc123" || version != "" || kind != "" {
		t.Errorf("raw mode must not rewrite the path, got (%q,%q,%q)", path, version, kind)
	}
}

func TestToScanEventClassifiesOperations(t *testing.T) {
	raw := false
	scanRaw = &raw

	entry := func(name string) *filer_pb.Entry {
		return &filer_pb.Entry{Name: name, Attributes: &filer_pb.FuseAttributes{FileSize: 11}}
	}

	tests := []struct {
		name     string
		resp     *filer_pb.SubscribeMetadataResponse
		wantOp   string
		wantPath string
	}{
		{
			name: "create",
			resp: &filer_pb.SubscribeMetadataResponse{
				EventNotification: &filer_pb.EventNotification{
					NewParentPath: "/b/rp", NewEntry: entry("obj"),
				},
			},
			wantOp: "CREATE", wantPath: "/b/rp/obj",
		},
		{
			name: "delete",
			resp: &filer_pb.SubscribeMetadataResponse{
				Directory:         "/b/rp",
				EventNotification: &filer_pb.EventNotification{OldEntry: entry("obj")},
			},
			wantOp: "DELETE", wantPath: "/b/rp/obj",
		},
		{
			name: "update in place",
			resp: &filer_pb.SubscribeMetadataResponse{
				Directory: "/b/rp",
				EventNotification: &filer_pb.EventNotification{
					OldEntry: entry("obj"), NewParentPath: "/b/rp", NewEntry: entry("obj"),
				},
			},
			wantOp: "UPDATE", wantPath: "/b/rp/obj",
		},
		{
			name: "rename",
			resp: &filer_pb.SubscribeMetadataResponse{
				Directory: "/b/rp",
				EventNotification: &filer_pb.EventNotification{
					OldEntry: entry("obj"), NewParentPath: "/b/rp2", NewEntry: entry("obj2"),
				},
			},
			wantOp: "RENAME", wantPath: "/b/rp2/obj2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := toScanEvent(tt.resp)
			if got == nil {
				t.Fatal("expected an event")
			}
			if got.op != tt.wantOp || got.path != tt.wantPath {
				t.Errorf("got (%s,%s), want (%s,%s)", got.op, got.path, tt.wantOp, tt.wantPath)
			}
		})
	}
}

// A retraction is a zero-length version carrying a flag; reporting it as a plain
// write would read as the opposite of what happened.
func TestDescribeEntryMarksDeleteMarkers(t *testing.T) {
	marker := &filer_pb.Entry{
		Name:       "v_abc",
		Attributes: &filer_pb.FuseAttributes{},
		Extended:   map[string][]byte{s3_constants.ExtDeleteMarkerKey: []byte("true")},
	}
	got := describeEntry(marker, "abc", "")
	if want := "version=abc delete-marker"; got != want {
		t.Errorf("describeEntry = %q, want %q", got, want)
	}
}

func TestParseScanTimeHonoursTimezone(t *testing.T) {
	saoPaulo, err := time.LoadLocation("America/Sao_Paulo")
	if err != nil {
		t.Skipf("tzdata unavailable: %v", err)
	}

	// The same wall-clock string is a different instant per zone, which is the
	// difference between finding a window and missing it by the UTC offset.
	inSP, err := parseScanTime("2026-08-03 01:16:42", saoPaulo)
	if err != nil {
		t.Fatal(err)
	}
	inUTC, err := parseScanTime("2026-08-03 01:16:42", time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	if delta := inSP.Sub(inUTC); delta != 3*time.Hour {
		t.Errorf("Sao Paulo should be 3h behind UTC, got %v", delta)
	}

	// An explicit zone in the string wins over loc.
	explicit, err := parseScanTime("2026-08-03T01:16:42Z", saoPaulo)
	if err != nil {
		t.Fatal(err)
	}
	if !explicit.Equal(inUTC) {
		t.Errorf("RFC3339 zone must win, got %s want %s", explicit, inUTC)
	}
}
