package command

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/replication/sink"
	"github.com/seaweedfs/seaweedfs/weed/replication/source"
	"github.com/seaweedfs/seaweedfs/weed/util"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
)

func TestMain(m *testing.M) {
	util_http.InitGlobalHttpClient()
	os.Exit(m.Run())
}

// readUrlError starts a test HTTP server returning the given status code
// and returns the error produced by ReadUrlAsStream.
//
// The error format is defined in ReadUrlAsStream:
// https://github.com/seaweedfs/seaweedfs/blob/3a765df2ff90839acb9acf910b73513417fa84d1/weed/util/http/http_global_client_util.go#L353
func readUrlError(t *testing.T, statusCode int) error {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, http.StatusText(statusCode), statusCode)
	}))
	defer server.Close()

	_, err := util_http.ReadUrlAsStream(context.Background(),
		server.URL+"/437,03f591a3a2b95e?readDeleted=true", "",
		nil, false, true, 0, 1024, func(data []byte) {})
	if err == nil {
		t.Fatal("expected error from ReadUrlAsStream, got nil")
	}
	return err
}

func TestIsIgnorable404_WrappedErrNotFound(t *testing.T) {
	readErr := readUrlError(t, http.StatusNotFound)
	// genProcessFunction wraps sink errors with %w:
	// https://github.com/seaweedfs/seaweedfs/blob/3a765df2ff90839acb9acf910b73513417fa84d1/weed/command/filer_sync.go#L496
	genErr := fmt.Errorf("create entry1 : %w", readErr)

	if !isIgnorable404(genErr) {
		t.Errorf("expected ignorable, got not: %v", genErr)
	}
}

func TestIsIgnorable404_BrokenUnwrapChain(t *testing.T) {
	readErr := readUrlError(t, http.StatusNotFound)
	// AWS SDK v1 wraps transport errors via awserr.New which uses origErr.Error()
	// instead of %w, so errors.Is cannot unwrap through it:
	// https://github.com/aws/aws-sdk-go/blob/v1.55.8/aws/corehandlers/handlers.go#L173
	// https://github.com/aws/aws-sdk-go/blob/v1.55.8/aws/awserr/types.go#L15
	awsSdkErr := fmt.Errorf("RequestError: send request failed\n"+
		"caused by: Put \"https://s3.amazonaws.com/bucket/key\": %s", readErr.Error())
	genErr := fmt.Errorf("create entry1 : %w", awsSdkErr)

	if !isIgnorable404(genErr) {
		t.Errorf("expected ignorable, got not: %v", genErr)
	}
}

func TestIsIgnorable404_NonIgnorableError(t *testing.T) {
	readErr := readUrlError(t, http.StatusForbidden)
	genErr := fmt.Errorf("create entry1 : %w", readErr)

	if isIgnorable404(genErr) {
		t.Errorf("expected not ignorable, got ignorable: %v", genErr)
	}
}

// stubSink is a minimal ReplicationSink used to exercise initialSnapshotTargetKey
// without standing up a real sink. Only the two methods read by the key builder
// (GetName, IsIncremental) need meaningful behavior; the rest satisfy the interface.
type stubSink struct {
	name          string
	isIncremental bool
}

func (s *stubSink) GetName() string                           { return s.name }
func (s *stubSink) Initialize(util.Configuration, string) error { return nil }
func (s *stubSink) DeleteEntry(string, bool, bool, []int32) error {
	return nil
}
func (s *stubSink) CreateEntry(string, *filer_pb.Entry, []int32) error { return nil }
func (s *stubSink) UpdateEntry(string, *filer_pb.Entry, string, *filer_pb.Entry, bool, []int32) (bool, error) {
	return false, nil
}
func (s *stubSink) GetSinkToDirectory() string       { return "" }
func (s *stubSink) SetSourceFiler(*source.FilerSource) {}
func (s *stubSink) IsIncremental() bool              { return s.isIncremental }

var _ sink.ReplicationSink = (*stubSink)(nil)

func TestInitialSnapshotTargetKey(t *testing.T) {
	// Mirror the non-incremental path of buildKey so a refactor of one without
	// the other will fail this test.
	mirror := &stubSink{name: "mirror", isIncremental: false}
	got := initialSnapshotTargetKey(mirror, "/backup", "/data", util.FullPath("/data/sub/file.txt"), &filer_pb.Entry{})
	if got != "/backup/sub/file.txt" {
		t.Errorf("mirror sink: got %q, want %q", got, "/backup/sub/file.txt")
	}

	// Incremental sinks partition by entry mtime, so the seed must use the same
	// YYYY-MM-DD prefix a replayed CreateEntry would produce. buildKey in
	// filer_sync.go formats the date in local time, so compute the expected
	// key the same way to keep the test timezone-independent.
	inc := &stubSink{name: "inc", isIncremental: true}
	mtime := int64(1704196800) // 2024-01-02T12:00:00 UTC — unambiguously Jan 2 in nearly all timezones
	gotInc := initialSnapshotTargetKey(inc, "/backup", "/data", util.FullPath("/data/sub/file.txt"), &filer_pb.Entry{
		Attributes: &filer_pb.FuseAttributes{Mtime: mtime},
	})
	wantInc := "/backup/" + time.Unix(mtime, 0).Format("2006-01-02") + "/sub/file.txt"
	if gotInc != wantInc {
		t.Errorf("incremental sink: got %q, want %q", gotInc, wantInc)
	}

	// Trailing-slash sourcePath still produces a clean relative key.
	gotTrail := initialSnapshotTargetKey(mirror, "/backup", "/data/", util.FullPath("/data/file.txt"), &filer_pb.Entry{})
	if gotTrail != "/backup/file.txt" {
		t.Errorf("trailing-slash sourcePath: got %q, want %q", gotTrail, "/backup/file.txt")
	}

	// Edge cases CodeRabbit called out: sourceKey equal to sourcePath
	// (non-trailing and trailing variants). Real TraverseBfs walks never emit
	// the root itself, but the helper must not panic if something else does.
	if got := initialSnapshotTargetKey(mirror, "/backup", "/data", util.FullPath("/data"), &filer_pb.Entry{}); got != "/backup" {
		t.Errorf("sourceKey == sourcePath (no slash): got %q, want %q", got, "/backup")
	}
	if got := initialSnapshotTargetKey(mirror, "/backup", "/data/", util.FullPath("/data"), &filer_pb.Entry{}); got != "/backup" {
		t.Errorf("sourceKey == sourcePath (trailing slash mismatch): got %q, want %q", got, "/backup")
	}
}
