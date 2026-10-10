package s3api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// A precondition can only fail against an object that exists. GET/HEAD of a missing
// key must stay a missing-key answer even when If-Match or If-Unmodified-Since is sent.
func TestValidateConditionalHeadersForReadsMissingObject(t *testing.T) {
	s3a := &S3ApiServer{}

	existing := &filer_pb.Entry{
		Attributes: &filer_pb.FuseAttributes{Mtime: time.Now().Unix()},
		Extended:   map[string][]byte{s3_constants.ExtETagKey: []byte("d41d8cd98f00b204e9800998ecf8427e")},
	}
	deleteMarker := &filer_pb.Entry{
		Attributes: &filer_pb.FuseAttributes{Mtime: time.Now().Unix()},
		Extended:   map[string][]byte{s3_constants.ExtDeleteMarkerKey: []byte("true")},
	}
	future := time.Now().Add(24 * time.Hour).UTC().Format(http.TimeFormat)

	testCases := []struct {
		name   string
		header string
		value  string
		entry  *filer_pb.Entry
		want   s3err.ErrorCode
	}{
		{"if-match on missing object", s3_constants.IfMatch, "0000", nil, s3err.ErrNoSuchKey},
		{"if-match star on missing object", s3_constants.IfMatch, "*", nil, s3err.ErrNoSuchKey},
		{"if-unmodified-since on missing object", s3_constants.IfUnmodifiedSince, future, nil, s3err.ErrNoSuchKey},
		{"if-match on delete marker", s3_constants.IfMatch, "0000", deleteMarker, s3err.ErrNoSuchKey},
		{"if-none-match on missing object", s3_constants.IfNoneMatch, "*", nil, s3err.ErrNone},
		{"if-modified-since on missing object", s3_constants.IfModifiedSince, future, nil, s3err.ErrNone},
		{"if-match mismatch on existing object", s3_constants.IfMatch, "0000", existing, s3err.ErrPreconditionFailed},
		{"if-match hit on existing object", s3_constants.IfMatch, "d41d8cd98f00b204e9800998ecf8427e", existing, s3err.ErrNone},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/bucket/object", nil)
			r.Header.Set(tc.header, tc.value)
			headers, errCode := parseConditionalHeaders(r)
			if errCode != s3err.ErrNone {
				t.Fatalf("parseConditionalHeaders: %v", errCode)
			}
			result := s3a.validateConditionalHeadersForReads(r, headers, tc.entry, "bucket", "object")
			if result.ErrorCode != tc.want {
				t.Errorf("got %v, want %v", result.ErrorCode, tc.want)
			}
		})
	}
}

func TestRemoteObjectETagAcrossCacheFill(t *testing.T) {
	s3a := &S3ApiServer{}
	entry := &filer_pb.Entry{
		Name:        "object",
		Attributes:  &filer_pb.FuseAttributes{FileSize: 2048, Mtime: 100},
		RemoteEntry: &filer_pb.RemoteEntry{RemoteSize: 2048, RemoteMtime: 100, RemoteETag: "remote-multipart-2"},
	}
	head := httptest.NewRecorder()
	s3a.setResponseHeaders(head, httptest.NewRequest(http.MethodHead, "/bucket/object", nil), entry, 2048)
	headETag := head.Header().Get("ETag")
	if headETag != `"remote-multipart-2"` {
		t.Errorf("cold ETag = %q", headETag)
	}
	entry.Chunks = []*filer_pb.FileChunk{
		{Offset: 0, Size: 1024, ETag: "1B2M2Y8AsgTpgAmY7PhCfg=="},
		{Offset: 1024, Size: 1024, ETag: "1B2M2Y8AsgTpgAmY7PhCfg=="},
	}
	entry.RemoteEntry.LastLocalSyncTsNs = 200
	if got := s3a.getObjectETag(entry); got != headETag {
		t.Errorf("cached ETag = %q, cold ETag = %q", got, headETag)
	}
	for _, method := range []string{http.MethodGet, http.MethodHead} {
		r := httptest.NewRequest(method, "/bucket/object", nil)
		r.Header.Set(s3_constants.IfMatch, headETag)
		r.Header.Set("Range", "bytes=1024-2047")
		headers, code := parseConditionalHeaders(r)
		if code != s3err.ErrNone {
			t.Fatal(code)
		}
		result := s3a.validateConditionalHeadersForReads(r, headers, entry, "bucket", "object")
		if result.ErrorCode != s3err.ErrNone {
			t.Errorf("%s after cache fill: %v", method, result.ErrorCode)
		}
	}
	entry.RemoteEntry.RemoteETag = `"remote-multipart-2"`
	if got := s3a.getObjectETag(entry); got != headETag {
		t.Errorf("quoted remote ETag = %q", got)
	}
	entry.Extended = map[string][]byte{s3_constants.ExtETagKey: []byte("local-etag")}
	if got := s3a.getObjectETag(entry); got != `"local-etag"` {
		t.Errorf("explicit ETag = %q", got)
	}
	entry.Extended = nil
	entry.RemoteEntry.LastLocalSyncTsNs = 0
	if got, want := s3a.getObjectETag(entry), s3a.calculateETagFromChunks(entry.Chunks); got != want {
		t.Errorf("unsynced local ETag = %q, want %q", got, want)
	}
}

// A metadata-only update such as utimens changes Mtime without touching the
// remote bytes, so the object must keep its remote ETag; otherwise a later
// cache fill restores it and breaks clients holding the interim value.
func TestRemoteObjectETagMetadataOnlyTouch(t *testing.T) {
	s3a := &S3ApiServer{}
	for _, cached := range []bool{false, true} {
		entry := &filer_pb.Entry{
			Name:       "object",
			Attributes: &filer_pb.FuseAttributes{FileSize: 2048, Mtime: 100},
			RemoteEntry: &filer_pb.RemoteEntry{
				RemoteSize: 2048, RemoteMtime: 100, RemoteETag: "remote-multipart-2",
			},
		}
		if cached {
			entry.Chunks = []*filer_pb.FileChunk{{Size: 2048, ETag: "1B2M2Y8AsgTpgAmY7PhCfg=="}}
			entry.RemoteEntry.LastLocalSyncTsNs = 200
		}
		entry.Attributes.Mtime = 101
		entry.Attributes.MtimeNs = 500
		if got := s3a.getObjectETag(entry); got != `"remote-multipart-2"` {
			t.Errorf("cached=%v touched ETag = %q", cached, got)
		}
	}
}

// Extending or shrinking a cached remote file through setattr changes FileSize
// while leaving its chunks and sync stamp alone; the remote ETag no longer
// describes the local bytes and conditional reads must not answer 304 for it.
func TestRemoteObjectETagLocalResize(t *testing.T) {
	s3a := &S3ApiServer{}
	for _, size := range []uint64{0, 1024, 4096} {
		entry := &filer_pb.Entry{
			Name:       "object",
			Attributes: &filer_pb.FuseAttributes{FileSize: 2048, Mtime: 100},
			Chunks:     []*filer_pb.FileChunk{{Size: 2048, ETag: "1B2M2Y8AsgTpgAmY7PhCfg=="}},
			RemoteEntry: &filer_pb.RemoteEntry{
				RemoteSize: 2048, RemoteMtime: 100, RemoteETag: "remote-multipart-2",
				LastLocalSyncTsNs: 200,
			},
		}
		entry.Attributes.FileSize = size
		if got := s3a.getObjectETag(entry); got == `"remote-multipart-2"` {
			t.Errorf("resized to %d retained remote ETag", size)
		}
		r := httptest.NewRequest(http.MethodHead, "/bucket/object", nil)
		r.Header.Set(s3_constants.IfNoneMatch, `"remote-multipart-2"`)
		headers, code := parseConditionalHeaders(r)
		if code != s3err.ErrNone {
			t.Fatal(code)
		}
		if got := s3a.validateConditionalHeadersForReads(r, headers, entry, "bucket", "object"); got.ErrorCode != s3err.ErrNone {
			t.Errorf("resized to %d read: %v", size, got.ErrorCode)
		}
	}
}

func TestRemoteObjectETagCopyConditions(t *testing.T) {
	s3a := &S3ApiServer{}
	entry := &filer_pb.Entry{
		Attributes:  &filer_pb.FuseAttributes{FileSize: 2048, Mtime: 100},
		RemoteEntry: &filer_pb.RemoteEntry{RemoteSize: 2048, RemoteMtime: 100, RemoteETag: "remote-multipart-2"},
	}
	for _, cached := range []bool{false, true} {
		if cached {
			entry.Chunks = []*filer_pb.FileChunk{{Size: 2048, ETag: "1B2M2Y8AsgTpgAmY7PhCfg=="}}
			entry.RemoteEntry.LastLocalSyncTsNs = 200
		}
		for _, condition := range []string{s3_constants.AmzCopySourceIfMatch, s3_constants.AmzCopySourceIfNoneMatch} {
			r := httptest.NewRequest(http.MethodPut, "/bucket/copy", nil)
			r.Header.Set(condition, s3a.getObjectETag(entry))
			want := s3err.ErrNone
			if condition == s3_constants.AmzCopySourceIfNoneMatch {
				want = s3err.ErrPreconditionFailed
			}
			if got := s3a.validateConditionalCopyHeaders(r, entry); got != want {
				t.Errorf("cached=%v %s: got %v, want %v", cached, condition, got, want)
			}
		}
	}
}

func TestRemoteObjectETagLocalChanges(t *testing.T) {
	s3a := &S3ApiServer{}
	for _, content := range [][]byte{nil, []byte("new local content")} {
		entry := &filer_pb.Entry{
			Attributes:  &filer_pb.FuseAttributes{FileSize: uint64(len(content)), Mtime: 101},
			Content:     content,
			RemoteEntry: &filer_pb.RemoteEntry{RemoteSize: 2048, RemoteMtime: 100, RemoteETag: "old-origin"},
		}
		if got, want := copyEntryETag(entry), strings.Trim(s3a.getObjectETag(entry), `"`); got != want {
			t.Errorf("local copy ETag = %q, read ETag = %q", got, want)
		}
		if got := s3a.getObjectETag(entry); got == `"old-origin"` {
			t.Errorf("local content %q retained remote ETag", content)
		}
		r := httptest.NewRequest(http.MethodHead, "/bucket/object", nil)
		r.Header.Set(s3_constants.IfNoneMatch, `"old-origin"`)
		headers, code := parseConditionalHeaders(r)
		if code != s3err.ErrNone {
			t.Fatal(code)
		}
		if got := s3a.validateConditionalHeadersForReads(r, headers, entry, "bucket", "object"); got.ErrorCode != s3err.ErrNone {
			t.Errorf("local content %q read: %v", content, got.ErrorCode)
		}
		r.Header.Set(s3_constants.AmzCopySourceIfMatch, `"old-origin"`)
		if got := s3a.validateConditionalCopyHeaders(r, entry); got != s3err.ErrPreconditionFailed {
			t.Errorf("local content %q copy: %v", content, got)
		}
	}
}

func TestRemoteObjectETagFallback(t *testing.T) {
	s3a := &S3ApiServer{}
	entry := &filer_pb.Entry{
		Attributes:  &filer_pb.FuseAttributes{Md5: []byte{0xab, 0xcd}},
		RemoteEntry: &filer_pb.RemoteEntry{},
	}
	if got := s3a.getObjectETag(entry); got != `"abcd"` {
		t.Errorf("missing remote ETag = %q", got)
	}
	entry.RemoteEntry.RemoteETag = "empty-origin"
	if got := s3a.getObjectETag(entry); got != `"empty-origin"` {
		t.Errorf("empty remote object ETag = %q", got)
	}
	entry.RemoteEntry = nil
	if got := s3a.getObjectETag(entry); got != `"abcd"` {
		t.Errorf("local object ETag = %q", got)
	}
}
