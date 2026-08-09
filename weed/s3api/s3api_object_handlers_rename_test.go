package s3api

import (
	"net/http"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRenameSourceCandidates: AWS spells x-amz-rename-source as a bare key in
// its CLI, Java and Rust examples and as bucket/key in a second CLI example and
// the boto3 conditional one, so both readings have to survive parsing. The
// literal key leads; the bucket-qualified reading follows only when the value
// carries the request's own bucket.
func TestRenameSourceCandidates(t *testing.T) {
	tests := []struct {
		name    string
		source  string
		want    []string
		wantErr s3err.ErrorCode
	}{
		{"bare key", "source.txt", []string{"source.txt"}, s3err.ErrNone},
		{"bare key with leading slash", "/source.txt", []string{"source.txt"}, s3err.ErrNone},
		{"bare key with prefix", "dir/source.txt", []string{"dir/source.txt"}, s3err.ErrNone},
		{"bucket qualified", "/bucket/dir/key.txt", []string{"bucket/dir/key.txt", "dir/key.txt"}, s3err.ErrNone},
		{"bucket qualified without leading slash", "bucket/key.txt", []string{"bucket/key.txt", "key.txt"}, s3err.ErrNone},
		{"key whose first segment is another bucket", "other/key.txt", []string{"other/key.txt"}, s3err.ErrNone},
		{"percent encoded", "a%20b.txt", []string{"a b.txt"}, s3err.ErrNone},
		{"plus stays literal", "a+b.txt", []string{"a+b.txt"}, s3err.ErrNone},
		{"duplicate slashes collapse", "//dir//key.txt", []string{"dir/key.txt"}, s3err.ErrNone},
		{"bucket name alone is a key", "bucket", []string{"bucket"}, s3err.ErrNone},
		{"bucket prefix with empty key", "bucket/", []string{"bucket/"}, s3err.ErrNone},
		{"missing header", "", nil, s3err.ErrInvalidRenameSource},
		{"parent traversal", "../other/key.txt", nil, s3err.ErrInvalidRenameSource},
		{"encoded parent traversal", "%2e%2e/other/key.txt", nil, s3err.ErrInvalidRenameSource},
		{"parent traversal behind the bucket", "/bucket/../other/key.txt", nil, s3err.ErrInvalidRenameSource},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r, err := http.NewRequest(http.MethodPut, "/bucket/dst.txt?renameObject", nil)
			require.NoError(t, err)
			if tc.source != "" {
				r.Header.Set(s3_constants.AmzRenameSource, tc.source)
			}

			candidates, errCode := renameSourceCandidates(r, "bucket")
			assert.Equal(t, tc.wantErr, errCode)
			assert.Equal(t, tc.want, candidates)
		})
	}
}

func TestRenameSourceConditionalHeaders(t *testing.T) {
	mtime := time.Date(2026, 2, 1, 12, 0, 0, 0, time.UTC)
	entry := &filer_pb.Entry{
		Attributes: &filer_pb.FuseAttributes{Mtime: mtime.Unix()},
		Extended:   map[string][]byte{s3_constants.ExtETagKey: []byte("d41d8cd98f00b204e9800998ecf8427e")},
	}

	tests := []struct {
		name   string
		header string
		value  string
		want   s3err.ErrorCode
	}{
		{"if-match hit", s3_constants.AmzRenameSourceIfMatch, `"d41d8cd98f00b204e9800998ecf8427e"`, s3err.ErrNone},
		{"if-match miss", s3_constants.AmzRenameSourceIfMatch, "0000", s3err.ErrPreconditionFailed},
		{"if-match star", s3_constants.AmzRenameSourceIfMatch, "*", s3err.ErrNone},
		{"if-none-match miss", s3_constants.AmzRenameSourceIfNoneMatch, "0000", s3err.ErrNone},
		{"if-none-match hit", s3_constants.AmzRenameSourceIfNoneMatch, "d41d8cd98f00b204e9800998ecf8427e", s3err.ErrPreconditionFailed},
		// AWS documents * on the source If-None-Match as always failing.
		{"if-none-match star", s3_constants.AmzRenameSourceIfNoneMatch, "*", s3err.ErrPreconditionFailed},
		{"modified since older", s3_constants.AmzRenameSourceIfModifiedSince, mtime.Add(-time.Hour).Format(http.TimeFormat), s3err.ErrNone},
		{"modified since newer", s3_constants.AmzRenameSourceIfModifiedSince, mtime.Add(time.Hour).Format(http.TimeFormat), s3err.ErrPreconditionFailed},
		{"unmodified since newer", s3_constants.AmzRenameSourceIfUnmodifiedSince, mtime.Add(time.Hour).Format(http.TimeFormat), s3err.ErrNone},
		{"unmodified since older", s3_constants.AmzRenameSourceIfUnmodifiedSince, mtime.Add(-time.Hour).Format(http.TimeFormat), s3err.ErrPreconditionFailed},
		{"unparsable date", s3_constants.AmzRenameSourceIfModifiedSince, "not a date", s3err.ErrInvalidRequest},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r, err := http.NewRequest(http.MethodPut, "/bucket/dst.txt?renameObject", nil)
			require.NoError(t, err)
			r.Header.Set(tc.header, tc.value)

			assert.Equal(t, tc.want, validateSourceConditionalHeaders(r, entry, renameSourceConditionalHeaders))
		})
	}

	t.Run("no headers", func(t *testing.T) {
		r, err := http.NewRequest(http.MethodPut, "/bucket/dst.txt?renameObject", nil)
		require.NoError(t, err)
		assert.Equal(t, s3err.ErrNone, validateSourceConditionalHeaders(r, entry, renameSourceConditionalHeaders))
	})
}

// TestSourceConditionalHeaderPrecedence: RFC 7232 lets an ETag precondition
// settle its own side, so the date header next to it is not evaluated. AWS
// documents the same for CopyObject: a matching x-amz-copy-source-if-match with
// a failing x-amz-copy-source-if-unmodified-since copies instead of returning 412.
func TestSourceConditionalHeaderPrecedence(t *testing.T) {
	mtime := time.Date(2026, 2, 1, 12, 0, 0, 0, time.UTC)
	etag := "d41d8cd98f00b204e9800998ecf8427e"
	entry := &filer_pb.Entry{
		Attributes: &filer_pb.FuseAttributes{Mtime: mtime.Unix()},
		Extended:   map[string][]byte{s3_constants.ExtETagKey: []byte(etag)},
	}
	before := mtime.Add(-time.Hour).Format(http.TimeFormat)
	after := mtime.Add(time.Hour).Format(http.TimeFormat)

	for _, names := range []sourceConditionalHeaderNames{copySourceConditionalHeaders, renameSourceConditionalHeaders} {
		t.Run(names.ifMatch, func(t *testing.T) {
			t.Run("matched if-match outranks a failing if-unmodified-since", func(t *testing.T) {
				r, err := http.NewRequest(http.MethodPut, "/bucket/dst.txt", nil)
				require.NoError(t, err)
				r.Header.Set(names.ifMatch, etag)
				r.Header.Set(names.ifUnmodifiedSince, before)
				assert.Equal(t, s3err.ErrNone, validateSourceConditionalHeaders(r, entry, names))
			})

			t.Run("passed if-none-match outranks a failing if-modified-since", func(t *testing.T) {
				r, err := http.NewRequest(http.MethodPut, "/bucket/dst.txt", nil)
				require.NoError(t, err)
				r.Header.Set(names.ifNoneMatch, "0000")
				r.Header.Set(names.ifModifiedSince, after)
				assert.Equal(t, s3err.ErrNone, validateSourceConditionalHeaders(r, entry, names))
			})

			t.Run("a failing if-match still loses", func(t *testing.T) {
				r, err := http.NewRequest(http.MethodPut, "/bucket/dst.txt", nil)
				require.NoError(t, err)
				r.Header.Set(names.ifMatch, "0000")
				r.Header.Set(names.ifUnmodifiedSince, after)
				assert.Equal(t, s3err.ErrPreconditionFailed, validateSourceConditionalHeaders(r, entry, names))
			})
		})
	}
}

// TestRouting_RenameObject pins PUT /bucket/key?renameObject to the RenameObject
// route rather than the plain PutObject one that would otherwise match it.
func TestRouting_RenameObject(t *testing.T) {
	router := mux.NewRouter()
	setupRoutingTestServer(t).registerRouter(router)

	req, err := http.NewRequest(http.MethodPut, "http://localhost/bucket/dst.txt?renameObject", nil)
	require.NoError(t, err)
	req.Header.Set(s3_constants.AmzRenameSource, "/bucket/src.txt")

	var match mux.RouteMatch
	require.True(t, router.Match(req, &match), "no route matched")

	queries, err := match.Route.GetQueriesTemplates()
	require.NoError(t, err)
	assert.Equal(t, []string{"renameObject="}, queries)
}
