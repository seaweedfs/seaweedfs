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

func TestRenameSourceObject(t *testing.T) {
	tests := []struct {
		name       string
		source     string
		wantObject string
		wantErr    s3err.ErrorCode
	}{
		{"bucket qualified", "/bucket/dir/key.txt", "dir/key.txt", s3err.ErrNone},
		{"no leading slash", "bucket/key.txt", "key.txt", s3err.ErrNone},
		{"percent encoded", "/bucket/a%20b.txt", "a b.txt", s3err.ErrNone},
		{"plus stays literal", "/bucket/a+b.txt", "a+b.txt", s3err.ErrNone},
		{"duplicate slashes collapse", "/bucket//dir//key.txt", "dir/key.txt", s3err.ErrNone},
		{"missing header", "", "", s3err.ErrInvalidRenameSource},
		{"key without bucket", "/key.txt", "", s3err.ErrInvalidRenameSource},
		{"another bucket", "/other/key.txt", "", s3err.ErrInvalidRenameSource},
		{"empty key", "/bucket/", "", s3err.ErrInvalidRenameSource},
		{"parent traversal", "/bucket/../other/key.txt", "", s3err.ErrInvalidRenameSource},
		{"encoded parent traversal", "/bucket/%2e%2e/other/key.txt", "", s3err.ErrInvalidRenameSource},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r, err := http.NewRequest(http.MethodPut, "/bucket/dst.txt?renameObject", nil)
			require.NoError(t, err)
			if tc.source != "" {
				r.Header.Set(s3_constants.AmzRenameSource, tc.source)
			}

			object, errCode := renameSourceObject(r, "bucket")
			assert.Equal(t, tc.wantErr, errCode)
			assert.Equal(t, tc.wantObject, object)
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
