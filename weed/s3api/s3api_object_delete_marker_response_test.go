package s3api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/stretchr/testify/require"
)

func TestWriteDeleteMarkerResponse(t *testing.T) {
	s3a := &S3ApiServer{}
	entry := &filer_pb.Entry{
		Name:       "v_marker",
		Attributes: &filer_pb.FuseAttributes{Mtime: 784111777},
		Extended:   map[string][]byte{s3_constants.ExtDeleteMarkerKey: []byte("true")},
	}
	for _, tc := range []struct {
		name, requestedVersionId string
		status                   int
		lastModified             string
	}{
		{"current version", "", http.StatusNotFound, ""},
		{"marker by versionId", "marker", http.StatusMethodNotAllowed, "Sun, 06 Nov 1994 08:49:37 GMT"},
	} {
		for _, method := range []string{http.MethodGet, http.MethodHead} {
			w := httptest.NewRecorder()
			s3a.writeDeleteMarkerResponse(w, httptest.NewRequest(method, "/bucket/object", nil), "bucket", "object", "TestHandler", entry, "marker", tc.requestedVersionId)
			if w.Code != tc.status {
				t.Errorf("%s %s: status %d, want %d", tc.name, method, w.Code, tc.status)
			}
			if got := w.Header().Get(s3_constants.AmzDeleteMarker); got != "true" {
				t.Errorf("%s %s: %s = %q, want true", tc.name, method, s3_constants.AmzDeleteMarker, got)
			}
			if got := w.Header().Get("x-amz-version-id"); got != "marker" {
				t.Errorf("%s %s: x-amz-version-id = %q, want marker", tc.name, method, got)
			}
			if got := w.Header().Get("Last-Modified"); got != tc.lastModified {
				t.Errorf("%s %s: Last-Modified = %q, want %q", tc.name, method, got, tc.lastModified)
			}
		}
	}
}

func TestWriteDeleteMarkerResponseAnonymous(t *testing.T) {
	s3a, _ := newAnonymousReadTestServer(t)
	require.NoError(t, s3a.policyEngine.engine.SetBucketPolicy("b", `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:ListBucket","Resource":"arn:aws:s3:::b"}]}`))
	entry := anonymousReadEntry(anonymousReadGrant("READ"))
	entry.Extended[s3_constants.ExtDeleteMarkerKey] = []byte("true")
	for _, query := range []string{"", "?versionId=m"} {
		r := mux.SetURLVars(httptest.NewRequest(http.MethodGet, "/b/o"+query, nil), map[string]string{"bucket": "b", "object": "o"})
		r, deferred := s3a.deferAnonymousObjectRead(r, s3_constants.ACTION_READ, "b", "o")
		require.True(t, deferred)
		w := httptest.NewRecorder()
		s3a.writeDeleteMarkerResponse(w, r, "b", "o", "TestHandler", entry, "m", r.URL.Query().Get("versionId"))
		require.Equal(t, http.StatusNotFound, w.Code, query)
		require.Empty(t, w.Header().Get(s3_constants.AmzDeleteMarker), query)
		require.Empty(t, w.Header().Get("x-amz-version-id"), query)
	}
}
