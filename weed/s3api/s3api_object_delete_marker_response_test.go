package s3api

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
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
		allow                    string
	}{
		{"current version", "", http.StatusNotFound, "", ""},
		{"marker by versionId", "marker", http.StatusMethodNotAllowed, "Sun, 06 Nov 1994 08:49:37 GMT", "DELETE"},
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
			if got := w.Header().Get("Allow"); got != tc.allow {
				t.Errorf("%s %s: Allow = %q, want %q", tc.name, method, got, tc.allow)
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

func TestWriteConditionalCheckErrorDeleteMarker(t *testing.T) {
	s3a := &S3ApiServer{}
	marker := &filer_pb.Entry{
		Name:     "v_marker",
		Extended: map[string][]byte{s3_constants.ExtDeleteMarkerKey: []byte("true"), s3_constants.ExtVersionIdKey: []byte("marker")},
	}
	for _, method := range []string{http.MethodGet, http.MethodHead} {
		w := httptest.NewRecorder()
		s3a.writeConditionalCheckError(w, httptest.NewRequest(method, "/bucket/object", nil), "bucket", "object", "TestHandler", ConditionalHeaderResult{ErrorCode: s3err.ErrNoSuchKey, DeleteMarker: marker})
		require.Equal(t, http.StatusNotFound, w.Code, method)
		require.Equal(t, "true", w.Header().Get(s3_constants.AmzDeleteMarker), method)
		require.Equal(t, "marker", w.Header().Get("x-amz-version-id"), method)
	}

	w := httptest.NewRecorder()
	s3a.writeConditionalCheckError(w, httptest.NewRequest(http.MethodGet, "/bucket/object", nil), "bucket", "object", "TestHandler", ConditionalHeaderResult{ErrorCode: s3err.ErrNoSuchKey})
	require.Equal(t, http.StatusNotFound, w.Code)
	require.Empty(t, w.Header().Get(s3_constants.AmzDeleteMarker))
}

func TestWriteConditionalCheckErrorDeleteMarkerVersionFromName(t *testing.T) {
	s3a := &S3ApiServer{}
	marker := &filer_pb.Entry{
		Name:     "v_named",
		Extended: map[string][]byte{s3_constants.ExtDeleteMarkerKey: []byte("true")},
	}
	w := httptest.NewRecorder()
	s3a.writeConditionalCheckError(w, httptest.NewRequest(http.MethodGet, "/bucket/object", nil), "bucket", "object", "TestHandler", ConditionalHeaderResult{ErrorCode: s3err.ErrNoSuchKey, DeleteMarker: marker})
	require.Equal(t, http.StatusNotFound, w.Code)
	require.Equal(t, "named", w.Header().Get("x-amz-version-id"))
}

func newDeleteMarkerTestServer(t *testing.T) *S3ApiServer {
	t.Helper()
	f := &anonymousReadFiler{entries: map[string]*filer_pb.Entry{
		"/buckets/b/o.versions": {
			Name:        "o.versions",
			IsDirectory: true,
			Attributes:  &filer_pb.FuseAttributes{Mtime: 784111700},
			Extended: map[string][]byte{
				s3_constants.ExtLatestVersionIdKey:       []byte("marker"),
				s3_constants.ExtLatestVersionFileNameKey: []byte("v_marker"),
			},
		},
		"/buckets/b/o.versions/v_marker": {
			Name:       "v_marker",
			Attributes: &filer_pb.FuseAttributes{Mtime: 784111777},
			Extended: map[string][]byte{
				s3_constants.ExtDeleteMarkerKey: []byte("true"),
				s3_constants.ExtVersionIdKey:    []byte("marker"),
			},
		},
	}}
	s3a := newPutTestServer(t, startFakeFiler(t, f))
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	s3a.bucketConfigCache.Set("b", &BucketConfig{Name: "b", Versioning: s3_constants.VersioningEnabled})
	return s3a
}

func TestGetHeadDeleteMarker(t *testing.T) {
	for _, tc := range []struct {
		name, query         string
		status              int
		lastModified, allow string
	}{
		{"current marker", "", http.StatusNotFound, "", ""},
		{"marker by versionId", "?versionId=marker", http.StatusMethodNotAllowed, "Sun, 06 Nov 1994 08:49:37 GMT", "DELETE"},
	} {
		for _, method := range []string{http.MethodGet, http.MethodHead} {
			t.Run(tc.name+"/"+method, func(t *testing.T) {
				s3a := newDeleteMarkerTestServer(t)
				r := mux.SetURLVars(httptest.NewRequest(method, "/b/o"+tc.query, nil), map[string]string{"bucket": "b", "object": "o"})
				w := httptest.NewRecorder()
				handler := s3a.GetObjectHandler
				if method == http.MethodHead {
					handler = s3a.HeadObjectHandler
				}
				handler(w, r)
				require.Equal(t, tc.status, w.Code)
				require.Equal(t, "true", w.Header().Get(s3_constants.AmzDeleteMarker))
				require.Equal(t, "marker", w.Header().Get("x-amz-version-id"))
				require.Equal(t, tc.lastModified, w.Header().Get("Last-Modified"))
				require.Equal(t, tc.allow, w.Header().Get("Allow"))
			})
		}
	}
}

func TestGetHeadNullDeleteMarker(t *testing.T) {
	f := &anonymousReadFiler{entries: map[string]*filer_pb.Entry{
		"/buckets/b/o.versions": {
			Name:        "o.versions",
			IsDirectory: true,
			Attributes:  &filer_pb.FuseAttributes{Mtime: 784111700},
			Extended: map[string][]byte{
				s3_constants.ExtLatestVersionIdKey:       []byte("null"),
				s3_constants.ExtLatestVersionFileNameKey: []byte("v_null"),
			},
		},
		"/buckets/b/o.versions/v_null": {
			Name:       "v_null",
			Attributes: &filer_pb.FuseAttributes{Mtime: 784111777},
			Extended: map[string][]byte{
				s3_constants.ExtDeleteMarkerKey: []byte("true"),
				s3_constants.ExtVersionIdKey:    []byte("null"),
			},
		},
	}}
	for _, method := range []string{http.MethodGet, http.MethodHead} {
		t.Run(method, func(t *testing.T) {
			s3a := newPutTestServer(t, startFakeFiler(t, f))
			s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
			s3a.bucketConfigCache.Set("b", &BucketConfig{Name: "b", Versioning: s3_constants.VersioningSuspended})
			r := mux.SetURLVars(httptest.NewRequest(method, "/b/o?versionId=null", nil), map[string]string{"bucket": "b", "object": "o"})
			w := httptest.NewRecorder()
			handler := s3a.GetObjectHandler
			if method == http.MethodHead {
				handler = s3a.HeadObjectHandler
			}
			handler(w, r)
			require.Equal(t, http.StatusMethodNotAllowed, w.Code)
			require.Equal(t, "true", w.Header().Get(s3_constants.AmzDeleteMarker))
			require.Equal(t, "null", w.Header().Get("x-amz-version-id"))
			require.Equal(t, "DELETE", w.Header().Get("Allow"))
		})
	}
}

func TestGetHeadNullVersionAbsent(t *testing.T) {
	s3a := newDeleteMarkerTestServer(t)
	r := mux.SetURLVars(httptest.NewRequest(http.MethodGet, "/b/o?versionId=null", nil), map[string]string{"bucket": "b", "object": "o"})
	w := httptest.NewRecorder()
	s3a.GetObjectHandler(w, r)
	require.Equal(t, http.StatusNotFound, w.Code)
	require.Empty(t, w.Header().Get(s3_constants.AmzDeleteMarker))
}

func TestGetHeadDeleteMarkerConditionalMiss(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodHead} {
		t.Run(method, func(t *testing.T) {
			s3a := newDeleteMarkerTestServer(t)
			r := httptest.NewRequest(method, "/b/o", nil)
			r.Header.Set("If-Match", `"no-match"`)
			r = mux.SetURLVars(r, map[string]string{"bucket": "b", "object": "o"})
			w := httptest.NewRecorder()
			handler := s3a.GetObjectHandler
			if method == http.MethodHead {
				handler = s3a.HeadObjectHandler
			}
			handler(w, r)
			require.Equal(t, http.StatusNotFound, w.Code)
			require.Equal(t, "true", w.Header().Get(s3_constants.AmzDeleteMarker))
			require.Equal(t, "marker", w.Header().Get("x-amz-version-id"))
		})
	}
}
