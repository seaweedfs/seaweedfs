package s3api

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
	"github.com/stretchr/testify/require"
)

func (f *fakeVolumeServer) storedCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.stored)
}

// An invalid x-amz-storage-class must be rejected before the body is
// uploaded, so a failed PUT or UploadPart leaves no chunks behind.
func TestPutToFilerStorageClass(t *testing.T) {
	for _, tt := range []struct {
		name       string
		class      string
		part       bool
		wantCode   s3err.ErrorCode
		wantStored int
	}{
		{name: "invalid", class: "BOGUS", wantCode: s3err.ErrInvalidStorageClass},
		{name: "invalid on a part", class: "BOGUS", part: true, wantCode: s3err.ErrInvalidStorageClass},
		{name: "valid", class: "STANDARD_IA", wantCode: s3err.ErrNone, wantStored: 1},
		{name: "absent", wantCode: s3err.ErrNone, wantStored: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			volume := startFakeVolumeServer(t)
			filer := &ambiguousPutFiler{volume: volume, apply: true, entries: map[string]*filer_pb.Entry{}}
			s3a := newPutTestServer(t, startFakeFiler(t, filer))

			r := httptest.NewRequest(http.MethodPut, "/b/o", nil)
			if tt.class != "" {
				r.Header.Set(s3_constants.AmzStorageClass, tt.class)
			}
			filePath, object, dest := "/buckets/b/o", "o", ""
			if tt.part {
				r = withMultipartPart(r)
				filePath, object, dest = "/buckets/b/.uploads/u/0001.part", "", "/buckets/b/o"
			}
			_, code, _ := s3a.putToFiler(r, filePath, strings.NewReader("hello world"), "b", object, 1, 0, nil, false, dest)

			require.Equal(t, tt.wantCode, code)
			require.Equal(t, tt.wantStored, volume.storedCount(), "chunks stored on the volume server")
			if tt.wantCode != s3err.ErrNone {
				require.Empty(t, filer.entries)
				return
			}
			entry := filer.entries["/buckets/b/o"]
			require.NotNil(t, entry)
			require.Equal(t, tt.class, string(entry.Extended[s3_constants.AmzStorageClass]))
		})
	}
}

// countingBody counts the reads a handler makes of the request body.
type countingBody struct {
	r     io.Reader
	reads atomic.Int32
}

func (b *countingBody) Read(p []byte) (int, error) { b.reads.Add(1); return b.r.Read(p) }
func (b *countingBody) Close() error               { return nil }

// drainProbe is the ResponseWriter of these tests. WriteErrorResponse drains
// the body before it answers, which would count as reads, so the probe notes
// how many the handler had made when that drain starts.
type drainProbe struct {
	*httptest.ResponseRecorder
	body         *countingBody
	readsAtDrain int32
}

func (p *drainProbe) SetReadDeadline(time.Time) error {
	if p.readsAtDrain < 0 {
		p.readsAtDrain = p.body.reads.Load()
	}
	return nil
}

func newHeaderTestServer(t *testing.T, versioning string) (*S3ApiServer, *ambiguousPutFiler, *fakeVolumeServer) {
	t.Helper()
	util_http.InitGlobalHttpClient()
	volume := startFakeVolumeServer(t)
	filer := &ambiguousPutFiler{volume: volume, apply: true, entries: map[string]*filer_pb.Entry{}}
	s3a := newPutTestServer(t, startFakeFiler(t, filer))
	s3a.iam = &IdentityAccessManagement{}
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	s3a.bucketConfigCache.Set("b", &BucketConfig{Name: "b", Versioning: versioning})
	return s3a, filer, volume
}

// newHeaderTestRequest builds a request without a Content-Type, so a handler
// has to sniff one from the body.
func newHeaderTestRequest(target, object, payload, header, value string) (*http.Request, *drainProbe) {
	body := &countingBody{r: strings.NewReader(payload)}
	req := httptest.NewRequest(http.MethodPut, target, body)
	req.ContentLength = int64(len(payload))
	req.Header.Set(header, value)
	req = mux.SetURLVars(req, map[string]string{"bucket": "b", "object": object})
	return req, &drainProbe{ResponseRecorder: httptest.NewRecorder(), body: body, readsAtDrain: -1}
}

func tagHeader(n int) string {
	tags := make([]string, n)
	for i := range tags {
		tags[i] = fmt.Sprintf("k%d=v", i)
	}
	return strings.Join(tags, "&")
}

var invalidPutHeaders = []struct {
	name, header, value string
	want                s3err.ErrorCode
}{
	{"storage class", s3_constants.AmzStorageClass, "BOGUS", s3err.ErrInvalidStorageClass},
	{"11 tags", s3_constants.AmzObjectTagging, tagHeader(11), s3err.ErrInvalidTag},
}

func requireRejected(t *testing.T, probe *drainProbe, volume *fakeVolumeServer, want s3err.ErrorCode) {
	t.Helper()
	apiErr := s3err.GetAPIError(want)
	require.Equal(t, apiErr.HTTPStatusCode, probe.Code, probe.Body.String())
	require.Contains(t, probe.Body.String(), "<Code>"+apiErr.Code+"</Code>")
	require.Zero(t, probe.readsAtDrain, "body reads before the error response")
	require.Zero(t, volume.storedCount(), "chunks stored on the volume server")
}

// An invalid storage class or tag set is answered before the handler touches
// the body, whichever way the bucket versions: the content-type sniff of a PUT
// without Content-Type blocks on the first read.
func TestPutObjectRejectsInvalidHeadersBeforeReadingBody(t *testing.T) {
	for _, state := range []struct{ name, versioning string }{
		{"unversioned", ""},
		{"versioning enabled", s3_constants.VersioningEnabled},
		{"versioning suspended", s3_constants.VersioningSuspended},
	} {
		for _, h := range invalidPutHeaders {
			t.Run(state.name+"/"+h.name, func(t *testing.T) {
				s3a, filer, volume := newHeaderTestServer(t, state.versioning)
				req, probe := newHeaderTestRequest("/b/o", "o", "hello world", h.header, h.value)

				s3a.PutObjectHandler(probe, req)

				requireRejected(t, probe, volume, h.want)
				filer.mu.Lock()
				defer filer.mu.Unlock()
				require.Empty(t, filer.entries)
			})
		}
	}
}

func TestPutObjectPartRejectsInvalidHeadersBeforeReadingBody(t *testing.T) {
	for _, h := range invalidPutHeaders {
		t.Run(h.name, func(t *testing.T) {
			s3a, filer, volume := newHeaderTestServer(t, "")
			uploadID := s3a.generateUploadID("o")
			filer.mu.Lock()
			filer.entries["/buckets/b/.uploads/"+uploadID] = &filer_pb.Entry{
				Name: uploadID, IsDirectory: true,
				Extended: map[string][]byte{s3_constants.ExtMultipartObjectKey: []byte("o")},
			}
			filer.mu.Unlock()
			req, probe := newHeaderTestRequest("/b/o?partNumber=1&uploadId="+uploadID, "o", "hello world", h.header, h.value)

			s3a.PutObjectPartHandler(probe, req)

			requireRejected(t, probe, volume, h.want)
		})
	}
}

// A directory marker is validated like any other PUT.
func TestPutObjectDirectoryMarkerRejectsInvalidHeaders(t *testing.T) {
	for _, h := range invalidPutHeaders {
		t.Run(h.name, func(t *testing.T) {
			s3a, filer, volume := newHeaderTestServer(t, "")
			req, probe := newHeaderTestRequest("/b/dir/", "dir/", "", h.header, h.value)

			s3a.PutObjectHandler(probe, req)

			requireRejected(t, probe, volume, h.want)
			filer.mu.Lock()
			defer filer.mu.Unlock()
			require.Nil(t, filer.entries["/buckets/b/dir"], "directory marker created")
		})
	}
}

func TestPutObjectDirectoryMarkerStoresHeaders(t *testing.T) {
	s3a, filer, _ := newHeaderTestServer(t, "")
	req, probe := newHeaderTestRequest("/b/dir/", "dir/", "", s3_constants.AmzStorageClass, "STANDARD_IA")
	req.Header.Set(s3_constants.AmzObjectTagging, "k1=v1&k2=v2")

	s3a.PutObjectHandler(probe, req)

	require.Equal(t, http.StatusOK, probe.Code, probe.Body.String())
	filer.mu.Lock()
	entry := filer.entries["/buckets/b/dir"]
	require.NotNil(t, entry)
	require.Equal(t, "STANDARD_IA", string(entry.Extended[s3_constants.AmzStorageClass]))
	require.Equal(t, "v1", string(entry.Extended[S3TAG_PREFIX+"k1"]))
	require.Equal(t, "v2", string(entry.Extended[S3TAG_PREFIX+"k2"]))
	filer.mu.Unlock()

	getReq := httptest.NewRequest(http.MethodGet, "/b/dir/?tagging", nil)
	getReq = mux.SetURLVars(getReq, map[string]string{"bucket": "b", "object": "dir/"})
	rec := httptest.NewRecorder()
	s3a.GetObjectTaggingHandler(rec, getReq)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var tagging Tagging
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &tagging))
	require.ElementsMatch(t, []Tag{{Key: "k1", Value: "v1"}, {Key: "k2", Value: "v2"}}, tagging.TagSet.Tag)
}

// A valid storage class still stores, and the content type is still sniffed.
func TestPutObjectValidStorageClassSniffsContentType(t *testing.T) {
	s3a, filer, volume := newHeaderTestServer(t, "")
	req, probe := newHeaderTestRequest("/b/o", "o", "hello world", s3_constants.AmzStorageClass, "STANDARD_IA")

	s3a.PutObjectHandler(probe, req)

	require.Equal(t, http.StatusOK, probe.Code, probe.Body.String())
	require.Equal(t, 1, volume.storedCount())
	filer.mu.Lock()
	defer filer.mu.Unlock()
	entry := filer.entries["/buckets/b/o"]
	require.NotNil(t, entry)
	require.Equal(t, "STANDARD_IA", string(entry.Extended[s3_constants.AmzStorageClass]))
	require.Equal(t, "text/plain; charset=utf-8", entry.Attributes.Mime)
}

// CreateMultipartUpload stores the storage class for the completed object,
// so it must reject an invalid one as PutObject and CopyObject do.
func TestParseS3MetadataRejectsInvalidStorageClass(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "http://s3/b/o?uploads", nil)
	req.Header.Set(s3_constants.AmzStorageClass, "BOGUS")
	_, code := ParseS3Metadata(req, nil, false)
	require.Equal(t, s3err.ErrInvalidStorageClass, code)

	req.Header.Set(s3_constants.AmzStorageClass, "GLACIER")
	metadata, code := ParseS3Metadata(req, nil, false)
	require.Equal(t, s3err.ErrNone, code)
	require.Equal(t, "GLACIER", string(metadata[s3_constants.AmzStorageClass]))
}
