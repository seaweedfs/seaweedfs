package s3api

import (
	"context"
	"encoding/xml"
	"fmt"
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
	weed_server "github.com/seaweedfs/seaweedfs/weed/server"
)

const keepAliveSrc, keepAliveDst, keepAliveObject = "ka-src", "ka-dst", "object"

// keepAliveCopyServer serves a chunked source object, so CopyObject and
// UploadPartCopy take their data-moving paths, with the keep-alive armed
// after delay and the copy itself replaced by copy (a slow or failing volume).
func keepAliveCopyServer(t *testing.T, delay time.Duration, dstVersioning string, work func() error) (*S3ApiServer, *copyACLFiler, *atomic.Int32) {
	t.Helper()
	oldDelay, oldInterval, oldObject, oldPart, oldReencrypt, oldFill := copyKeepAliveDelay, copyKeepAliveInterval, copyObjectData, copyPartData, copyPartReencrypt, copyRemoteFill
	t.Cleanup(func() {
		copyKeepAliveDelay, copyKeepAliveInterval, copyObjectData, copyPartData, copyPartReencrypt, copyRemoteFill = oldDelay, oldInterval, oldObject, oldPart, oldReencrypt, oldFill
	})
	copyKeepAliveDelay, copyKeepAliveInterval = delay, 10*time.Millisecond
	calls := &atomic.Int32{}
	copied := []*filer_pb.FileChunk{{FileId: "3,0200000000000002", Offset: 0, Size: 4}}
	copyObjectData = func(*S3ApiServer, *filer_pb.Entry, *http.Request, string, string, string, string, bool) ([]*filer_pb.FileChunk, map[string][]byte, error) {
		calls.Add(1)
		if err := work(); err != nil {
			return nil, nil, err
		}
		return copied, nil, nil
	}
	copyPartReencrypt = func(*S3ApiServer, *http.Request, *filer_pb.Entry, int64, int64, string, string, string, int, *filer_pb.Entry) (string, SSEResponseMetadata, s3err.ErrorCode) {
		calls.Add(1)
		if err := work(); err != nil {
			return "", SSEResponseMetadata{}, s3err.ErrInternalError
		}
		return "0123456789abcdef0123456789abcdef", SSEResponseMetadata{ChecksumHeaderName: "X-Amz-Checksum-Crc32", ChecksumValue: "AAAAAA=="}, s3err.ErrNone
	}
	copyPartData = func(*S3ApiServer, *filer_pb.Entry, int64, int64, string) ([]*filer_pb.FileChunk, []byte, error) {
		calls.Add(1)
		if err := work(); err != nil {
			return nil, nil, err
		}
		return copied, []byte("0123456789abcdef"), nil
	}

	filer := &copyACLFiler{ambiguousPutFiler: &ambiguousPutFiler{apply: true, entries: map[string]*filer_pb.Entry{}}}
	s3a := newPutTestServer(t, startFakeFiler(t, filer))
	s3a.iam = &IdentityAccessManagement{}
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	s3a.bucketConfigCache.Set(keepAliveSrc, &BucketConfig{Name: keepAliveSrc, Ownership: s3_constants.OwnershipObjectWriter})
	s3a.bucketConfigCache.Set(keepAliveDst, &BucketConfig{Name: keepAliveDst, Versioning: dstVersioning, Ownership: s3_constants.OwnershipObjectWriter})
	filer.entries["/buckets/"+keepAliveSrc+"/"+keepAliveObject] = &filer_pb.Entry{
		Name:       keepAliveObject,
		Attributes: &filer_pb.FuseAttributes{FileMode: defaultFileMode, FileSize: 4, Md5: []byte("0123456789abcdef")},
		Chunks:     []*filer_pb.FileChunk{{FileId: "3,0100000000000001", Offset: 0, Size: 4}},
	}
	return s3a, filer, calls
}

func copyObjectRequest(source string) *http.Request {
	req := httptest.NewRequest(http.MethodPut, "/"+keepAliveDst+"/"+keepAliveObject, nil)
	req = mux.SetURLVars(req, map[string]string{"bucket": keepAliveDst, "object": keepAliveObject})
	req.Header.Set("X-Amz-Copy-Source", source)
	return req
}

// partCopyRequest opens an upload carrying extended (SSE, checksum) and
// returns an UploadPartCopy of the source into its part 1.
func partCopyRequest(s3a *S3ApiServer, filer *copyACLFiler, extended map[string][]byte) *http.Request {
	uploadID := s3a.generateUploadID(keepAliveObject)
	ext := map[string][]byte{s3_constants.ExtMultipartObjectKey: []byte(keepAliveObject)}
	for k, v := range extended {
		ext[k] = v
	}
	filer.entries["/buckets/"+keepAliveDst+"/.uploads/"+uploadID] = &filer_pb.Entry{Name: uploadID, IsDirectory: true, Extended: ext}
	req := httptest.NewRequest(http.MethodPut, "/"+keepAliveDst+"/"+keepAliveObject+"?partNumber=1&uploadId="+uploadID, nil)
	req = mux.SetURLVars(req, map[string]string{"bucket": keepAliveDst, "object": keepAliveObject})
	req.Header.Set("X-Amz-Copy-Source", "/"+keepAliveSrc+"/"+keepAliveObject)
	return req
}

func slowCopy() error { time.Sleep(150 * time.Millisecond); return nil }

// keptAliveBody checks the shape of a kept-alive answer: 200, the XML
// declaration flushed first, whitespace while the copy ran, then the
// document; it returns that document.
func keptAliveBody(t *testing.T, rr *httptest.ResponseRecorder) string {
	t.Helper()
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body: %q", rr.Code, rr.Body.String())
	}
	if got := rr.Header().Get("Content-Type"); got != "application/xml" {
		t.Fatalf("Content-Type = %q", got)
	}
	if rr.Header().Get("Content-Length") != "" {
		t.Fatalf("a kept-alive answer cannot know its length up front")
	}
	body := rr.Body.String()
	rest, ok := strings.CutPrefix(body, xml.Header)
	if !ok {
		t.Fatalf("body does not open with the XML declaration: %q", body)
	}
	doc := strings.TrimLeft(rest, " ")
	if len(rest)-len(doc) < 2 {
		t.Fatalf("no keep-alive whitespace before the document: %q", body)
	}
	return doc
}

// A copy that outlasts the delay answers 200 at once and keeps the connection
// alive; the result a standard decoder (and the AWS SDKs, which skip leading
// whitespace) reads is the CopyObjectResult.
func TestCopyObjectKeepAliveSlowCopySucceeds(t *testing.T) {
	s3a, filer, _ := keepAliveCopyServer(t, 20*time.Millisecond, "", slowCopy)
	rr := httptest.NewRecorder()
	s3a.CopyObjectHandler(rr, copyObjectRequest("/"+keepAliveSrc+"/"+keepAliveObject))
	doc := keptAliveBody(t, rr)
	if !strings.HasPrefix(doc, "<CopyObjectResult") {
		t.Fatalf("document = %q, want a CopyObjectResult", doc)
	}
	var result CopyObjectResult
	if err := xml.NewDecoder(strings.NewReader(rr.Body.String())).Decode(&result); err != nil {
		t.Fatalf("decode %q: %v", rr.Body.String(), err)
	}
	if result.ETag == "" {
		t.Fatalf("CopyObjectResult carries no ETag: %q", rr.Body.String())
	}
	if filer.entries["/buckets/"+keepAliveDst+"/"+keepAliveObject] == nil {
		t.Fatalf("the destination was not written")
	}
}

// A slow copy that fails has already sent its 200: the failure follows as the
// <Error> document with the code the handler answers it with (a read-only
// volume is AccessDenied), which the SDKs raise as that error.
func TestCopyObjectKeepAliveSlowCopyFails(t *testing.T) {
	s3a, filer, _ := keepAliveCopyServer(t, 20*time.Millisecond, "", func() error {
		time.Sleep(150 * time.Millisecond)
		return fmt.Errorf("assign volume: %w", weed_server.ErrReadOnly)
	})
	rr := httptest.NewRecorder()
	s3a.CopyObjectHandler(rr, copyObjectRequest("/"+keepAliveSrc+"/"+keepAliveObject))
	doc := keptAliveBody(t, rr)
	var e s3err.RESTErrorResponse
	if err := xml.NewDecoder(strings.NewReader(doc)).Decode(&e); err != nil {
		t.Fatalf("decode %q: %v", doc, err)
	}
	if e.Code != "AccessDenied" || e.RequestID == "" || e.Resource == "" {
		t.Fatalf("error document = %+v, want AccessDenied with its request id and resource", e)
	}
	if rr.Header().Get("x-amz-request-id") != e.RequestID {
		t.Fatalf("request id header %q differs from the document's %q", rr.Header().Get("x-amz-request-id"), e.RequestID)
	}
	if filer.entries["/buckets/"+keepAliveDst+"/"+keepAliveObject] != nil {
		t.Fatalf("a failed copy wrote the destination")
	}
}

// A copy that ends before the delay answers exactly as before: one document
// with its length, no leading whitespace, the ETag header set.
func TestCopyObjectKeepAliveFastCopyUnchanged(t *testing.T) {
	s3a, _, calls := keepAliveCopyServer(t, time.Second, "", func() error { return nil })
	rr := httptest.NewRecorder()
	s3a.CopyObjectHandler(rr, copyObjectRequest("/"+keepAliveSrc+"/"+keepAliveObject))
	if calls.Load() != 1 {
		t.Fatalf("copy ran %d times, want 1", calls.Load())
	}
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d; body: %q", rr.Code, rr.Body.String())
	}
	if !strings.HasPrefix(rr.Body.String(), xml.Header+"<CopyObjectResult") {
		t.Fatalf("body = %q, want the declaration then the result", rr.Body.String())
	}
	if rr.Header().Get("Content-Length") == "" || len(rr.Header()["ETag"]) == 0 {
		t.Fatalf("fast answer lost its headers: %v", rr.Header())
	}
}

// A refusal comes before the keep-alive arms: it keeps its own status even
// when the delay is long past.
func TestCopyObjectKeepAliveRefusalKeepsStatus(t *testing.T) {
	s3a, _, calls := keepAliveCopyServer(t, time.Nanosecond, "", slowCopy)
	rr := httptest.NewRecorder()
	s3a.CopyObjectHandler(rr, copyObjectRequest("/"+keepAliveSrc+"/missing"))
	if rr.Code != http.StatusNotFound || !strings.Contains(rr.Body.String(), "<Code>NoSuchKey</Code>") {
		t.Fatalf("status = %d, body %q; want 404 NoSuchKey", rr.Code, rr.Body.String())
	}
	if strings.Contains(rr.Body.String(), "> <") || calls.Load() != 0 {
		t.Fatalf("a refused copy reached the copy step or the keep-alive")
	}
}

// UploadPartCopy keeps a slow part copy alive the same way, ending in the
// CopyPartResult.
func TestCopyObjectPartKeepAliveSlowCopySucceeds(t *testing.T) {
	s3a, filer, _ := keepAliveCopyServer(t, 20*time.Millisecond, "", slowCopy)
	rr := httptest.NewRecorder()
	s3a.CopyObjectPartHandler(rr, partCopyRequest(s3a, filer, nil))
	doc := keptAliveBody(t, rr)
	if !strings.HasPrefix(doc, "<CopyPartResult") {
		t.Fatalf("document = %q, want a CopyPartResult", doc)
	}
	var result CopyPartResult
	if err := xml.NewDecoder(strings.NewReader(rr.Body.String())).Decode(&result); err != nil || result.ETag == "" {
		t.Fatalf("decode %q: %v (ETag %q)", rr.Body.String(), err, result.ETag)
	}
}

// A versioning destination mints x-amz-version-id only once the copy is
// finalized, so a slow copy there keeps the single answer carrying it.
func TestCopyObjectKeepAliveVersionedDestinationKeepsVersionHeader(t *testing.T) {
	s3a, filer, _ := keepAliveCopyServer(t, 20*time.Millisecond, s3_constants.VersioningEnabled, slowCopy)
	// The fake filer makes no parent directories; the pointer lives on this one.
	filer.entries["/buckets/"+keepAliveDst+"/"+keepAliveObject+s3_constants.VersionsFolder] = &filer_pb.Entry{
		Name: keepAliveObject + s3_constants.VersionsFolder, IsDirectory: true, Extended: map[string][]byte{},
	}
	rr := httptest.NewRecorder()
	s3a.CopyObjectHandler(rr, copyObjectRequest("/"+keepAliveSrc+"/"+keepAliveObject))
	if rr.Code != http.StatusOK || rr.Header().Get("x-amz-version-id") == "" {
		t.Fatalf("status = %d, x-amz-version-id = %q; body %q", rr.Code, rr.Header().Get("x-amz-version-id"), rr.Body.String())
	}
	if !strings.HasPrefix(rr.Body.String(), xml.Header+"<CopyObjectResult") {
		t.Fatalf("body = %q, want one document", rr.Body.String())
	}
}

// An SSE-C upload answers the customer-key echo headers once the part is
// written, so a slow part copy into it keeps the single answer carrying them.
func TestCopyObjectPartKeepAliveSSECUploadKeepsHeaders(t *testing.T) {
	s3a, filer, _ := keepAliveCopyServer(t, 20*time.Millisecond, "", slowCopy)
	// The checksum sends it down the re-streaming path, which answers SSE headers.
	req := partCopyRequest(s3a, filer, map[string][]byte{
		s3_constants.ExtChecksumAlgorithm:                     []byte("X-Amz-Checksum-Crc32"),
		s3_constants.AmzServerSideEncryptionCustomerAlgorithm: []byte("AES256"),
		s3_constants.AmzServerSideEncryptionCustomerKeyMD5:    []byte("bWQ1"),
	})
	req.Header.Set(s3_constants.AmzServerSideEncryptionCustomerAlgorithm, "AES256")
	req.Header.Set(s3_constants.AmzServerSideEncryptionCustomerKeyMD5, "bWQ1")
	copyPartReencrypt = func(*S3ApiServer, *http.Request, *filer_pb.Entry, int64, int64, string, string, string, int, *filer_pb.Entry) (string, SSEResponseMetadata, s3err.ErrorCode) {
		time.Sleep(150 * time.Millisecond)
		return "0123456789abcdef0123456789abcdef", SSEResponseMetadata{SSEType: s3_constants.SSETypeC}, s3err.ErrNone
	}
	rr := httptest.NewRecorder()
	s3a.CopyObjectPartHandler(rr, req)
	if rr.Code != http.StatusOK || rr.Header().Get(s3_constants.AmzServerSideEncryptionCustomerAlgorithm) != "AES256" {
		t.Fatalf("status = %d, SSE-C algorithm header = %q; body %q", rr.Code, rr.Header().Get(s3_constants.AmzServerSideEncryptionCustomerAlgorithm), rr.Body.String())
	}
	if !strings.HasPrefix(rr.Body.String(), xml.Header+"<CopyPartResult") {
		t.Fatalf("body = %q, want one document", rr.Body.String())
	}
}

// A slow part copy into an upload with a checksum algorithm is kept alive and
// still hands the client its checksum, in the CopyPartResult as AWS does.
func TestCopyObjectPartKeepAliveChecksumInBody(t *testing.T) {
	s3a, filer, _ := keepAliveCopyServer(t, 20*time.Millisecond, "", slowCopy)
	req := partCopyRequest(s3a, filer, map[string][]byte{s3_constants.ExtChecksumAlgorithm: []byte("X-Amz-Checksum-Crc32")})
	rr := httptest.NewRecorder()
	s3a.CopyObjectPartHandler(rr, req)
	doc := keptAliveBody(t, rr)
	if !strings.Contains(doc, "<ChecksumCRC32>AAAAAA==</ChecksumCRC32>") {
		t.Fatalf("CopyPartResult lost the checksum: %q", doc)
	}
}

// A remote-only source is filled after the keep-alive arms: on a cell whose
// disk is a cache, that fill is the slow part.
func TestCopyObjectKeepAliveCoversRemoteFill(t *testing.T) {
	s3a, filer, calls := keepAliveCopyServer(t, 20*time.Millisecond, "", func() error { return nil })
	key := "/buckets/" + keepAliveSrc + "/" + keepAliveObject
	filled := filer.entries[key]
	remote := &filer_pb.RemoteEntry{RemoteSize: 4, RemoteETag: "r", RemoteMtime: 1}
	filled.RemoteEntry = remote
	filer.entries[key] = &filer_pb.Entry{Name: keepAliveObject, Attributes: filled.Attributes, RemoteEntry: remote}
	copyRemoteFill = func(_ *S3ApiServer, _ context.Context, src *filer_pb.Entry, _, _, _ string) *filer_pb.Entry {
		if !src.IsInRemoteOnly() {
			t.Errorf("fill called for a local source")
		}
		time.Sleep(150 * time.Millisecond)
		return filled
	}
	rr := httptest.NewRecorder()
	s3a.CopyObjectHandler(rr, copyObjectRequest("/"+keepAliveSrc+"/"+keepAliveObject))
	if doc := keptAliveBody(t, rr); !strings.HasPrefix(doc, "<CopyObjectResult") {
		t.Fatalf("document = %q, want a CopyObjectResult", doc)
	}
	if calls.Load() != 1 {
		t.Fatalf("the filled source was not copied")
	}
}

// A client that goes away after the 200 ends the keep-alive: the handler
// finishes the copy and returns, and nothing panics or races.
func TestCopyObjectKeepAliveClientGone(t *testing.T) {
	req := copyObjectRequest("/" + keepAliveSrc + "/" + keepAliveObject)
	ctx, cancel := context.WithCancel(req.Context())
	defer cancel()
	req = req.WithContext(ctx)
	s3a, filer, _ := keepAliveCopyServer(t, 20*time.Millisecond, "", func() error {
		time.Sleep(60 * time.Millisecond)
		cancel()
		time.Sleep(60 * time.Millisecond)
		return nil
	})
	rr := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		defer close(done)
		s3a.CopyObjectHandler(rr, req)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatalf("the handler did not return after the client left")
	}
	if !strings.HasPrefix(rr.Body.String(), xml.Header) || filer.entries["/buckets/"+keepAliveDst+"/"+keepAliveObject] == nil {
		t.Fatalf("the copy did not finish after the client left: %q", rr.Body.String())
	}
}

// A refusal the request earns keeps its status however long the checks
// before it took: a slow source lookup ahead of a bad x-amz-tagging is still
// 400 InvalidTag, not a 200 with an <Error> in it.
func TestCopyObjectKeepAliveSlowLookupThenBadTagKeepsStatus(t *testing.T) {
	s3a, filer, calls := keepAliveCopyServer(t, 20*time.Millisecond, "", slowCopy)
	filer.afterLookup = func(*filer_pb.LookupDirectoryEntryRequest) { time.Sleep(200 * time.Millisecond) }
	req := copyObjectRequest("/" + keepAliveSrc + "/" + keepAliveObject)
	req.Header.Set(s3_constants.AmzObjectTaggingDirective, "REPLACE")
	req.Header.Set(s3_constants.AmzObjectTagging, strings.Repeat("k", 200)+"=v")
	rr := httptest.NewRecorder()
	s3a.CopyObjectHandler(rr, req)
	if rr.Code != http.StatusBadRequest || !strings.Contains(rr.Body.String(), "<Code>InvalidTag</Code>") {
		t.Fatalf("status = %d, body %q; want 400 InvalidTag", rr.Code, rr.Body.String())
	}
	if calls.Load() != 0 {
		t.Fatalf("a refused copy reached the copy step")
	}
}

// The metadata-only self-copy moves no data and is never kept alive: a
// source condition that fails on the snapshot read under the write lock,
// after a slow wait, still answers 412.
func TestCopyObjectSelfCopyConditionUnderLockKeeps412(t *testing.T) {
	s3a, filer, _ := keepAliveCopyServer(t, 20*time.Millisecond, "", slowCopy)
	key := "/buckets/" + keepAliveSrc + "/" + keepAliveObject
	filer.entries[key].Extended = map[string][]byte{s3_constants.ExtETagKey: []byte("aaaa")}
	lookups := 0
	filer.afterLookup = func(req *filer_pb.LookupDirectoryEntryRequest) {
		if req.Directory+"/"+req.Name != key {
			return
		}
		if lookups++; lookups == 1 {
			// a concurrent writer replaces the object right after the first read
			filer.entries[key].Extended[s3_constants.ExtETagKey] = []byte("bbbb")
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	req := httptest.NewRequest(http.MethodPut, "/"+keepAliveSrc+"/"+keepAliveObject, nil)
	req = mux.SetURLVars(req, map[string]string{"bucket": keepAliveSrc, "object": keepAliveObject})
	req.Header.Set("X-Amz-Copy-Source", "/"+keepAliveSrc+"/"+keepAliveObject)
	req.Header.Set(s3_constants.AmzUserMetaDirective, "REPLACE")
	req.Header.Set("X-Amz-Copy-Source-If-Match", `"aaaa"`)
	rr := httptest.NewRecorder()
	s3a.CopyObjectHandler(rr, req)
	filer.mu.Lock()
	defer filer.mu.Unlock()
	if lookups < 2 {
		t.Fatalf("the self-copy did not re-read its source under the lock (%d lookups)", lookups)
	}
	if rr.Code != http.StatusPreconditionFailed {
		t.Fatalf("status = %d, body %q; want 412", rr.Code, rr.Body.String())
	}
}

// UploadPartCopy checks a remote-only source's range against the remote
// object's size: its local attributes can be stale until the fill.
func TestCopyObjectPartRangeOnRemoteOnlySourceUsesRemoteSize(t *testing.T) {
	s3a, filer, calls := keepAliveCopyServer(t, time.Second, "", func() error { return nil })
	key := "/buckets/" + keepAliveSrc + "/" + keepAliveObject
	filled := filer.entries[key]
	remote := &filer_pb.RemoteEntry{RemoteSize: 4, RemoteETag: "r", RemoteMtime: 1}
	filled.RemoteEntry = remote
	filled.Attributes.Mtime = 2
	// stale local size, and a remote mtime that does not postdate it
	filer.entries[key] = &filer_pb.Entry{Name: keepAliveObject, Attributes: &filer_pb.FuseAttributes{FileMode: defaultFileMode, Mtime: 2}, RemoteEntry: remote}
	copyRemoteFill = func(*S3ApiServer, context.Context, *filer_pb.Entry, string, string, string) *filer_pb.Entry {
		return filled
	}
	req := partCopyRequest(s3a, filer, nil)
	req.Header.Set("X-Amz-Copy-Source-Range", "bytes=0-3")
	rr := httptest.NewRecorder()
	s3a.CopyObjectPartHandler(rr, req)
	if rr.Code != http.StatusOK || calls.Load() != 1 {
		t.Fatalf("status = %d, copies %d, body %q; want the range copied", rr.Code, calls.Load(), rr.Body.String())
	}
}
