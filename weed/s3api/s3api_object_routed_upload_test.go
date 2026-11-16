package s3api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The gateway's own filer never receives the owner's metadata here, so every
// multipart step after CreateMultipartUpload must find the upload and its
// parts on the object's write owner.
func TestMultipartUploadOnOwnerFiler(t *testing.T) {
	util_http.InitGlobalHttpClient()
	volume := startFakeVolumeServer(t)
	owner := &ambiguousPutFiler{volume: volume, apply: true, entries: map[string]*filer_pb.Entry{}}
	replica := &ambiguousPutFiler{volume: volume, apply: true, entries: map[string]*filer_pb.Entry{}}
	ownerAddr := startFakeFiler(t, owner)
	replicaAddr := startFakeFiler(t, replica)
	s3a := newPutTestServer(t, replicaAddr)
	s3a.iam = &IdentityAccessManagement{}
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	s3a.bucketConfigCache.Set("b", &BucketConfig{Name: "b"})
	s3a.objectWriteLockClient = cluster.NewLockClient(s3a.option.GrpcDialOption, ownerAddr)
	s3a.objectWriteLockClient.SetRing([]pb.ServerAddress{ownerAddr}, 1)

	has := func(f *ambiguousPutFiler, path string) bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.entries[path] != nil
	}
	partsUnder := func(f *ambiguousPutFiler, dir string) int {
		f.mu.Lock()
		defer f.mu.Unlock()
		n := 0
		for key, entry := range f.entries {
			if strings.HasPrefix(key, dir+"/") && !entry.IsDirectory {
				n++
			}
		}
		return n
	}
	initiate := func() string {
		r := httptest.NewRequest(http.MethodPost, "/b/dir/object?uploads", nil)
		out, code := s3a.createMultipartUpload(r, &s3.CreateMultipartUploadInput{Bucket: aws.String("b"), Key: aws.String("dir/object")})
		require.Equal(t, s3err.ErrNone, code)
		uploadDir := "/buckets/b/.uploads/" + *out.UploadId
		require.True(t, has(owner, uploadDir), "the upload must be created on the owner")
		require.False(t, has(replica, uploadDir), "the gateway's filer must still be behind")
		return *out.UploadId
	}
	uploadPart := func(uploadID string, partNumber int, body string) string {
		r := httptest.NewRequest(http.MethodPut, fmt.Sprintf("/b/dir/object?partNumber=%d&uploadId=%s", partNumber, uploadID), strings.NewReader(body))
		r = mux.SetURLVars(r, map[string]string{"bucket": "b", "object": "dir/object"})
		w := httptest.NewRecorder()
		s3a.PutObjectPartHandler(w, r)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		etag := w.Header()["ETag"]
		require.Len(t, etag, 1)
		return etag[0]
	}

	uploadID := initiate()
	uploadDir := "/buckets/b/.uploads/" + uploadID
	etag1 := uploadPart(uploadID, 1, strings.Repeat("a", 5*1024*1024))
	etag2 := uploadPart(uploadID, 2, "tail")
	require.Equal(t, 2, partsUnder(owner, uploadDir), "parts must be stored next to the upload")
	require.Equal(t, 0, partsUnder(replica, uploadDir))

	listed, code := s3a.listObjectParts(&s3.ListPartsInput{
		Bucket: aws.String("b"), Key: aws.String("dir/object"), UploadId: aws.String(uploadID),
		MaxParts: aws.Int64(1000), PartNumberMarker: aws.Int64(0),
	})
	require.Equal(t, s3err.ErrNone, code)
	require.Len(t, listed.Part, 2)

	complete := httptest.NewRequest(http.MethodPost, "/b/dir/object?uploadId="+uploadID, nil)
	_, code = s3a.completeMultipartUpload(complete, &s3.CompleteMultipartUploadInput{
		Bucket: aws.String("b"), Key: aws.String("dir/object"), UploadId: aws.String(uploadID),
	}, &CompleteMultipartUpload{Parts: []CompletedPart{{ETag: etag1, PartNumber: 1}, {ETag: etag2, PartNumber: 2}}})
	require.Equal(t, s3err.ErrNone, code)
	require.True(t, has(owner, "/buckets/b/dir/object"), "the object must commit on the owner")
	require.False(t, has(owner, uploadDir), "completion must remove the upload")

	aborted := initiate()
	uploadPart(aborted, 1, "data")
	_, code = s3a.abortMultipartUpload(&s3.AbortMultipartUploadInput{
		Bucket: aws.String("b"), Key: aws.String("dir/object"), UploadId: aws.String(aborted),
	})
	require.Equal(t, s3err.ErrNone, code)
	require.False(t, has(owner, "/buckets/b/.uploads/"+aborted), "abort must remove the upload")
}

// A part create the owner stored but answered with an error must be found
// there before its chunks are freed, even though the owner is not one of the
// gateway's own filers.
func TestMultipartPartCreateErrorChecksUploadOwner(t *testing.T) {
	util_http.InitGlobalHttpClient()
	volume := startFakeVolumeServer(t)
	owner := &ambiguousPutFiler{volume: volume, apply: true, entries: map[string]*filer_pb.Entry{},
		respError: "create parent directories: i/o timeout"}
	replica := &ambiguousPutFiler{volume: volume, entries: map[string]*filer_pb.Entry{}}
	ownerAddr := startFakeFiler(t, owner)
	s3a := newPutTestServer(t, startFakeFiler(t, replica))
	s3a.objectWriteLockClient = cluster.NewLockClient(s3a.option.GrpcDialOption, ownerAddr)
	s3a.objectWriteLockClient.SetRing([]pb.ServerAddress{ownerAddr}, 1)

	r := withMultipartPart(httptest.NewRequest(http.MethodPut, "/b/dir/object?partNumber=1&uploadId=up1", nil), s3a.uploadFiler("b", "dir/object"))
	etag, code, _ := s3a.putToFiler(r, "/buckets/b/.uploads/up1/0001_x.part", strings.NewReader("part data"), "b", "", 1, 0, nil, false, "/buckets/b/dir/object")
	require.Equal(t, s3err.ErrNone, code)
	require.NotEmpty(t, etag)
	require.Empty(t, volume.deleted(), "chunks under the stored part must survive")
}

// Right after the key moves to a new owner that has not received the upload
// yet, ListParts finds the record and its parts on the prior owner.
func TestListPartsAfterOwnerChange(t *testing.T) {
	util_http.InitGlobalHttpClient()
	volume := startFakeVolumeServer(t)
	prior := &ambiguousPutFiler{volume: volume, apply: true, entries: map[string]*filer_pb.Entry{}}
	next := &ambiguousPutFiler{volume: volume, apply: true, entries: map[string]*filer_pb.Entry{}}
	priorAddr := startFakeFiler(t, prior)
	nextAddr := startFakeFiler(t, next)
	s3a := newPutTestServer(t, startFakeFiler(t, &ambiguousPutFiler{volume: volume, entries: map[string]*filer_pb.Entry{}}))
	s3a.iam = &IdentityAccessManagement{}
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	s3a.bucketConfigCache.Set("b", &BucketConfig{Name: "b"})
	s3a.objectWriteLockClient = cluster.NewLockClient(s3a.option.GrpcDialOption, priorAddr)
	s3a.objectWriteLockClient.SetRing([]pb.ServerAddress{priorAddr}, 1)

	out, code := s3a.createMultipartUpload(httptest.NewRequest(http.MethodPost, "/b/dir/object?uploads", nil),
		&s3.CreateMultipartUploadInput{Bucket: aws.String("b"), Key: aws.String("dir/object")})
	require.Equal(t, s3err.ErrNone, code)
	r := httptest.NewRequest(http.MethodPut, "/b/dir/object?partNumber=1&uploadId="+*out.UploadId, strings.NewReader("data"))
	r = mux.SetURLVars(r, map[string]string{"bucket": "b", "object": "dir/object"})
	w := httptest.NewRecorder()
	s3a.PutObjectPartHandler(w, r)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	s3a.objectWriteLockClient.SetRing([]pb.ServerAddress{nextAddr}, 2)
	require.Equal(t, priorAddr, s3a.priorWriteOwner("b", "dir/object"))

	listed, code := s3a.listObjectParts(&s3.ListPartsInput{
		Bucket: aws.String("b"), Key: aws.String("dir/object"), UploadId: out.UploadId,
		MaxParts: aws.Int64(1000), PartNumberMarker: aws.Int64(0),
	})
	require.Equal(t, s3err.ErrNone, code)
	require.Len(t, listed.Part, 1)
}

// A part routed to a new owner that has not received the upload yet makes that
// filer create a bare .uploads/<id> directory; the upload must stay open.
func TestUploadPartAfterOwnerChange(t *testing.T) {
	util_http.InitGlobalHttpClient()
	volume := startFakeVolumeServer(t)
	prior := &ambiguousPutFiler{volume: volume, apply: true, entries: map[string]*filer_pb.Entry{}}
	next := &ambiguousPutFiler{volume: volume, apply: true, entries: map[string]*filer_pb.Entry{}}
	priorAddr := startFakeFiler(t, prior)
	nextAddr := startFakeFiler(t, next)
	s3a := newPutTestServer(t, startFakeFiler(t, &ambiguousPutFiler{volume: volume, entries: map[string]*filer_pb.Entry{}}))
	s3a.iam = &IdentityAccessManagement{}
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	s3a.bucketConfigCache.Set("b", &BucketConfig{Name: "b"})
	s3a.objectWriteLockClient = cluster.NewLockClient(s3a.option.GrpcDialOption, priorAddr)
	s3a.objectWriteLockClient.SetRing([]pb.ServerAddress{priorAddr}, 1)

	out, code := s3a.createMultipartUpload(httptest.NewRequest(http.MethodPost, "/b/dir/object?uploads", nil),
		&s3.CreateMultipartUploadInput{Bucket: aws.String("b"), Key: aws.String("dir/object")})
	require.Equal(t, s3err.ErrNone, code)
	uploadDir := "/buckets/b/.uploads/" + *out.UploadId
	next.beforeCreate = func() {
		if next.entries[uploadDir] == nil {
			next.entries[uploadDir] = &filer_pb.Entry{Name: *out.UploadId, IsDirectory: true}
		}
	}
	s3a.objectWriteLockClient.SetRing([]pb.ServerAddress{nextAddr}, 2)

	r := httptest.NewRequest(http.MethodPut, "/b/dir/object?partNumber=1&uploadId="+*out.UploadId, strings.NewReader("data"))
	r = mux.SetURLVars(r, map[string]string{"bucket": "b", "object": "dir/object"})
	w := httptest.NewRecorder()
	s3a.PutObjectPartHandler(w, r)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	listed, code := s3a.listObjectParts(&s3.ListPartsInput{
		Bucket: aws.String("b"), Key: aws.String("dir/object"), UploadId: out.UploadId,
		MaxParts: aws.Int64(1000), PartNumberMarker: aws.Int64(0),
	})
	require.Equal(t, s3err.ErrNone, code)
	require.Len(t, listed.Part, 1)
}

// A new owner that received only some of the parts so far lists them together
// with the rest still on the prior owner.
func TestListPartsMergesPriorOwnerParts(t *testing.T) {
	util_http.InitGlobalHttpClient()
	volume := startFakeVolumeServer(t)
	prior := &ambiguousPutFiler{volume: volume, apply: true, entries: map[string]*filer_pb.Entry{}}
	next := &ambiguousPutFiler{volume: volume, apply: true, entries: map[string]*filer_pb.Entry{}}
	priorAddr := startFakeFiler(t, prior)
	nextAddr := startFakeFiler(t, next)
	s3a := newPutTestServer(t, startFakeFiler(t, &ambiguousPutFiler{volume: volume, entries: map[string]*filer_pb.Entry{}}))
	s3a.iam = &IdentityAccessManagement{}
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	s3a.bucketConfigCache.Set("b", &BucketConfig{Name: "b"})
	s3a.objectWriteLockClient = cluster.NewLockClient(s3a.option.GrpcDialOption, priorAddr)
	s3a.objectWriteLockClient.SetRing([]pb.ServerAddress{priorAddr}, 1)

	out, code := s3a.createMultipartUpload(httptest.NewRequest(http.MethodPost, "/b/dir/object?uploads", nil),
		&s3.CreateMultipartUploadInput{Bucket: aws.String("b"), Key: aws.String("dir/object")})
	require.Equal(t, s3err.ErrNone, code)
	uploadDir := "/buckets/b/.uploads/" + *out.UploadId
	etags := map[int]string{}
	for partNumber, body := range []string{strings.Repeat("a", 5*1024*1024), "tail"} {
		r := httptest.NewRequest(http.MethodPut, fmt.Sprintf("/b/dir/object?partNumber=%d&uploadId=%s", partNumber+1, *out.UploadId), strings.NewReader(body))
		r = mux.SetURLVars(r, map[string]string{"bucket": "b", "object": "dir/object"})
		w := httptest.NewRecorder()
		s3a.PutObjectPartHandler(w, r)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		require.Len(t, w.Header()["ETag"], 1)
		etags[partNumber+1] = w.Header()["ETag"][0]
	}

	prior.mu.Lock()
	for key, entry := range prior.entries {
		if key == uploadDir || strings.HasPrefix(key, uploadDir+"/0001") {
			next.entries[key] = entry
		}
	}
	prior.mu.Unlock()
	s3a.objectWriteLockClient.SetRing([]pb.ServerAddress{nextAddr}, 2)

	listParts := func(maxParts, marker int64) *ListPartsResult {
		listed, code := s3a.listObjectParts(&s3.ListPartsInput{
			Bucket: aws.String("b"), Key: aws.String("dir/object"), UploadId: out.UploadId,
			MaxParts: aws.Int64(maxParts), PartNumberMarker: aws.Int64(marker),
		})
		require.Equal(t, s3err.ErrNone, code)
		return listed
	}
	require.Len(t, listParts(1000, 0).Part, 2)
	first := listParts(1, 0)
	require.Len(t, first.Part, 1)
	require.Equal(t, int64(1), *first.Part[0].PartNumber)
	require.True(t, *first.IsTruncated)
	second := listParts(1, 1)
	require.Len(t, second.Part, 1)
	require.Equal(t, int64(2), *second.Part[0].PartNumber)
	require.False(t, *second.IsTruncated)

	_, code = s3a.completeMultipartUpload(httptest.NewRequest(http.MethodPost, "/b/dir/object?uploadId="+*out.UploadId, nil),
		&s3.CompleteMultipartUploadInput{Bucket: aws.String("b"), Key: aws.String("dir/object"), UploadId: out.UploadId},
		&CompleteMultipartUpload{Parts: []CompletedPart{{ETag: etags[1], PartNumber: 1}, {ETag: etags[2], PartNumber: 2}}})
	require.Equal(t, s3err.ErrNone, code)
}

type failingListFiler struct {
	*ambiguousPutFiler
}

func (f failingListFiler) ListEntries(*filer_pb.ListEntriesRequest, filer_pb.SeaweedFiler_ListEntriesServer) error {
	return status.Error(codes.Unavailable, "store unavailable")
}

// With no parts on the new owner yet, a prior owner that cannot be listed may
// hold all of them, so completion must not read the upload as empty.
func TestCompleteFailsWhenPriorOwnerListingFails(t *testing.T) {
	util_http.InitGlobalHttpClient()
	volume := startFakeVolumeServer(t)
	prior := &ambiguousPutFiler{volume: volume, apply: true, entries: map[string]*filer_pb.Entry{}}
	next := &ambiguousPutFiler{volume: volume, apply: true, entries: map[string]*filer_pb.Entry{}}
	priorAddr := startFakeFiler(t, failingListFiler{prior})
	nextAddr := startFakeFiler(t, next)
	s3a := newPutTestServer(t, startFakeFiler(t, &ambiguousPutFiler{volume: volume, entries: map[string]*filer_pb.Entry{}}))
	s3a.iam = &IdentityAccessManagement{}
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	s3a.bucketConfigCache.Set("b", &BucketConfig{Name: "b"})
	s3a.objectWriteLockClient = cluster.NewLockClient(s3a.option.GrpcDialOption, priorAddr)
	s3a.objectWriteLockClient.SetRing([]pb.ServerAddress{priorAddr}, 1)

	out, code := s3a.createMultipartUpload(httptest.NewRequest(http.MethodPost, "/b/dir/object?uploads", nil),
		&s3.CreateMultipartUploadInput{Bucket: aws.String("b"), Key: aws.String("dir/object")})
	require.Equal(t, s3err.ErrNone, code)
	r := httptest.NewRequest(http.MethodPut, "/b/dir/object?partNumber=1&uploadId="+*out.UploadId, strings.NewReader("data"))
	r = mux.SetURLVars(r, map[string]string{"bucket": "b", "object": "dir/object"})
	w := httptest.NewRecorder()
	s3a.PutObjectPartHandler(w, r)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Len(t, w.Header()["ETag"], 1)

	uploadDir := "/buckets/b/.uploads/" + *out.UploadId
	prior.mu.Lock()
	next.entries[uploadDir] = prior.entries[uploadDir]
	prior.mu.Unlock()
	s3a.objectWriteLockClient.SetRing([]pb.ServerAddress{nextAddr}, 2)

	_, code = s3a.completeMultipartUpload(httptest.NewRequest(http.MethodPost, "/b/dir/object?uploadId="+*out.UploadId, nil),
		&s3.CompleteMultipartUploadInput{Bucket: aws.String("b"), Key: aws.String("dir/object"), UploadId: out.UploadId},
		&CompleteMultipartUpload{Parts: []CompletedPart{{ETag: w.Header()["ETag"][0], PartNumber: 1}}})
	require.Equal(t, s3err.ErrInternalError, code)
}

// A prior owner that cannot be asked for the upload record must not make an
// upload the new owner has not received yet read as gone.
func TestUploadLookupFailsWhenPriorOwnerUnreachable(t *testing.T) {
	util_http.InitGlobalHttpClient()
	volume := startFakeVolumeServer(t)
	prior := &ambiguousPutFiler{volume: volume, apply: true, entries: map[string]*filer_pb.Entry{}}
	next := &ambiguousPutFiler{volume: volume, apply: true, entries: map[string]*filer_pb.Entry{}}
	priorAddr := startFakeFiler(t, prior)
	nextAddr := startFakeFiler(t, next)
	s3a := newPutTestServer(t, startFakeFiler(t, &ambiguousPutFiler{volume: volume, entries: map[string]*filer_pb.Entry{}}))
	s3a.objectWriteLockClient = cluster.NewLockClient(s3a.option.GrpcDialOption, priorAddr)
	s3a.objectWriteLockClient.SetRing([]pb.ServerAddress{priorAddr}, 1)

	out, code := s3a.createMultipartUpload(httptest.NewRequest(http.MethodPost, "/b/dir/object?uploads", nil),
		&s3.CreateMultipartUploadInput{Bucket: aws.String("b"), Key: aws.String("dir/object")})
	require.Equal(t, s3err.ErrNone, code)
	uploadDir := "/buckets/b/.uploads/" + *out.UploadId
	prior.mu.Lock()
	prior.lookupErr, prior.lookupFailKey = status.Error(codes.Unavailable, "store unavailable"), uploadDir
	prior.mu.Unlock()
	s3a.objectWriteLockClient.SetRing([]pb.ServerAddress{nextAddr}, 2)

	listParts := func() s3err.ErrorCode {
		_, code := s3a.listObjectParts(&s3.ListPartsInput{
			Bucket: aws.String("b"), Key: aws.String("dir/object"), UploadId: out.UploadId,
			MaxParts: aws.Int64(1000), PartNumberMarker: aws.Int64(0),
		})
		return code
	}
	require.Equal(t, s3err.ErrInternalError, listParts(), "new owner has nothing yet")
	next.mu.Lock()
	next.entries[uploadDir] = &filer_pb.Entry{Name: *out.UploadId, IsDirectory: true}
	next.mu.Unlock()
	require.Equal(t, s3err.ErrInternalError, listParts(), "new owner has only a bare directory")
}
