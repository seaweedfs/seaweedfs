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
