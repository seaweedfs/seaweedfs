package s3api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateDeleteObjectIdentifier(t *testing.T) {
	tests := []struct {
		name       string
		identifier ObjectIdentifier
		want       s3err.ErrorCode
	}{
		{"clean key", ObjectIdentifier{Key: "dir/key"}, s3err.ErrNone},
		{"clean version", ObjectIdentifier{Key: "dir/key", VersionId: "opaque-version"}, s3err.ErrNone},
		{"key traversal", ObjectIdentifier{Key: "../victim/key"}, s3err.ErrInvalidRequest},
		{"encoded traversal already decoded", ObjectIdentifier{Key: "dir/../../victim/key"}, s3err.ErrInvalidRequest},
		{"backslash traversal", ObjectIdentifier{Key: `..\victim\key`}, s3err.ErrInvalidRequest},
		{"version traversal", ObjectIdentifier{Key: "key", VersionId: "v1/../../../victim"}, s3err.ErrInvalidRequest},
		{"version backslash", ObjectIdentifier{Key: "key", VersionId: `v1\..\victim`}, s3err.ErrInvalidRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, validateDeleteObjectIdentifier(tt.identifier))
		})
	}
}

func TestGetSpecificObjectVersionRejectsUnsafeVersionID(t *testing.T) {
	s3a := &S3ApiServer{option: &S3ApiServerOption{BucketsPath: "/buckets"}}

	_, err := s3a.getSpecificObjectVersion("bucket", "key", "v1/../../../victim")

	require.Error(t, err)
	assert.True(t, errors.Is(err, errInvalidVersionID))
}

func TestDeleteUnversionedObjectWithClient_MetadataOnlySkipsChunkDelete(t *testing.T) {
	// metadataOnly=true must reach the filer as IsDeleteData=false so the
	// volume server reclaims chunks via TTL instead of the filer enqueueing
	// per-chunk DeleteFile RPCs.
	s3a := &S3ApiServer{option: &S3ApiServerOption{BucketsPath: "/buckets"}}
	client := &deleteObjectEntryTestClient{}

	err := s3a.deleteUnversionedObjectWithClient(context.Background(), client, "b", "k", true)
	require.NoError(t, err)
	require.NotNil(t, client.deleteReq)
	assert.Equal(t, "/buckets/b", client.deleteReq.Directory)
	assert.Equal(t, "k", client.deleteReq.Name)
	assert.False(t, client.deleteReq.IsDeleteData, "metadataOnly must clear IsDeleteData")
}

func TestDeleteUnversionedObjectWithClient_FullDeletePreservesIsDeleteData(t *testing.T) {
	// Default behavior (metadataOnly=false): filer should still enqueue
	// chunk deletions as before.
	s3a := &S3ApiServer{option: &S3ApiServerOption{BucketsPath: "/buckets"}}
	client := &deleteObjectEntryTestClient{}

	err := s3a.deleteUnversionedObjectWithClient(context.Background(), client, "b", "k", false)
	require.NoError(t, err)
	require.NotNil(t, client.deleteReq)
	assert.True(t, client.deleteReq.IsDeleteData, "default delete must keep IsDeleteData true")
}

func TestDeleteUnversionedObjectWithClient_FullPathFromBucketsRoot(t *testing.T) {
	// Sanity: BucketsPath joins to <bucketsPath>/<bucket>/<object> in the
	// DeleteEntryRequest so the filer can locate the entry. Object keys
	// with multiple path segments should split into Directory + Name
	// correctly.
	s3a := &S3ApiServer{option: &S3ApiServerOption{BucketsPath: "/buckets"}}
	client := &deleteObjectEntryTestClient{}

	err := s3a.deleteUnversionedObjectWithClient(context.Background(), client, "mybucket", "a/b/c.txt", false)
	require.NoError(t, err)
	require.NotNil(t, client.deleteReq)
	assert.Equal(t, "/buckets/mybucket/a/b", client.deleteReq.Directory)
	assert.Equal(t, "c.txt", client.deleteReq.Name)
}

func TestDeleteUnversionedObjectWithClientRejectsTraversal(t *testing.T) {
	s3a := &S3ApiServer{option: &S3ApiServerOption{BucketsPath: "/buckets"}}
	client := &deleteObjectEntryTestClient{}

	err := s3a.deleteUnversionedObjectWithClient(context.Background(), client, "source-bucket", "../victim-bucket/secret", false)

	require.Error(t, err)
	assert.Nil(t, client.deleteReq, "invalid path must be rejected before a filer delete RPC")
}

func TestDeleteUnversionedObjectWithClient_PropagatesEntryAttributesIrrelevant(t *testing.T) {
	// The metadataOnly decision is the caller's responsibility (the
	// lifecycle handler). This function is dumb plumbing — it must not
	// inspect the entry itself, only translate the bool to IsDeleteData.
	s3a := &S3ApiServer{option: &S3ApiServerOption{BucketsPath: "/buckets"}}
	client := &deleteObjectEntryTestClient{
		// A response with no error is fine; attributes on a delete are unused.
		deleteResp: &filer_pb.DeleteEntryResponse{},
	}

	require.NoError(t, s3a.deleteUnversionedObjectWithClient(context.Background(), client, "b", "k", true))
	require.NotNil(t, client.deleteReq)
	assert.False(t, client.deleteReq.IsDeleteData)
}

// The gateway's own filer is not the object's write owner and has not seen the
// owner's writes yet. The delete marker must still commit on the owner, where
// the latest-pointer recompute scans .versions, so the marker becomes current.
func TestVersionedDeleteMarkerCommitsOnOwner(t *testing.T) {
	util_http.InitGlobalHttpClient()
	volume := startFakeVolumeServer(t)
	owner := &ambiguousPutFiler{volume: volume, apply: true, entries: map[string]*filer_pb.Entry{}}
	replica := &ambiguousPutFiler{volume: volume, apply: true, entries: map[string]*filer_pb.Entry{}}
	ownerAddr := startFakeFiler(t, owner)
	replicaAddr := startFakeFiler(t, replica)
	s3a := newPutTestServer(t, replicaAddr)
	s3a.iam = &IdentityAccessManagement{}
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	s3a.bucketConfigCache.Set("bucket", &BucketConfig{Name: "bucket", Versioning: s3_constants.VersioningEnabled})
	s3a.objectWriteLockClient = cluster.NewLockClient(s3a.option.GrpcDialOption, ownerAddr)
	s3a.objectWriteLockClient.SetRing([]pb.ServerAddress{ownerAddr}, 1)

	put := httptest.NewRequest(http.MethodPut, "/bucket/key", strings.NewReader("data"))
	put = mux.SetURLVars(put, map[string]string{"bucket": "bucket", "object": "key"})
	putResult := httptest.NewRecorder()
	s3a.PutObjectHandler(putResult, put)
	require.Equal(t, http.StatusOK, putResult.Code, putResult.Body.String())
	dataVersionId := putResult.Header().Get("x-amz-version-id")
	require.NotEmpty(t, dataVersionId)

	var deleteTxns []*filer_pb.ObjectTransactionRequest
	owner.mu.Lock()
	owner.txnCheck = func(req *filer_pb.ObjectTransactionRequest) { deleteTxns = append(deleteTxns, req) }
	owner.mu.Unlock()

	del := httptest.NewRequest(http.MethodDelete, "/bucket/key", nil)
	del = mux.SetURLVars(del, map[string]string{"bucket": "bucket", "object": "key"})
	delResult := httptest.NewRecorder()
	s3a.DeleteObjectHandler(delResult, del)
	require.Equal(t, http.StatusNoContent, delResult.Code, delResult.Body.String())
	markerId := delResult.Header().Get("x-amz-version-id")
	require.NotEmpty(t, markerId)
	require.NotEqual(t, dataVersionId, markerId)

	markerKey := "/buckets/bucket/key.versions/v_" + markerId
	owner.mu.Lock()
	require.Len(t, deleteTxns, 1, "the marker and the pointer flip must share one transaction")
	mutations := deleteTxns[0].Mutations
	require.Len(t, mutations, 2)
	require.Equal(t, filer_pb.ObjectMutation_PUT, mutations[0].Type)
	require.Equal(t, "/buckets/bucket/key.versions", mutations[0].Directory)
	require.Equal(t, "v_"+markerId, mutations[0].Entry.GetName())
	require.Equal(t, filer_pb.ObjectMutation_RECOMPUTE_LATEST, mutations[1].Type)
	require.NotNil(t, owner.entries[markerKey], "the marker must commit on the owner")
	pointer := owner.entries["/buckets/bucket/key.versions"]
	require.NotNil(t, pointer)
	require.Equal(t, markerId, string(pointer.Extended[s3_constants.ExtLatestVersionIdKey]))
	require.Equal(t, "true", string(pointer.Extended[s3_constants.ExtLatestVersionIsDeleteMarker]))
	owner.mu.Unlock()
	replica.mu.Lock()
	require.Nil(t, replica.entries[markerKey], "the replica must still be behind")
	replica.mu.Unlock()
}
