package s3api

import (
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeleteUnversionedObjectWithClient_MetadataOnlySkipsChunkDelete(t *testing.T) {
	// metadataOnly=true must reach the filer as IsDeleteData=false so the
	// volume server reclaims chunks via TTL instead of the filer enqueueing
	// per-chunk DeleteFile RPCs.
	s3a := &S3ApiServer{option: &S3ApiServerOption{BucketsPath: "/buckets"}}
	client := &deleteObjectEntryTestClient{}

	err := s3a.deleteUnversionedObjectWithClient(client, "b", "k", true)
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

	err := s3a.deleteUnversionedObjectWithClient(client, "b", "k", false)
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

	err := s3a.deleteUnversionedObjectWithClient(client, "mybucket", "a/b/c.txt", false)
	require.NoError(t, err)
	require.NotNil(t, client.deleteReq)
	assert.Equal(t, "/buckets/mybucket/a/b", client.deleteReq.Directory)
	assert.Equal(t, "c.txt", client.deleteReq.Name)
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

	require.NoError(t, s3a.deleteUnversionedObjectWithClient(client, "b", "k", true))
	require.NotNil(t, client.deleteReq)
	assert.False(t, client.deleteReq.IsDeleteData)
}
