package s3api

import (
	"context"
	"errors"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	grpc "google.golang.org/grpc"
)

type deleteObjectEntryTestClient struct {
	filer_pb.SeaweedFilerClient

	deleteResp *filer_pb.DeleteEntryResponse
	deleteErr  error
	lookupResp *filer_pb.LookupDirectoryEntryResponse
	lookupErr  error
	updateErr  error

	deleteReq *filer_pb.DeleteEntryRequest
	lookupReq *filer_pb.LookupDirectoryEntryRequest
	updateReq *filer_pb.UpdateEntryRequest
}

func (c *deleteObjectEntryTestClient) DeleteEntry(_ context.Context, req *filer_pb.DeleteEntryRequest, _ ...grpc.CallOption) (*filer_pb.DeleteEntryResponse, error) {
	c.deleteReq = req
	if c.deleteResp == nil {
		return &filer_pb.DeleteEntryResponse{}, c.deleteErr
	}
	return c.deleteResp, c.deleteErr
}

func (c *deleteObjectEntryTestClient) LookupDirectoryEntry(_ context.Context, req *filer_pb.LookupDirectoryEntryRequest, _ ...grpc.CallOption) (*filer_pb.LookupDirectoryEntryResponse, error) {
	c.lookupReq = req
	if c.lookupResp == nil {
		return &filer_pb.LookupDirectoryEntryResponse{}, c.lookupErr
	}
	return c.lookupResp, c.lookupErr
}

func (c *deleteObjectEntryTestClient) UpdateEntry(_ context.Context, req *filer_pb.UpdateEntryRequest, _ ...grpc.CallOption) (*filer_pb.UpdateEntryResponse, error) {
	c.updateReq = req
	return &filer_pb.UpdateEntryResponse{}, c.updateErr
}

func TestDeleteObjectEntryDemotesNonEmptyDirectoryMarker(t *testing.T) {
	client := &deleteObjectEntryTestClient{
		deleteResp: &filer_pb.DeleteEntryResponse{
			Error: filer.MsgFailDelNonEmptyFolder + ": /buckets/test/photos",
		},
		lookupResp: &filer_pb.LookupDirectoryEntryResponse{
			Entry: &filer_pb.Entry{
				Name:        "photos",
				IsDirectory: true,
				Attributes: &filer_pb.FuseAttributes{
					Mime:     "application/octet-stream",
					Md5:      []byte{1, 2, 3, 4},
					FileSize: 4,
				},
				Content: []byte("test"),
				Extended: map[string][]byte{
					s3_constants.ExtETagKey:                   []byte("etag"),
					s3_constants.ExtAmzOwnerKey:               []byte("owner"),
					s3_constants.AmzUserMetaPrefix + "Color":  []byte("blue"),
					s3_constants.AmzObjectTaggingPrefix + "k": []byte("v"),
					"xattr-keep":           []byte("keep-me"),
					"x-seaweedfs-internal": []byte("keep-me-too"),
				},
			},
		},
	}

	err := deleteObjectEntry(client, "/buckets/test", "photos", true, false)
	require.NoError(t, err)
	require.NotNil(t, client.lookupReq)
	require.NotNil(t, client.updateReq)

	updated := client.updateReq.Entry
	require.NotNil(t, updated)
	assert.False(t, updated.IsDirectoryKeyObject())
	assert.Equal(t, "", updated.Attributes.Mime)
	assert.Empty(t, updated.Attributes.Md5)
	assert.Zero(t, updated.Attributes.FileSize)
	assert.Nil(t, updated.Content)
	assert.Nil(t, updated.Chunks)
	assert.Equal(t, map[string][]byte{
		"xattr-keep":           []byte("keep-me"),
		"x-seaweedfs-internal": []byte("keep-me-too"),
	}, updated.Extended)
}

func TestDeleteObjectEntryTreatsImplicitDirectoryAsSuccessfulNoop(t *testing.T) {
	client := &deleteObjectEntryTestClient{
		deleteResp: &filer_pb.DeleteEntryResponse{
			Error: filer.MsgFailDelNonEmptyFolder + ": /buckets/test/photos",
		},
		lookupResp: &filer_pb.LookupDirectoryEntryResponse{
			Entry: &filer_pb.Entry{
				Name:        "photos",
				IsDirectory: true,
				Attributes:  &filer_pb.FuseAttributes{},
			},
		},
	}

	err := deleteObjectEntry(client, "/buckets/test", "photos", true, false)
	require.NoError(t, err)
	require.NotNil(t, client.lookupReq)
	assert.Nil(t, client.updateReq)
}

func TestDeleteObjectEntryPropagatesNonDirectoryDeleteErrors(t *testing.T) {
	client := &deleteObjectEntryTestClient{
		deleteErr: errors.New("boom"),
	}

	err := deleteObjectEntry(client, "/buckets/test", "photos", true, false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "boom")
	assert.Nil(t, client.lookupReq)
	assert.Nil(t, client.updateReq)
}
