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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

	err := deleteObjectEntry(context.Background(), client, "/buckets/test", "photos", true, false)
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

// A prefix object is demoted the same way, and the mark has to go with the data:
// the path is a plain directory again, and no longer a key of its own.
func TestDeleteObjectEntryDemotesPrefixObject(t *testing.T) {
	client := &deleteObjectEntryTestClient{
		deleteResp: &filer_pb.DeleteEntryResponse{
			Error: filer.MsgFailDelNonEmptyFolder + ": /buckets/test/photos",
		},
		lookupResp: &filer_pb.LookupDirectoryEntryResponse{
			Entry: &filer_pb.Entry{
				Name:        "photos",
				IsDirectory: true,
				Attributes:  &filer_pb.FuseAttributes{},
				Extended: map[string][]byte{
					s3_constants.SeaweedFSPrefixObject: []byte("true"),
					s3_constants.ExtETagKey:            []byte("etag"),
				},
			},
		},
	}

	require.NoError(t, deleteObjectEntry(context.Background(), client, "/buckets/test", "photos", true, false))
	require.NotNil(t, client.updateReq)

	updated := client.updateReq.Entry
	require.NotNil(t, updated)
	assert.False(t, updated.IsPrefixObject())
	assert.False(t, updated.IsDirectoryKeyObject())
	assert.Empty(t, updated.Extended)
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

	err := deleteObjectEntry(context.Background(), client, "/buckets/test", "photos", true, false)
	require.NoError(t, err)
	require.NotNil(t, client.lookupReq)
	assert.Nil(t, client.updateReq)
}

func TestDeleteObjectEntryIgnoresConcurrentUpdateNotFound(t *testing.T) {
	client := &deleteObjectEntryTestClient{
		deleteResp: &filer_pb.DeleteEntryResponse{
			Error: filer.MsgFailDelNonEmptyFolder + ": /buckets/test/photos",
		},
		lookupResp: &filer_pb.LookupDirectoryEntryResponse{
			Entry: &filer_pb.Entry{
				Name:        "photos",
				IsDirectory: true,
				Attributes: &filer_pb.FuseAttributes{
					Mime: "application/octet-stream",
				},
			},
		},
		updateErr: status.Error(codes.NotFound, "already removed"),
	}

	err := deleteObjectEntry(context.Background(), client, "/buckets/test", "photos", true, false)
	require.NoError(t, err)
	require.NotNil(t, client.lookupReq)
	require.NotNil(t, client.updateReq)
}

// The key is the client's and the filer echoes it in the message it sends back,
// so a key named after the marker must not turn a real failure into the demote
// no-op, which would answer a failed delete with a 204.
func TestDeleteObjectEntryIgnoresMarkerSpoofedByKey(t *testing.T) {
	name := filer.MsgFailDelNonEmptyFolder
	client := &deleteObjectEntryTestClient{
		deleteResp: &filer_pb.DeleteEntryResponse{
			Error: "delete file /buckets/test/" + name + ": filer store delete: disk full",
		},
	}

	err := deleteObjectEntry(context.Background(), client, "/buckets/test", name, true, false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "disk full")
	assert.Nil(t, client.lookupReq)
	assert.Nil(t, client.updateReq)
}

func TestDeleteObjectEntryPropagatesNonDirectoryDeleteErrors(t *testing.T) {
	client := &deleteObjectEntryTestClient{
		deleteErr: errors.New("boom"),
	}

	err := deleteObjectEntry(context.Background(), client, "/buckets/test", "photos", true, false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "boom")
	assert.Nil(t, client.lookupReq)
	assert.Nil(t, client.updateReq)
}
