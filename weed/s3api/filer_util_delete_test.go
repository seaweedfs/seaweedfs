package s3api

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

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

	// deleteCallErrs scripts the RPC itself per attempt, deleteRespErrs the
	// filer's own message: the two ways a delete fails, told apart because
	// only the first is a transport that dropped a reply.
	deleteCallErrs []error
	deleteRespErrs []string

	deleteAttempts int
	deleteReqs     []*filer_pb.DeleteEntryRequest
	deleteReq      *filer_pb.DeleteEntryRequest
	lookupReq      *filer_pb.LookupDirectoryEntryRequest
	updateReq      *filer_pb.UpdateEntryRequest
}

func (c *deleteObjectEntryTestClient) DeleteEntry(ctx context.Context, req *filer_pb.DeleteEntryRequest, _ ...grpc.CallOption) (*filer_pb.DeleteEntryResponse, error) {
	attempt := c.deleteAttempts
	c.deleteAttempts++
	c.deleteReq = req
	c.deleteReqs = append(c.deleteReqs, req)

	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if attempt < len(c.deleteCallErrs) && c.deleteCallErrs[attempt] != nil {
		return nil, c.deleteCallErrs[attempt]
	}
	if attempt < len(c.deleteRespErrs) && c.deleteRespErrs[attempt] != "" {
		return &filer_pb.DeleteEntryResponse{Error: c.deleteRespErrs[attempt]}, nil
	}
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

// A delete is idempotent at the filer, so a reply the transport dropped is
// reissued rather than surfaced. Issue #7204 saw the surfaced version as a 500
// on the bucket delete, which boto3 resent and then answered NoSuchBucket;
// issue #7224 saw it as a per-key InternalError inside a 200, which no SDK
// retries, so the object silently stayed.
func TestDeleteUnversionedObjectReplaysADroppedReply(t *testing.T) {
	s3a := &S3ApiServer{option: &S3ApiServerOption{BucketsPath: "/buckets"}}
	client := &deleteObjectEntryTestClient{
		deleteCallErrs: []error{status.Error(codes.Unavailable, "transport is closing"), nil},
	}

	err := s3a.deleteUnversionedObjectWithClient(testRetryCtx(10*time.Millisecond), client, "mybucket", "a/b/c.txt", false)

	require.NoError(t, err)
	assert.Equal(t, 2, client.deleteAttempts)
	// the replay must ask for the same delete, not a differently shaped one
	require.Len(t, client.deleteReqs, 2)
	assert.Equal(t, client.deleteReqs[0].String(), client.deleteReqs[1].String())
}

// The filer formats the deleted path, and for a recursive delete the child it
// stopped on, into the message it sends back, so both are the client's text.
// Nothing here reads that text, and this pins it: the same failure under a key
// named after a transport condition is replayed exactly as often as under a
// plain one.
func TestDeleteUnversionedObjectReplayIgnoresTheObjectName(t *testing.T) {
	attemptsFor := func(key string) int {
		s3a := &S3ApiServer{option: &S3ApiServerOption{BucketsPath: "/buckets"}}
		client := &deleteObjectEntryTestClient{
			deleteRespErrs: []string{"delete file /buckets/b/" + key + ": permission denied", ""},
		}
		require.NoError(t, s3a.deleteUnversionedObjectWithClient(testRetryCtx(10*time.Millisecond), client, "b", key, false))
		return client.deleteAttempts
	}

	plain := attemptsFor("plain.log")
	for _, key := range []string{"transport.log", "unavailable.txt", "slowdown.csv", filer_pb.ErrNotFound.Error()} {
		assert.Equal(t, plain, attemptsFor(key), "key %q must not steer the replay", key)
	}
}

// A folder the filer refused because it still has children is an answer about
// the tree, not a hiccup: it is handed straight to the demote path instead of
// being replayed until the allowance runs out.
func TestDeleteUnversionedObjectDoesNotReplayANonEmptyFolder(t *testing.T) {
	s3a := &S3ApiServer{option: &S3ApiServerOption{BucketsPath: "/buckets"}}
	client := &deleteObjectEntryTestClient{
		deleteResp: &filer_pb.DeleteEntryResponse{
			Error: filer.MsgFailDelNonEmptyFolder + ": /buckets/b/photos",
		},
	}

	require.NoError(t, s3a.deleteUnversionedObjectWithClient(context.Background(), client, "b", "photos", false))
	assert.Equal(t, 1, client.deleteAttempts)
	assert.NotNil(t, client.lookupReq, "the demote must still run")
}

// A caller that has gone away is not worth replaying for.
func TestDeleteUnversionedObjectDoesNotReplayForACancelledCaller(t *testing.T) {
	s3a := &S3ApiServer{option: &S3ApiServerOption{BucketsPath: "/buckets"}}
	client := &deleteObjectEntryTestClient{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := s3a.deleteUnversionedObjectWithClient(ctx, client, "b", "k", false)

	require.Error(t, err)
	assert.Equal(t, 1, client.deleteAttempts)
}

// The client picks how many keys a multi-object delete carries, so the backoff
// is drawn from one allowance held by the request rather than one per key.
func TestDeleteUnversionedObjectDrawsFromTheRequestAllowance(t *testing.T) {
	s3a := &S3ApiServer{option: &S3ApiServerOption{BucketsPath: "/buckets"}}
	ctx := testRetryCtx(20 * time.Millisecond)

	start := time.Now()
	var attempts int
	for i := 0; i < 50; i++ {
		client := &deleteObjectEntryTestClient{deleteErr: status.Error(codes.Unavailable, "transport is closing")}
		require.Error(t, s3a.deleteUnversionedObjectWithClient(ctx, client, "b", fmt.Sprintf("k%d", i), false))
		attempts += client.deleteAttempts
	}

	assert.Less(t, time.Since(start), time.Second, "one allowance, not one per key")
	assert.Greater(t, attempts, 50, "the allowance must still buy a replay")
}

// testRetryCtx caps the backoff so a test exercises the replay without waiting
// out the real per-request allowance.
func testRetryCtx(allowance time.Duration) context.Context {
	return withFilerRetryBudget(context.Background(), allowance)
}

func TestIsRetryableFilerErr(t *testing.T) {
	assert.False(t, isRetryableFilerErr(nil))
	assert.True(t, isRetryableFilerErr(status.Error(codes.Unavailable, "transport is closing")))
	assert.True(t, isRetryableFilerErr(fmt.Errorf("delete entry /b/k: %w", status.Error(codes.Unavailable, "x"))))
	// the filer's own message carries no status, and a store error may well clear
	assert.True(t, isRetryableFilerErr(filer.DeleteEntryError("delete file /b/k: filer store delete: disk full")))

	assert.False(t, isRetryableFilerErr(filer.DeleteEntryError(filer.MsgFailDelNonEmptyFolder+": /b/photos")))
	assert.False(t, isRetryableFilerErr(filer_pb.ErrNotFound))
	assert.False(t, isRetryableFilerErr(status.Error(codes.NotFound, "gone")))
	assert.False(t, isRetryableFilerErr(context.Canceled))
	assert.False(t, isRetryableFilerErr(status.Error(codes.Canceled, "context canceled")))
	assert.False(t, isRetryableFilerErr(status.Error(codes.DeadlineExceeded, "context deadline exceeded")))

	// the message is never read, so the same code classifies the same way
	// whatever the client named the key
	assert.Equal(t,
		isRetryableFilerErr(status.Error(codes.PermissionDenied, "denied for /b/plain.log")),
		isRetryableFilerErr(status.Error(codes.PermissionDenied, "denied for /b/transport.log")))
}
