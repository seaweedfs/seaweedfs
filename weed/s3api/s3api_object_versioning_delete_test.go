package s3api

import (
	"context"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDeleteSpecificObjectVersionNullLookupError(t *testing.T) {
	// A filer error other than not-found must not be treated as missing
	filer := &fakeLookupFiler{
		lookupErr: status.Error(codes.Internal, "filer internal error"),
	}
	s3a := newHeadBucketTestServer(t, filer)

	err := s3a.deleteSpecificObjectVersion(context.Background(), "bucket", "object", "null", false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to lookup null version")
	assert.Contains(t, err.Error(), "filer internal error")
}

func TestDeleteSpecificObjectVersionNullNotFoundIsIdempotent(t *testing.T) {
	// NotFound is clean idempotent success
	filer := &fakeLookupFiler{
		lookupErr: status.Error(codes.NotFound, filer_pb.ErrNotFound.Error()),
	}
	s3a := newHeadBucketTestServer(t, filer)

	err := s3a.deleteSpecificObjectVersion(context.Background(), "bucket", "object", "null", false)
	assert.NoError(t, err)
}

func TestDeleteSpecificObjectVersionDirectoryLookupError(t *testing.T) {
	// Error checking whether version is latest must be surfaced
	filer := &fakeLookupFiler{
		lookupErr: status.Error(codes.Internal, "filer store unavailable"),
	}
	s3a := newHeadBucketTestServer(t, filer)

	err := s3a.deleteSpecificObjectVersion(context.Background(), "bucket", "object", "672a75d526ef29c79fe6b5680cad4a0d", false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to check latest version")
	assert.Contains(t, err.Error(), "filer store unavailable")
}
