package s3api

import (
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateBucketConfigDoesNotMutateCacheOnPersistFailure(t *testing.T) {
	const bucket = "cleanup-test-net"

	s3a := newTestS3ApiServerWithMemoryIAM(t, nil)
	s3a.option = &S3ApiServerOption{
		BucketsPath: "/buckets",
	}
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	s3a.bucketConfigCache.Set(bucket, &BucketConfig{
		Name:       bucket,
		Versioning: "",
	})

	// This test server only has in-memory IAM state and no filer connection, so
	// updateBucketConfig is expected to fail with an internal error when it
	// reads the bucket entry. The assertion below verifies that the cached
	// config stays unchanged when the write path fails.
	errCode := s3a.updateBucketConfig(bucket, func(config *BucketConfig) error {
		config.Versioning = s3_constants.VersioningEnabled
		return nil
	})

	require.Equal(t, s3err.ErrInternalError, errCode)

	config, found := s3a.bucketConfigCache.Get(bucket)
	require.True(t, found)
	assert.Empty(t, config.Versioning)
}
