package s3api

import (
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3lifecycle/scheduler"
)

const (
	// The stored form is defined in the scheduler package, which callers
	// outside the gateway also read; these are local shorthands for it.
	bucketLifecycleConfigurationXMLKey            = scheduler.BucketLifecycleConfigurationXMLKey
	bucketLifecycleTransitionMinimumObjectSizeKey = scheduler.BucketLifecycleTransitionMinimumObjectSizeKey
	maxBucketLifecycleConfigurationSize           = scheduler.MaxBucketLifecycleConfigurationSize

	bucketLifecycleTransitionMinimumObjectSizeHeader = "X-Amz-Transition-Default-Minimum-Object-Size"
	defaultLifecycleTransitionMinimumObjectSize      = "all_storage_classes_128K"
)

func normalizeBucketLifecycleTransitionMinimumObjectSize(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return defaultLifecycleTransitionMinimumObjectSize
	}
	return value
}

func (s3a *S3ApiServer) getStoredBucketLifecycleConfiguration(bucket string) ([]byte, string, bool, s3err.ErrorCode) {
	config, errCode := s3a.getBucketConfig(bucket)
	if errCode != s3err.ErrNone {
		return nil, "", false, errCode
	}
	if len(config.LifecycleXML) == 0 {
		return nil, "", false, s3err.ErrNone
	}

	transitionMinimumObjectSize := normalizeBucketLifecycleTransitionMinimumObjectSize(config.LifecycleTransitionMinSize)

	return append([]byte(nil), config.LifecycleXML...), transitionMinimumObjectSize, true, s3err.ErrNone
}

func (s3a *S3ApiServer) storeBucketLifecycleConfiguration(bucket string, lifecycleXML []byte, transitionMinimumObjectSize string) s3err.ErrorCode {
	return s3a.updateBucketConfig(bucket, func(config *BucketConfig) error {
		config.LifecycleXML = append([]byte(nil), lifecycleXML...)
		config.LifecycleTransitionMinSize = normalizeBucketLifecycleTransitionMinimumObjectSize(transitionMinimumObjectSize)
		return nil
	})
}

func (s3a *S3ApiServer) clearStoredBucketLifecycleConfiguration(bucket string) s3err.ErrorCode {
	return s3a.updateBucketConfig(bucket, func(config *BucketConfig) error {
		config.LifecycleXML = nil
		config.LifecycleTransitionMinSize = ""
		return nil
	})
}
