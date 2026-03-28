package s3api

import (
	"fmt"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

const (
	bucketLifecycleConfigurationXMLKey               = "s3-bucket-lifecycle-configuration-xml"
	bucketLifecycleTransitionMinimumObjectSizeKey    = "s3-bucket-lifecycle-transition-default-minimum-object-size"
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
	if config.Entry == nil || config.Entry.Extended == nil {
		return nil, "", false, s3err.ErrNone
	}

	lifecycleXML, found := config.Entry.Extended[bucketLifecycleConfigurationXMLKey]
	if !found || len(lifecycleXML) == 0 {
		return nil, "", false, s3err.ErrNone
	}

	transitionMinimumObjectSize := normalizeBucketLifecycleTransitionMinimumObjectSize(
		string(config.Entry.Extended[bucketLifecycleTransitionMinimumObjectSizeKey]),
	)

	return append([]byte(nil), lifecycleXML...), transitionMinimumObjectSize, true, s3err.ErrNone
}

func (s3a *S3ApiServer) storeBucketLifecycleConfiguration(bucket string, lifecycleXML []byte, transitionMinimumObjectSize string) s3err.ErrorCode {
	return s3a.updateBucketConfig(bucket, func(config *BucketConfig) error {
		if config.Entry == nil {
			return fmt.Errorf("bucket %s is missing its filer entry", bucket)
		}
		if config.Entry.Extended == nil {
			config.Entry.Extended = make(map[string][]byte)
		}

		config.Entry.Extended[bucketLifecycleConfigurationXMLKey] = append([]byte(nil), lifecycleXML...)
		config.Entry.Extended[bucketLifecycleTransitionMinimumObjectSizeKey] = []byte(
			normalizeBucketLifecycleTransitionMinimumObjectSize(transitionMinimumObjectSize),
		)

		return nil
	})
}

func (s3a *S3ApiServer) clearStoredBucketLifecycleConfiguration(bucket string) s3err.ErrorCode {
	return s3a.updateBucketConfig(bucket, func(config *BucketConfig) error {
		if config.Entry == nil {
			return fmt.Errorf("bucket %s is missing its filer entry", bucket)
		}

		delete(config.Entry.Extended, bucketLifecycleConfigurationXMLKey)
		delete(config.Entry.Extended, bucketLifecycleTransitionMinimumObjectSizeKey)
		return nil
	})
}
