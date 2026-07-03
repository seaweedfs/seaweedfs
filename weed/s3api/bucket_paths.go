package s3api

import (
	"errors"
	"path"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

var tableBucketFileValidator = s3tables.NewTableBucketFileValidator()

func (s3a *S3ApiServer) isTableBucket(bucket string) bool {
	if bucket == "" {
		return false
	}

	if s3a.bucketRegistry != nil {
		metadata, errCode := s3a.bucketRegistry.GetBucketMetadata(bucket)
		if errCode != s3err.ErrNone {
			if errCode != s3err.ErrNoSuchBucket {
				glog.V(1).Infof("bucket lookup failed for %s: %v", bucket, errCode)
			}
			return false
		}
		return metadata.IsTableBucket
	}

	entry, err := s3a.getEntry(s3a.option.BucketsPath, bucket)
	if err == nil && entry != nil {
		return s3tables.IsTableBucketEntry(entry)
	}

	if err != nil && !errors.Is(err, filer_pb.ErrNotFound) {
		glog.V(1).Infof("bucket lookup failed for %s: %v", bucket, err)
	}
	return false
}

func (s3a *S3ApiServer) bucketRoot(bucket string) string {
	// Returns the unified buckets root path for all bucket types
	return s3a.option.BucketsPath
}

func (s3a *S3ApiServer) bucketDir(bucket string) string {
	return path.Join(s3a.bucketRoot(bucket), bucket)
}

func (s3a *S3ApiServer) validateTableBucketObjectPath(bucket, object string) error {
	if !s3a.isTableBucket(bucket) {
		return nil
	}
	cleanObject := strings.TrimPrefix(object, "/")
	if cleanObject == "" {
		return &s3tables.IcebergLayoutError{
			Code:    s3tables.ErrCodeInvalidIcebergLayout,
			Message: "object must be under namespace/table/data or metadata",
		}
	}
	fullPath := s3a.bucketDir(bucket)
	if !strings.HasSuffix(fullPath, "/") {
		fullPath += "/"
	}
	fullPath += cleanObject
	if err := tableBucketFileValidator.ValidateTableBucketUpload(fullPath); err != nil {
		return err
	}
	parts := strings.SplitN(cleanObject, "/", 4)
	if len(parts) < 4 {
		return &s3tables.IcebergLayoutError{
			Code:    s3tables.ErrCodeInvalidIcebergLayout,
			Message: "object must be under namespace/table/data or metadata",
		}
	}
	return nil
}

func (s3a *S3ApiServer) bucketPrefix(bucket string) string {
	return s3a.bucketDir(bucket) + "/"
}

func (s3a *S3ApiServer) bucketExists(bucket string) (bool, error) {
	entry, err := s3a.getBucketEntry(bucket)
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	return entry != nil, nil
}

func (s3a *S3ApiServer) getBucketEntry(bucket string) (*filer_pb.Entry, error) {
	// Dot-prefixed entries under /buckets are internal (.system) and can never
	// be valid bucket names; don't let them resolve as buckets.
	if strings.HasPrefix(bucket, ".") {
		return nil, filer_pb.ErrNotFound
	}
	return s3a.getEntry(s3a.option.BucketsPath, bucket)
}
