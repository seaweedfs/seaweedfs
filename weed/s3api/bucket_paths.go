package s3api

import (
	"errors"
	"path"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

func (s3a *S3ApiServer) isTableBucket(bucket string) bool {
	if bucket == "" {
		return false
	}

	// Check cache first
	if found, ok := s3a.getTableBucketFromCache(bucket); ok {
		return found
	}

	entry, err := s3a.getEntry(s3tables.TablesPath, bucket)
	isTable := err == nil && entry != nil

	// Update cache
	s3a.cacheTableBucket(bucket, isTable)

	if !isTable && !errors.Is(err, filer_pb.ErrNotFound) && err != nil {
		glog.V(1).Infof("bucket lookup failed for %s: %v", bucket, err)
	}
	return isTable
}

func (s3a *S3ApiServer) getTableBucketFromCache(bucket string) (bool, bool) {
	if s3a.bucketRegistry == nil {
		return false, false
	}
	s3a.bucketRegistry.tableBucketLock.RLock()
	defer s3a.bucketRegistry.tableBucketLock.RUnlock()
	
	found, ok := s3a.bucketRegistry.tableBucketCache[bucket]
	return found, ok
}

func (s3a *S3ApiServer) cacheTableBucket(bucket string, isTable bool) {
	if s3a.bucketRegistry == nil {
		return
	}
	s3a.bucketRegistry.tableBucketLock.Lock()
	defer s3a.bucketRegistry.tableBucketLock.Unlock()
	
	s3a.bucketRegistry.tableBucketCache[bucket] = isTable
}

func (s3a *S3ApiServer) tableLocationDir(bucket string) (string, bool) {
	if bucket == "" {
		return "", false
	}
	entry, err := s3a.getEntry(s3tables.GetTableLocationMappingDir(), bucket)
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			return "", false
		}
		glog.V(1).Infof("table location mapping lookup failed for %s: %v", bucket, err)
		return "", false
	}
	if entry == nil || len(entry.Content) == 0 {
		return "", false
	}
	tablePath := strings.TrimSpace(string(entry.Content))
	if tablePath == "" {
		return "", false
	}
	return tablePath, true
}

func (s3a *S3ApiServer) bucketRoot(bucket string) string {
	if s3a.isTableBucket(bucket) {
		return s3tables.TablesPath
	}
	return s3a.option.BucketsPath
}

func (s3a *S3ApiServer) bucketDir(bucket string) string {
	if tablePath, ok := s3a.tableLocationDir(bucket); ok {
		return tablePath
	}
	if s3a.isTableBucket(bucket) {
		return s3tables.GetTableObjectBucketPath(bucket)
	}
	return path.Join(s3a.bucketRoot(bucket), bucket)
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
	if tablePath, ok := s3a.tableLocationDir(bucket); ok {
		return s3a.getEntry(path.Dir(tablePath), path.Base(tablePath))
	}
	entry, err := s3a.getEntry(s3tables.TablesPath, bucket)
	if err == nil || !errors.Is(err, filer_pb.ErrNotFound) {
		return entry, err
	}
	return s3a.getEntry(s3a.option.BucketsPath, bucket)
}
