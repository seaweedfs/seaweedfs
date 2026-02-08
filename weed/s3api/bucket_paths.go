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
	if s3a.bucketRegistry != nil {
		s3a.bucketRegistry.metadataCacheLock.RLock()
		if metadata, ok := s3a.bucketRegistry.metadataCache[bucket]; ok {
			s3a.bucketRegistry.metadataCacheLock.RUnlock()
			return metadata.IsTableBucket
		}
		s3a.bucketRegistry.metadataCacheLock.RUnlock()
	}

	entry, err := s3a.getEntry(s3a.option.BucketsPath, bucket)
	if err == nil && entry != nil {
		if s3a.bucketRegistry != nil {
			s3a.bucketRegistry.LoadBucketMetadata(entry)
		}
		return s3tables.IsTableBucketEntry(entry)
	}

	if err != nil && !errors.Is(err, filer_pb.ErrNotFound) {
		glog.V(1).Infof("bucket lookup failed for %s: %v", bucket, err)
	}
	return false
}

func (s3a *S3ApiServer) tableLocationDir(bucket string) (string, bool) {
	if bucket == "" {
		return "", false
	}

	// Check cache first
	if s3a.bucketRegistry != nil {
		s3a.bucketRegistry.tableLocationLock.RLock()
		if tablePath, ok := s3a.bucketRegistry.tableLocationCache[bucket]; ok {
			s3a.bucketRegistry.tableLocationLock.RUnlock()
			return tablePath, tablePath != ""
		}
		s3a.bucketRegistry.tableLocationLock.RUnlock()
	}

	entry, err := s3a.getEntry(s3tables.GetTableLocationMappingDir(), bucket)
	tablePath := ""
	if err == nil && entry != nil && len(entry.Content) > 0 {
		tablePath = strings.TrimSpace(string(entry.Content))
	}

	// Only cache definitive results: successful lookup (tablePath set) or definitive not-found (ErrNotFound)
	// Don't cache transient errors to avoid treating temporary failures as permanent misses
	if err == nil || errors.Is(err, filer_pb.ErrNotFound) {
		if s3a.bucketRegistry != nil {
			s3a.bucketRegistry.tableLocationLock.Lock()
			s3a.bucketRegistry.tableLocationCache[bucket] = tablePath
			s3a.bucketRegistry.tableLocationLock.Unlock()
		}
	}

	if tablePath == "" {
		if err != nil && !errors.Is(err, filer_pb.ErrNotFound) {
			glog.V(1).Infof("table location mapping lookup failed for %s: %v", bucket, err)
		}
		return "", false
	}

	return tablePath, true
}

func (s3a *S3ApiServer) bucketRoot(bucket string) string {
	// Returns the unified buckets root path for all bucket types
	return s3a.option.BucketsPath
}

func (s3a *S3ApiServer) bucketDir(bucket string) string {
	if tablePath, ok := s3a.tableLocationDir(bucket); ok {
		return tablePath
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
	return s3a.getEntry(s3a.option.BucketsPath, bucket)
}
