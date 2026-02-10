package s3api

import (
	"context"
	"errors"
	"io"
	"path"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
)

var tableBucketFileValidator = s3tables.NewTableBucketFileValidator()

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
	if err == nil && entry != nil {
		if entry.IsDirectory {
			tablePath, err = s3a.readTableLocationMappingFromDirectory(bucket)
		} else if len(entry.Content) > 0 {
			// Backward compatibility with legacy single-file mappings.
			tablePath = normalizeTableLocationMappingPath(string(entry.Content))
		}
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

func (s3a *S3ApiServer) readTableLocationMappingFromDirectory(bucket string) (string, error) {
	mappingDir := s3tables.GetTableLocationMappingPath(bucket)
	var mappedPath string
	conflict := false

	err := s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
stream, err := client.ListEntries(context.Background(), &filer_pb.ListEntriesRequest{
			Directory: mappingDir,
			Limit:     4294967295, // math.MaxUint32
		})
		if err != nil {
			return err
		}

		for {
			resp, recvErr := stream.Recv()
			if recvErr == io.EOF {
				return nil
			}
			if recvErr != nil {
				return recvErr
			}
			if resp == nil || resp.Entry == nil || resp.Entry.IsDirectory || len(resp.Entry.Content) == 0 {
				continue
			}

			candidate := normalizeTableLocationMappingPath(string(resp.Entry.Content))
			if candidate == "" {
				continue
			}
			if mappedPath == "" {
				mappedPath = candidate
				continue
			}
			if mappedPath != candidate {
				conflict = true
				return nil
			}
		}
	})
	if err != nil {
		return "", err
	}

	if conflict {
		glog.V(1).Infof("table location mapping conflict for %s: multiple mapped roots found", bucket)
		return "", nil
	}
	return mappedPath, nil
}

func normalizeTableLocationMappingPath(rawPath string) string {
	rawPath = strings.TrimSpace(rawPath)
	if rawPath == "" {
		return ""
	}

	normalized := path.Clean("/" + strings.TrimPrefix(rawPath, "/"))
	tablesPrefix := strings.TrimSuffix(s3tables.TablesPath, "/") + "/"
	if !strings.HasPrefix(normalized, tablesPrefix) {
		return normalized
	}

	remaining := strings.TrimPrefix(normalized, tablesPrefix)
	bucketName, _, _ := strings.Cut(remaining, "/")
	if bucketName == "" {
		return ""
	}
	return path.Join(s3tables.TablesPath, bucketName)
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
	if tablePath, ok := s3a.tableLocationDir(bucket); ok {
		return s3a.getEntry(path.Dir(tablePath), path.Base(tablePath))
	}
	return s3a.getEntry(s3a.option.BucketsPath, bucket)
}
