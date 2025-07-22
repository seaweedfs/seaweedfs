package s3api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/cors"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// BucketConfig represents cached bucket configuration
type BucketConfig struct {
	Name             string
	Versioning       string // "Enabled", "Suspended", or ""
	Ownership        string
	ACL              []byte
	Owner            string
	CORS             *cors.CORSConfiguration
	ObjectLockConfig *ObjectLockConfiguration // Cached parsed Object Lock configuration
	LastModified     time.Time
	Entry            *filer_pb.Entry
}

// BucketConfigCache provides caching for bucket configurations
// Cache entries are automatically updated/invalidated through metadata subscription events,
// so TTL serves as a safety fallback rather than the primary consistency mechanism
type BucketConfigCache struct {
	cache map[string]*BucketConfig
	mutex sync.RWMutex
	ttl   time.Duration // Safety fallback TTL; real-time consistency maintained via events
}

// NewBucketConfigCache creates a new bucket configuration cache
// TTL can be set to a longer duration since cache consistency is maintained
// through real-time metadata subscription events rather than TTL expiration
func NewBucketConfigCache(ttl time.Duration) *BucketConfigCache {
	return &BucketConfigCache{
		cache: make(map[string]*BucketConfig),
		ttl:   ttl,
	}
}

// Get retrieves bucket configuration from cache
func (bcc *BucketConfigCache) Get(bucket string) (*BucketConfig, bool) {
	bcc.mutex.RLock()
	defer bcc.mutex.RUnlock()

	config, exists := bcc.cache[bucket]
	if !exists {
		return nil, false
	}

	// Check if cache entry is expired (safety fallback; entries are normally updated via events)
	if time.Since(config.LastModified) > bcc.ttl {
		return nil, false
	}

	return config, true
}

// Set stores bucket configuration in cache
func (bcc *BucketConfigCache) Set(bucket string, config *BucketConfig) {
	bcc.mutex.Lock()
	defer bcc.mutex.Unlock()

	config.LastModified = time.Now()
	bcc.cache[bucket] = config
}

// Remove removes bucket configuration from cache
func (bcc *BucketConfigCache) Remove(bucket string) {
	bcc.mutex.Lock()
	defer bcc.mutex.Unlock()

	delete(bcc.cache, bucket)
}

// Clear clears all cached configurations
func (bcc *BucketConfigCache) Clear() {
	bcc.mutex.Lock()
	defer bcc.mutex.Unlock()

	bcc.cache = make(map[string]*BucketConfig)
}

// getBucketConfig retrieves bucket configuration with caching
func (s3a *S3ApiServer) getBucketConfig(bucket string) (*BucketConfig, s3err.ErrorCode) {
	// Try cache first
	if config, found := s3a.bucketConfigCache.Get(bucket); found {
		return config, s3err.ErrNone
	}

	// Try to get from filer
	entry, err := s3a.getEntry(s3a.option.BucketsPath, bucket)
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			// Bucket doesn't exist
			return nil, s3err.ErrNoSuchBucket
		}
		glog.Errorf("getBucketConfig: failed to get bucket entry for %s: %v", bucket, err)
		return nil, s3err.ErrInternalError
	}

	config := &BucketConfig{
		Name:  bucket,
		Entry: entry,
	}

	// Extract configuration from extended attributes
	if entry.Extended != nil {
		if versioning, exists := entry.Extended[s3_constants.ExtVersioningKey]; exists {
			config.Versioning = string(versioning)
		}
		if ownership, exists := entry.Extended[s3_constants.ExtOwnershipKey]; exists {
			config.Ownership = string(ownership)
		}
		if acl, exists := entry.Extended[s3_constants.ExtAmzAclKey]; exists {
			config.ACL = acl
		}
		if owner, exists := entry.Extended[s3_constants.ExtAmzOwnerKey]; exists {
			config.Owner = string(owner)
		}
		// Parse Object Lock configuration if present
		if objectLockConfig, found := LoadObjectLockConfigurationFromExtended(entry); found {
			config.ObjectLockConfig = objectLockConfig
			glog.V(2).Infof("getBucketConfig: cached Object Lock configuration for bucket %s", bucket)
		}
	}

	// Load CORS configuration from bucket directory content
	if corsConfig, err := s3a.loadCORSFromBucketContent(bucket); err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			// Missing metadata is not an error; fall back cleanly
			glog.V(2).Infof("CORS metadata not found for bucket %s, falling back to default behavior", bucket)
		} else {
			// Log parsing or validation errors
			glog.Errorf("Failed to load CORS configuration for bucket %s: %v", bucket, err)
		}
	} else {
		config.CORS = corsConfig
	}

	// Cache the result
	s3a.bucketConfigCache.Set(bucket, config)

	return config, s3err.ErrNone
}

// updateBucketConfig updates bucket configuration and invalidates cache
func (s3a *S3ApiServer) updateBucketConfig(bucket string, updateFn func(*BucketConfig) error) s3err.ErrorCode {
	config, errCode := s3a.getBucketConfig(bucket)
	if errCode != s3err.ErrNone {
		return errCode
	}

	// Apply update function
	if err := updateFn(config); err != nil {
		glog.Errorf("updateBucketConfig: update function failed for bucket %s: %v", bucket, err)
		return s3err.ErrInternalError
	}

	// Prepare extended attributes
	if config.Entry.Extended == nil {
		config.Entry.Extended = make(map[string][]byte)
	}

	// Update extended attributes
	if config.Versioning != "" {
		config.Entry.Extended[s3_constants.ExtVersioningKey] = []byte(config.Versioning)
	}
	if config.Ownership != "" {
		config.Entry.Extended[s3_constants.ExtOwnershipKey] = []byte(config.Ownership)
	}
	if config.ACL != nil {
		config.Entry.Extended[s3_constants.ExtAmzAclKey] = config.ACL
	}
	if config.Owner != "" {
		config.Entry.Extended[s3_constants.ExtAmzOwnerKey] = []byte(config.Owner)
	}
	// Update Object Lock configuration
	if config.ObjectLockConfig != nil {
		if err := StoreObjectLockConfigurationInExtended(config.Entry, config.ObjectLockConfig); err != nil {
			glog.Errorf("updateBucketConfig: failed to store Object Lock configuration for bucket %s: %v", bucket, err)
			return s3err.ErrInternalError
		}
	}

	// Save to filer
	err := s3a.updateEntry(s3a.option.BucketsPath, config.Entry)
	if err != nil {
		glog.Errorf("updateBucketConfig: failed to update bucket entry for %s: %v", bucket, err)
		return s3err.ErrInternalError
	}

	// Update cache
	s3a.bucketConfigCache.Set(bucket, config)

	return s3err.ErrNone
}

// isVersioningEnabled checks if versioning is enabled for a bucket (with caching)
func (s3a *S3ApiServer) isVersioningEnabled(bucket string) (bool, error) {
	config, errCode := s3a.getBucketConfig(bucket)
	if errCode != s3err.ErrNone {
		if errCode == s3err.ErrNoSuchBucket {
			return false, filer_pb.ErrNotFound
		}
		return false, fmt.Errorf("failed to get bucket config: %v", errCode)
	}

	// Versioning is enabled if explicitly set to "Enabled" OR if object lock is enabled
	// (since object lock requires versioning to be enabled)
	return config.Versioning == s3_constants.VersioningEnabled || config.ObjectLockConfig != nil, nil
}

// isVersioningConfigured checks if versioning has been configured (either Enabled or Suspended)
func (s3a *S3ApiServer) isVersioningConfigured(bucket string) (bool, error) {
	config, errCode := s3a.getBucketConfig(bucket)
	if errCode != s3err.ErrNone {
		if errCode == s3err.ErrNoSuchBucket {
			return false, filer_pb.ErrNotFound
		}
		return false, fmt.Errorf("failed to get bucket config: %v", errCode)
	}

	// Versioning is configured if explicitly set to either "Enabled" or "Suspended"
	// OR if object lock is enabled (which forces versioning)
	return config.Versioning != "" || config.ObjectLockConfig != nil, nil
}

// getVersioningState returns the detailed versioning state for a bucket
func (s3a *S3ApiServer) getVersioningState(bucket string) (string, error) {
	config, errCode := s3a.getBucketConfig(bucket)
	if errCode != s3err.ErrNone {
		if errCode == s3err.ErrNoSuchBucket {
			return "", filer_pb.ErrNotFound
		}
		return "", fmt.Errorf("failed to get bucket config: %v", errCode)
	}

	// If object lock is enabled, versioning must be enabled regardless of explicit setting
	if config.ObjectLockConfig != nil {
		return s3_constants.VersioningEnabled, nil
	}

	// Return the explicit versioning status (empty string means never configured)
	return config.Versioning, nil
}

// getBucketVersioningStatus returns the versioning status for a bucket
func (s3a *S3ApiServer) getBucketVersioningStatus(bucket string) (string, s3err.ErrorCode) {
	config, errCode := s3a.getBucketConfig(bucket)
	if errCode != s3err.ErrNone {
		return "", errCode
	}

	// Return exactly what's stored - empty string means versioning was never configured
	// This matches AWS S3 behavior where new buckets have no Status field in GetBucketVersioning response
	return config.Versioning, s3err.ErrNone
}

// setBucketVersioningStatus sets the versioning status for a bucket
func (s3a *S3ApiServer) setBucketVersioningStatus(bucket, status string) s3err.ErrorCode {
	return s3a.updateBucketConfig(bucket, func(config *BucketConfig) error {
		config.Versioning = status
		return nil
	})
}

// getBucketOwnership returns the ownership setting for a bucket
func (s3a *S3ApiServer) getBucketOwnership(bucket string) (string, s3err.ErrorCode) {
	config, errCode := s3a.getBucketConfig(bucket)
	if errCode != s3err.ErrNone {
		return "", errCode
	}

	return config.Ownership, s3err.ErrNone
}

// setBucketOwnership sets the ownership setting for a bucket
func (s3a *S3ApiServer) setBucketOwnership(bucket, ownership string) s3err.ErrorCode {
	return s3a.updateBucketConfig(bucket, func(config *BucketConfig) error {
		config.Ownership = ownership
		return nil
	})
}

// loadCORSFromBucketContent loads CORS configuration from bucket directory content
func (s3a *S3ApiServer) loadCORSFromBucketContent(bucket string) (*cors.CORSConfiguration, error) {
	metadata, err := s3a.getBucketMetadata(bucket)
	if err != nil {
		return nil, err
	}

	if metadata.CORS == nil {
		return nil, fmt.Errorf("no CORS configuration found")
	}

	return metadata.CORS, nil
}

// getCORSConfiguration retrieves CORS configuration with caching
func (s3a *S3ApiServer) getCORSConfiguration(bucket string) (*cors.CORSConfiguration, s3err.ErrorCode) {
	config, errCode := s3a.getBucketConfig(bucket)
	if errCode != s3err.ErrNone {
		return nil, errCode
	}

	return config.CORS, s3err.ErrNone
}

// updateCORSConfiguration updates CORS configuration in bucket directory content
func (s3a *S3ApiServer) updateCORSConfiguration(bucket string, corsConfig *cors.CORSConfiguration) s3err.ErrorCode {
	// Get existing metadata
	metadata, err := s3a.getBucketMetadata(bucket)
	if err != nil {
		glog.Errorf("updateCORSConfiguration: failed to get bucket metadata for bucket %s: %v", bucket, err)
		return s3err.ErrInternalError
	}

	// Update CORS configuration
	metadata.CORS = corsConfig

	// Store updated metadata
	if err := s3a.setBucketMetadata(bucket, metadata); err != nil {
		glog.Errorf("updateCORSConfiguration: failed to persist CORS config to bucket content for bucket %s: %v", bucket, err)
		return s3err.ErrInternalError
	}

	// Invalidate cache to force reload from disk
	s3a.bucketConfigCache.Remove(bucket)

	return s3err.ErrNone
}

// removeCORSConfiguration removes CORS configuration from bucket directory content
func (s3a *S3ApiServer) removeCORSConfiguration(bucket string) s3err.ErrorCode {
	// Get existing metadata
	metadata, err := s3a.getBucketMetadata(bucket)
	if err != nil {
		glog.Errorf("removeCORSConfiguration: failed to get bucket metadata for bucket %s: %v", bucket, err)
		return s3err.ErrInternalError
	}

	// Remove CORS configuration
	metadata.CORS = nil

	// Store updated metadata
	if err := s3a.setBucketMetadata(bucket, metadata); err != nil {
		glog.Errorf("removeCORSConfiguration: failed to remove CORS config from bucket content for bucket %s: %v", bucket, err)
		return s3err.ErrInternalError
	}

	// Invalidate cache to force reload from disk
	s3a.bucketConfigCache.Remove(bucket)

	return s3err.ErrNone
}

// BucketMetadata represents the unified bucket metadata structure
type BucketMetadata struct {
	Tags map[string]string       `json:"tags,omitempty"`
	CORS *cors.CORSConfiguration `json:"cors,omitempty"`
}

// getBucketMetadata retrieves bucket metadata from bucket directory content
func (s3a *S3ApiServer) getBucketMetadata(bucket string) (*BucketMetadata, error) {
	// Validate bucket name to prevent path traversal attacks
	if bucket == "" || strings.Contains(bucket, "/") || strings.Contains(bucket, "\\") ||
		strings.Contains(bucket, "..") || strings.Contains(bucket, "~") {
		return nil, fmt.Errorf("invalid bucket name: %s", bucket)
	}

	// Clean the bucket name further to prevent any potential path traversal
	bucket = filepath.Clean(bucket)
	if bucket == "." || bucket == ".." {
		return nil, fmt.Errorf("invalid bucket name: %s", bucket)
	}

	// Get bucket directory entry to access its content
	entry, err := s3a.getEntry(s3a.option.BucketsPath, bucket)
	if err != nil {
		return nil, fmt.Errorf("error retrieving bucket directory %s: %w", bucket, err)
	}
	if entry == nil {
		return nil, fmt.Errorf("bucket directory not found %s", bucket)
	}

	// If no content, return empty metadata
	if len(entry.Content) == 0 {
		return &BucketMetadata{}, nil
	}

	// Unmarshal metadata from JSON
	var metadata BucketMetadata
	if err := json.Unmarshal(entry.Content, &metadata); err != nil {
		glog.Errorf("getBucketMetadata: failed to unmarshal metadata for bucket %s: %v", bucket, err)
		return &BucketMetadata{}, nil // Return empty metadata on error, don't fail
	}

	return &metadata, nil
}

// setBucketMetadata stores bucket metadata in bucket directory content
func (s3a *S3ApiServer) setBucketMetadata(bucket string, metadata *BucketMetadata) error {
	// Validate bucket name to prevent path traversal attacks
	if bucket == "" || strings.Contains(bucket, "/") || strings.Contains(bucket, "\\") ||
		strings.Contains(bucket, "..") || strings.Contains(bucket, "~") {
		return fmt.Errorf("invalid bucket name: %s", bucket)
	}

	// Clean the bucket name further to prevent any potential path traversal
	bucket = filepath.Clean(bucket)
	if bucket == "." || bucket == ".." {
		return fmt.Errorf("invalid bucket name: %s", bucket)
	}

	// Marshal metadata to JSON
	metadataBytes, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal bucket metadata: %w", err)
	}

	// Update the bucket entry with new content
	return s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		// Get current bucket entry
		entry, err := s3a.getEntry(s3a.option.BucketsPath, bucket)
		if err != nil {
			return fmt.Errorf("error retrieving bucket directory %s: %w", bucket, err)
		}
		if entry == nil {
			return fmt.Errorf("bucket directory not found %s", bucket)
		}

		// Update content with metadata
		entry.Content = metadataBytes

		request := &filer_pb.UpdateEntryRequest{
			Directory: s3a.option.BucketsPath,
			Entry:     entry,
		}

		_, err = client.UpdateEntry(context.Background(), request)
		return err
	})
}

// getBucketTags retrieves bucket tags from bucket directory content
func (s3a *S3ApiServer) getBucketTags(bucket string) (map[string]string, error) {
	metadata, err := s3a.getBucketMetadata(bucket)
	if err != nil {
		return nil, err
	}

	if metadata.Tags == nil {
		return nil, fmt.Errorf("no tags configuration found")
	}

	return metadata.Tags, nil
}

// setBucketTags stores bucket tags in bucket directory content
func (s3a *S3ApiServer) setBucketTags(bucket string, tags map[string]string) error {
	// Get existing metadata
	metadata, err := s3a.getBucketMetadata(bucket)
	if err != nil {
		return err
	}

	// Update tags
	metadata.Tags = tags

	// Store updated metadata
	return s3a.setBucketMetadata(bucket, metadata)
}

// deleteBucketTags removes bucket tags from bucket directory content
func (s3a *S3ApiServer) deleteBucketTags(bucket string) error {
	// Get existing metadata
	metadata, err := s3a.getBucketMetadata(bucket)
	if err != nil {
		return err
	}

	// Remove tags
	metadata.Tags = nil

	// Store updated metadata
	return s3a.setBucketMetadata(bucket, metadata)
}
