package s3api

import (
	"encoding/json"
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

	// Load from filer
	bucketEntry, err := s3a.getEntry(s3a.option.BucketsPath, bucket)
	if err != nil {
		if err == filer_pb.ErrNotFound {
			return nil, s3err.ErrNoSuchBucket
		}
		glog.Errorf("getBucketConfig: failed to get bucket entry for %s: %v", bucket, err)
		return nil, s3err.ErrInternalError
	}

	config := &BucketConfig{
		Name:  bucket,
		Entry: bucketEntry,
	}

	// Extract configuration from extended attributes
	if bucketEntry.Extended != nil {
		if versioning, exists := bucketEntry.Extended[s3_constants.ExtVersioningKey]; exists {
			config.Versioning = string(versioning)
		}
		if ownership, exists := bucketEntry.Extended[s3_constants.ExtOwnershipKey]; exists {
			config.Ownership = string(ownership)
		}
		if acl, exists := bucketEntry.Extended[s3_constants.ExtAmzAclKey]; exists {
			config.ACL = acl
		}
		if owner, exists := bucketEntry.Extended[s3_constants.ExtAmzOwnerKey]; exists {
			config.Owner = string(owner)
		}
		// Parse Object Lock configuration if present
		if objectLockConfig, found := LoadObjectLockConfigurationFromExtended(bucketEntry); found {
			config.ObjectLockConfig = objectLockConfig
			glog.V(2).Infof("getBucketConfig: cached Object Lock configuration for bucket %s", bucket)
		}
	}

	// Load CORS configuration from .s3metadata
	if corsConfig, err := s3a.loadCORSFromMetadata(bucket); err != nil {
		if err == filer_pb.ErrNotFound {
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

// loadCORSFromMetadata loads CORS configuration from bucket metadata
func (s3a *S3ApiServer) loadCORSFromMetadata(bucket string) (*cors.CORSConfiguration, error) {
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

	bucketMetadataPath := filepath.Join(s3a.option.BucketsPath, bucket, cors.S3MetadataFileName)

	entry, err := s3a.getEntry("", bucketMetadataPath)
	if err != nil {
		glog.V(3).Infof("loadCORSFromMetadata: error retrieving metadata for bucket %s: %v", bucket, err)
		return nil, fmt.Errorf("error retrieving metadata for bucket %s: %v", bucket, err)
	}
	if entry == nil {
		glog.V(3).Infof("loadCORSFromMetadata: no metadata entry found for bucket %s", bucket)
		return nil, fmt.Errorf("no metadata entry found for bucket %s", bucket)
	}

	if len(entry.Content) == 0 {
		glog.V(3).Infof("loadCORSFromMetadata: empty metadata content for bucket %s", bucket)
		return nil, fmt.Errorf("no metadata content for bucket %s", bucket)
	}

	var metadata map[string]json.RawMessage
	if err := json.Unmarshal(entry.Content, &metadata); err != nil {
		glog.Errorf("loadCORSFromMetadata: failed to unmarshal metadata for bucket %s: %v", bucket, err)
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	corsData, exists := metadata["cors"]
	if !exists {
		glog.V(3).Infof("loadCORSFromMetadata: no CORS configuration found for bucket %s", bucket)
		return nil, fmt.Errorf("no CORS configuration found")
	}

	// Directly unmarshal the raw JSON to CORSConfiguration to avoid round-trip allocations
	var config cors.CORSConfiguration
	if err := json.Unmarshal(corsData, &config); err != nil {
		glog.Errorf("loadCORSFromMetadata: failed to unmarshal CORS configuration for bucket %s: %v", bucket, err)
		return nil, fmt.Errorf("failed to unmarshal CORS configuration: %w", err)
	}

	return &config, nil
}

// getCORSConfiguration retrieves CORS configuration with caching
func (s3a *S3ApiServer) getCORSConfiguration(bucket string) (*cors.CORSConfiguration, s3err.ErrorCode) {
	config, errCode := s3a.getBucketConfig(bucket)
	if errCode != s3err.ErrNone {
		return nil, errCode
	}

	return config.CORS, s3err.ErrNone
}

// getCORSStorage returns a CORS storage instance for persistent operations
func (s3a *S3ApiServer) getCORSStorage() *cors.Storage {
	entryGetter := &S3EntryGetter{server: s3a}
	return cors.NewStorage(s3a, entryGetter, s3a.option.BucketsPath)
}

// updateCORSConfiguration updates CORS configuration and invalidates cache
func (s3a *S3ApiServer) updateCORSConfiguration(bucket string, corsConfig *cors.CORSConfiguration) s3err.ErrorCode {
	// Update in-memory cache
	errCode := s3a.updateBucketConfig(bucket, func(config *BucketConfig) error {
		config.CORS = corsConfig
		return nil
	})
	if errCode != s3err.ErrNone {
		return errCode
	}

	// Persist to .s3metadata file
	storage := s3a.getCORSStorage()
	if err := storage.Store(bucket, corsConfig); err != nil {
		glog.Errorf("updateCORSConfiguration: failed to persist CORS config to metadata for bucket %s: %v", bucket, err)
		return s3err.ErrInternalError
	}

	return s3err.ErrNone
}

// removeCORSConfiguration removes CORS configuration and invalidates cache
func (s3a *S3ApiServer) removeCORSConfiguration(bucket string) s3err.ErrorCode {
	// Remove from in-memory cache
	errCode := s3a.updateBucketConfig(bucket, func(config *BucketConfig) error {
		config.CORS = nil
		return nil
	})
	if errCode != s3err.ErrNone {
		return errCode
	}

	// Remove from .s3metadata file
	storage := s3a.getCORSStorage()
	if err := storage.Delete(bucket); err != nil {
		glog.Errorf("removeCORSConfiguration: failed to remove CORS config from metadata for bucket %s: %v", bucket, err)
		return s3err.ErrInternalError
	}

	return s3err.ErrNone
}
