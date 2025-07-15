package s3api

import (
	"encoding/json"
	"fmt"
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
	Name         string
	Versioning   string // "Enabled", "Suspended", or ""
	Ownership    string
	ACL          []byte
	Owner        string
	CORS         *cors.CORSConfiguration
	LastModified time.Time
	Entry        *filer_pb.Entry
}

// BucketConfigCache provides caching for bucket configurations
type BucketConfigCache struct {
	cache map[string]*BucketConfig
	mutex sync.RWMutex
	ttl   time.Duration
}

// NewBucketConfigCache creates a new bucket configuration cache
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

	// Check if cache entry is expired
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
	}

	// Load CORS configuration from .s3metadata
	if corsConfig, err := s3a.loadCORSFromMetadata(bucket); err == nil {
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

	return config.Versioning == "Enabled", nil
}

// getBucketVersioningStatus returns the versioning status for a bucket
func (s3a *S3ApiServer) getBucketVersioningStatus(bucket string) (string, s3err.ErrorCode) {
	config, errCode := s3a.getBucketConfig(bucket)
	if errCode != s3err.ErrNone {
		return "", errCode
	}

	if config.Versioning == "" {
		return "Suspended", s3err.ErrNone
	}

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

// removeBucketConfigKey removes a specific configuration key from bucket
func (s3a *S3ApiServer) removeBucketConfigKey(bucket, key string) s3err.ErrorCode {
	return s3a.updateBucketConfig(bucket, func(config *BucketConfig) error {
		if config.Entry.Extended != nil {
			delete(config.Entry.Extended, key)
		}

		// Update our local config too
		switch key {
		case s3_constants.ExtVersioningKey:
			config.Versioning = ""
		case s3_constants.ExtOwnershipKey:
			config.Ownership = ""
		case s3_constants.ExtAmzAclKey:
			config.ACL = nil
		case s3_constants.ExtAmzOwnerKey:
			config.Owner = ""
		}

		return nil
	})
}

// loadCORSFromMetadata loads CORS configuration from bucket metadata
func (s3a *S3ApiServer) loadCORSFromMetadata(bucket string) (*cors.CORSConfiguration, error) {
	bucketMetadataPath := fmt.Sprintf("%s/%s/.s3metadata", s3a.option.BucketsPath, bucket)

	entry, err := s3a.getEntry("", bucketMetadataPath)
	if err != nil || entry == nil {
		return nil, fmt.Errorf("no metadata found")
	}

	if len(entry.Content) == 0 {
		return nil, fmt.Errorf("no metadata content")
	}

	var metadata map[string]interface{}
	if err := json.Unmarshal(entry.Content, &metadata); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %v", err)
	}

	corsData, exists := metadata["cors"]
	if !exists {
		return nil, fmt.Errorf("no CORS configuration found")
	}

	// Convert back to CORSConfiguration
	corsBytes, err := json.Marshal(corsData)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal CORS data: %v", err)
	}

	var config cors.CORSConfiguration
	if err := json.Unmarshal(corsBytes, &config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal CORS configuration: %v", err)
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

// updateCORSConfiguration updates CORS configuration and invalidates cache
func (s3a *S3ApiServer) updateCORSConfiguration(bucket string, corsConfig *cors.CORSConfiguration) s3err.ErrorCode {
	return s3a.updateBucketConfig(bucket, func(config *BucketConfig) error {
		config.CORS = corsConfig
		return nil
	})
}

// removeCORSConfiguration removes CORS configuration and invalidates cache
func (s3a *S3ApiServer) removeCORSConfiguration(bucket string) s3err.ErrorCode {
	return s3a.updateBucketConfig(bucket, func(config *BucketConfig) error {
		config.CORS = nil
		return nil
	})
}
