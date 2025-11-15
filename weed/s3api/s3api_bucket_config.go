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

	"github.com/aws/aws-sdk-go/service/s3"
	"google.golang.org/protobuf/proto"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/iam/policy"
	"github.com/seaweedfs/seaweedfs/weed/kms"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/s3_pb"
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
	IsPublicRead     bool // Cached flag to avoid JSON parsing on every request
	CORS             *cors.CORSConfiguration
	ObjectLockConfig *ObjectLockConfiguration // Cached parsed Object Lock configuration
	BucketPolicy     *policy.PolicyDocument   // Cached bucket policy for performance
	KMSKeyCache      *BucketKMSCache          // Per-bucket KMS key cache for SSE-KMS operations
	LastModified     time.Time
	Entry            *filer_pb.Entry
}

// BucketKMSCache represents per-bucket KMS key caching for SSE-KMS operations
// This provides better isolation and automatic cleanup compared to global caching
type BucketKMSCache struct {
	cache   map[string]*BucketKMSCacheEntry // Key: contextHash, Value: cached data key
	mutex   sync.RWMutex
	bucket  string        // Bucket name for logging/debugging
	lastTTL time.Duration // TTL used for cache entries (typically 1 hour)
}

// BucketKMSCacheEntry represents a single cached KMS data key
type BucketKMSCacheEntry struct {
	DataKey     interface{} // Could be *kms.GenerateDataKeyResponse or similar
	ExpiresAt   time.Time
	KeyID       string
	ContextHash string // Hash of encryption context for cache validation
}

// NewBucketKMSCache creates a new per-bucket KMS key cache
func NewBucketKMSCache(bucketName string, ttl time.Duration) *BucketKMSCache {
	return &BucketKMSCache{
		cache:   make(map[string]*BucketKMSCacheEntry),
		bucket:  bucketName,
		lastTTL: ttl,
	}
}

// Get retrieves a cached KMS data key if it exists and hasn't expired
func (bkc *BucketKMSCache) Get(contextHash string) (*BucketKMSCacheEntry, bool) {
	if bkc == nil {
		return nil, false
	}

	bkc.mutex.RLock()
	defer bkc.mutex.RUnlock()

	entry, exists := bkc.cache[contextHash]
	if !exists {
		return nil, false
	}

	// Check if entry has expired
	if time.Now().After(entry.ExpiresAt) {
		return nil, false
	}

	return entry, true
}

// Set stores a KMS data key in the cache
func (bkc *BucketKMSCache) Set(contextHash, keyID string, dataKey interface{}, ttl time.Duration) {
	if bkc == nil {
		return
	}

	bkc.mutex.Lock()
	defer bkc.mutex.Unlock()

	bkc.cache[contextHash] = &BucketKMSCacheEntry{
		DataKey:     dataKey,
		ExpiresAt:   time.Now().Add(ttl),
		KeyID:       keyID,
		ContextHash: contextHash,
	}
	bkc.lastTTL = ttl
}

// CleanupExpired removes expired entries from the cache
func (bkc *BucketKMSCache) CleanupExpired() int {
	if bkc == nil {
		return 0
	}

	bkc.mutex.Lock()
	defer bkc.mutex.Unlock()

	now := time.Now()
	expiredCount := 0

	for key, entry := range bkc.cache {
		if now.After(entry.ExpiresAt) {
			// Clear sensitive data before removing from cache
			bkc.clearSensitiveData(entry)
			delete(bkc.cache, key)
			expiredCount++
		}
	}

	return expiredCount
}

// Size returns the current number of cached entries
func (bkc *BucketKMSCache) Size() int {
	if bkc == nil {
		return 0
	}

	bkc.mutex.RLock()
	defer bkc.mutex.RUnlock()

	return len(bkc.cache)
}

// clearSensitiveData securely clears sensitive data from a cache entry
func (bkc *BucketKMSCache) clearSensitiveData(entry *BucketKMSCacheEntry) {
	if dataKeyResp, ok := entry.DataKey.(*kms.GenerateDataKeyResponse); ok {
		// Zero out the plaintext data key to prevent it from lingering in memory
		if dataKeyResp.Plaintext != nil {
			for i := range dataKeyResp.Plaintext {
				dataKeyResp.Plaintext[i] = 0
			}
			dataKeyResp.Plaintext = nil
		}
	}
}

// Clear clears all cached KMS entries, securely zeroing sensitive data first
func (bkc *BucketKMSCache) Clear() {
	if bkc == nil {
		return
	}

	bkc.mutex.Lock()
	defer bkc.mutex.Unlock()

	// Clear sensitive data from all entries before deletion
	for _, entry := range bkc.cache {
		bkc.clearSensitiveData(entry)
	}

	// Clear the cache map
	bkc.cache = make(map[string]*BucketKMSCacheEntry)
}

// BucketConfigCache provides caching for bucket configurations
// Cache entries are automatically updated/invalidated through metadata subscription events,
// so TTL serves as a safety fallback rather than the primary consistency mechanism
type BucketConfigCache struct {
	cache         map[string]*BucketConfig
	negativeCache map[string]time.Time // Cache for non-existent buckets
	mutex         sync.RWMutex
	ttl           time.Duration // Safety fallback TTL; real-time consistency maintained via events
	negativeTTL   time.Duration // TTL for negative cache entries
}

// BucketMetadata represents the complete metadata for a bucket
type BucketMetadata struct {
	Tags       map[string]string              `json:"tags,omitempty"`
	CORS       *cors.CORSConfiguration        `json:"cors,omitempty"`
	Encryption *s3_pb.EncryptionConfiguration `json:"encryption,omitempty"`
	// Future extensions can be added here:
	// Versioning    *s3_pb.VersioningConfiguration   `json:"versioning,omitempty"`
	// Lifecycle     *s3_pb.LifecycleConfiguration    `json:"lifecycle,omitempty"`
	// Notification  *s3_pb.NotificationConfiguration `json:"notification,omitempty"`
	// Replication   *s3_pb.ReplicationConfiguration  `json:"replication,omitempty"`
	// Analytics     *s3_pb.AnalyticsConfiguration    `json:"analytics,omitempty"`
	// Logging       *s3_pb.LoggingConfiguration      `json:"logging,omitempty"`
	// Website       *s3_pb.WebsiteConfiguration      `json:"website,omitempty"`
	// RequestPayer  *s3_pb.RequestPayerConfiguration `json:"requestPayer,omitempty"`
	// PublicAccess  *s3_pb.PublicAccessConfiguration `json:"publicAccess,omitempty"`
}

// NewBucketMetadata creates a new BucketMetadata with default values
func NewBucketMetadata() *BucketMetadata {
	return &BucketMetadata{
		Tags: make(map[string]string),
	}
}

// IsEmpty returns true if the metadata has no configuration set
func (bm *BucketMetadata) IsEmpty() bool {
	return len(bm.Tags) == 0 && bm.CORS == nil && bm.Encryption == nil
}

// HasEncryption returns true if bucket has encryption configuration
func (bm *BucketMetadata) HasEncryption() bool {
	return bm.Encryption != nil
}

// HasCORS returns true if bucket has CORS configuration
func (bm *BucketMetadata) HasCORS() bool {
	return bm.CORS != nil
}

// HasTags returns true if bucket has tags
func (bm *BucketMetadata) HasTags() bool {
	return len(bm.Tags) > 0
}

// NewBucketConfigCache creates a new bucket configuration cache
// TTL can be set to a longer duration since cache consistency is maintained
// through real-time metadata subscription events rather than TTL expiration
func NewBucketConfigCache(ttl time.Duration) *BucketConfigCache {
	negativeTTL := ttl / 4 // Negative cache TTL is shorter than positive cache
	if negativeTTL < 30*time.Second {
		negativeTTL = 30 * time.Second // Minimum 30 seconds for negative cache
	}

	return &BucketConfigCache{
		cache:         make(map[string]*BucketConfig),
		negativeCache: make(map[string]time.Time),
		ttl:           ttl,
		negativeTTL:   negativeTTL,
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
	bcc.negativeCache = make(map[string]time.Time)
}

// IsNegativelyCached checks if a bucket is in the negative cache (doesn't exist)
func (bcc *BucketConfigCache) IsNegativelyCached(bucket string) bool {
	bcc.mutex.RLock()
	defer bcc.mutex.RUnlock()

	if cachedTime, exists := bcc.negativeCache[bucket]; exists {
		// Check if the negative cache entry is still valid
		if time.Since(cachedTime) < bcc.negativeTTL {
			return true
		}
		// Entry expired, remove it
		delete(bcc.negativeCache, bucket)
	}
	return false
}

// SetNegativeCache marks a bucket as non-existent in the negative cache
func (bcc *BucketConfigCache) SetNegativeCache(bucket string) {
	bcc.mutex.Lock()
	defer bcc.mutex.Unlock()

	bcc.negativeCache[bucket] = time.Now()
}

// RemoveNegativeCache removes a bucket from the negative cache
func (bcc *BucketConfigCache) RemoveNegativeCache(bucket string) {
	bcc.mutex.Lock()
	defer bcc.mutex.Unlock()

	delete(bcc.negativeCache, bucket)
}

// loadBucketPolicyFromExtended loads and parses bucket policy from entry extended attributes
func loadBucketPolicyFromExtended(entry *filer_pb.Entry, bucket string) *policy.PolicyDocument {
	if entry.Extended == nil {
		return nil
	}

	policyJSON, exists := entry.Extended[BUCKET_POLICY_METADATA_KEY]
	if !exists || len(policyJSON) == 0 {
		glog.V(4).Infof("loadBucketPolicyFromExtended: no bucket policy found for bucket %s", bucket)
		return nil
	}

	var policyDoc policy.PolicyDocument
	if err := json.Unmarshal(policyJSON, &policyDoc); err != nil {
		glog.Errorf("loadBucketPolicyFromExtended: failed to parse bucket policy for %s: %v", bucket, err)
		return nil
	}

	glog.V(3).Infof("loadBucketPolicyFromExtended: loaded bucket policy for bucket %s", bucket)
	return &policyDoc
}

// getBucketConfig retrieves bucket configuration with caching
func (s3a *S3ApiServer) getBucketConfig(bucket string) (*BucketConfig, s3err.ErrorCode) {
	// Check negative cache first
	if s3a.bucketConfigCache.IsNegativelyCached(bucket) {
		return nil, s3err.ErrNoSuchBucket
	}

	// Try positive cache
	if config, found := s3a.bucketConfigCache.Get(bucket); found {
		return config, s3err.ErrNone
	}

	// Try to get from filer
	entry, err := s3a.getEntry(s3a.option.BucketsPath, bucket)
	if err != nil {
		if errors.Is(err, filer_pb.ErrNotFound) {
			// Bucket doesn't exist - set negative cache
			s3a.bucketConfigCache.SetNegativeCache(bucket)
			return nil, s3err.ErrNoSuchBucket
		}
		glog.Errorf("getBucketConfig: failed to get bucket entry for %s: %v", bucket, err)
		return nil, s3err.ErrInternalError
	}

	config := &BucketConfig{
		Name:         bucket,
		Entry:        entry,
		IsPublicRead: false, // Explicitly default to false for private buckets
	}

	// Extract configuration from extended attributes
	if entry.Extended != nil {
		glog.V(3).Infof("getBucketConfig: checking extended attributes for bucket %s, ExtObjectLockEnabledKey value=%s",
			bucket, string(entry.Extended[s3_constants.ExtObjectLockEnabledKey]))
		if versioning, exists := entry.Extended[s3_constants.ExtVersioningKey]; exists {
			config.Versioning = string(versioning)
		}
		if ownership, exists := entry.Extended[s3_constants.ExtOwnershipKey]; exists {
			config.Ownership = string(ownership)
		}
		if acl, exists := entry.Extended[s3_constants.ExtAmzAclKey]; exists {
			config.ACL = acl
			// Parse ACL once and cache public-read status
			config.IsPublicRead = parseAndCachePublicReadStatus(acl)
		} else {
			// No ACL means private bucket
			config.IsPublicRead = false
		}
		if owner, exists := entry.Extended[s3_constants.ExtAmzOwnerKey]; exists {
			config.Owner = string(owner)
		}
		// Parse Object Lock configuration if present
		if objectLockConfig, found := LoadObjectLockConfigurationFromExtended(entry); found {
			config.ObjectLockConfig = objectLockConfig
			glog.V(3).Infof("getBucketConfig: loaded Object Lock config from extended attributes for bucket %s: %+v", bucket, objectLockConfig)
		} else {
			glog.V(3).Infof("getBucketConfig: no Object Lock config found in extended attributes for bucket %s", bucket)
		}
		
		// Load bucket policy if present (for performance optimization)
		config.BucketPolicy = loadBucketPolicyFromExtended(entry, bucket)
	}

	// Sync bucket policy to the policy engine for evaluation
	s3a.syncBucketPolicyToEngine(bucket, config.BucketPolicy)

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
		glog.V(3).Infof("updateBucketConfig: storing Object Lock config for bucket %s: %+v", bucket, config.ObjectLockConfig)
		if err := StoreObjectLockConfigurationInExtended(config.Entry, config.ObjectLockConfig); err != nil {
			glog.Errorf("updateBucketConfig: failed to store Object Lock configuration for bucket %s: %v", bucket, err)
			return s3err.ErrInternalError
		}
		glog.V(3).Infof("updateBucketConfig: stored Object Lock config in extended attributes for bucket %s, key=%s, value=%s",
			bucket, s3_constants.ExtObjectLockEnabledKey, string(config.Entry.Extended[s3_constants.ExtObjectLockEnabledKey]))
	}

	// Save to filer
	glog.V(3).Infof("updateBucketConfig: saving entry to filer for bucket %s", bucket)
	err := s3a.updateEntry(s3a.option.BucketsPath, config.Entry)
	if err != nil {
		glog.Errorf("updateBucketConfig: failed to update bucket entry for %s: %v", bucket, err)
		return s3err.ErrInternalError
	}
	glog.V(3).Infof("updateBucketConfig: saved entry to filer for bucket %s", bucket)

	// Update cache
	glog.V(0).Infof("updateBucketConfig: updating cache for bucket %s, Versioning='%s', ObjectLockConfig=%+v", bucket, config.Versioning, config.ObjectLockConfig)
	s3a.bucketConfigCache.Set(bucket, config)
	
	// Verify cache update
	if cached, found := s3a.bucketConfigCache.Get(bucket); found {
		glog.V(0).Infof("updateBucketConfig: cache verification - bucket %s now has Versioning='%s'", bucket, cached.Versioning)
	} else {
		glog.Errorf("updateBucketConfig: CACHE VERIFICATION FAILED - bucket %s not found in cache after Set", bucket)
	}

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
			glog.V(0).Infof("getVersioningState: bucket %s not found", bucket)
			return "", nil
		}
		glog.Errorf("getVersioningState: failed to get bucket config for %s: %v", bucket, errCode)
		return "", fmt.Errorf("failed to get bucket config: %v", errCode)
	}

	// If object lock is enabled, versioning must be enabled regardless of explicit setting
	if config.ObjectLockConfig != nil {
		glog.V(0).Infof("getVersioningState: bucket %s has object lock, returning VersioningEnabled", bucket)
		return s3_constants.VersioningEnabled, nil
	}

	// Return the explicit versioning status (empty string means never configured)
	glog.V(0).Infof("getVersioningState: bucket %s returning versioning state='%s'", bucket, config.Versioning)
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
	glog.V(0).Infof("setBucketVersioningStatus: bucket=%s, setting status='%s'", bucket, status)
	errCode := s3a.updateBucketConfig(bucket, func(config *BucketConfig) error {
		config.Versioning = status
		glog.V(0).Infof("setBucketVersioningStatus: bucket=%s, updated config.Versioning='%s'", bucket, config.Versioning)
		return nil
	})
	glog.V(0).Infof("setBucketVersioningStatus: bucket=%s, update complete with errCode=%v", bucket, errCode)
	return errCode
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
	metadata, err := s3a.GetBucketMetadata(bucket)
	if err != nil {
		return nil, err
	}

	// Note: corsConfig can be nil if no CORS configuration is set, which is valid
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

// updateCORSConfiguration updates the CORS configuration for a bucket
func (s3a *S3ApiServer) updateCORSConfiguration(bucket string, corsConfig *cors.CORSConfiguration) s3err.ErrorCode {
	// Update using structured API
	err := s3a.UpdateBucketCORS(bucket, corsConfig)
	if err != nil {
		glog.Errorf("updateCORSConfiguration: failed to update CORS config for bucket %s: %v", bucket, err)
		return s3err.ErrInternalError
	}

	// Cache will be updated automatically via metadata subscription
	return s3err.ErrNone
}

// removeCORSConfiguration removes the CORS configuration for a bucket
func (s3a *S3ApiServer) removeCORSConfiguration(bucket string) s3err.ErrorCode {
	// Update using structured API
	err := s3a.ClearBucketCORS(bucket)
	if err != nil {
		glog.Errorf("removeCORSConfiguration: failed to remove CORS config for bucket %s: %v", bucket, err)
		return s3err.ErrInternalError
	}

	// Cache will be updated automatically via metadata subscription
	return s3err.ErrNone
}

// Conversion functions between CORS types and protobuf types

// corsRuleToProto converts a CORS rule to protobuf format
func corsRuleToProto(rule cors.CORSRule) *s3_pb.CORSRule {
	return &s3_pb.CORSRule{
		AllowedHeaders: rule.AllowedHeaders,
		AllowedMethods: rule.AllowedMethods,
		AllowedOrigins: rule.AllowedOrigins,
		ExposeHeaders:  rule.ExposeHeaders,
		MaxAgeSeconds:  int32(getMaxAgeSecondsValue(rule.MaxAgeSeconds)),
		Id:             rule.ID,
	}
}

// corsRuleFromProto converts a protobuf CORS rule to standard format
func corsRuleFromProto(protoRule *s3_pb.CORSRule) cors.CORSRule {
	var maxAge *int
	// Always create the pointer if MaxAgeSeconds is >= 0
	// This prevents nil pointer dereferences in tests and matches AWS behavior
	if protoRule.MaxAgeSeconds >= 0 {
		age := int(protoRule.MaxAgeSeconds)
		maxAge = &age
	}
	// Only leave maxAge as nil if MaxAgeSeconds was explicitly set to a negative value

	return cors.CORSRule{
		AllowedHeaders: protoRule.AllowedHeaders,
		AllowedMethods: protoRule.AllowedMethods,
		AllowedOrigins: protoRule.AllowedOrigins,
		ExposeHeaders:  protoRule.ExposeHeaders,
		MaxAgeSeconds:  maxAge,
		ID:             protoRule.Id,
	}
}

// corsConfigToProto converts CORS configuration to protobuf format
func corsConfigToProto(config *cors.CORSConfiguration) *s3_pb.CORSConfiguration {
	if config == nil {
		return nil
	}

	protoRules := make([]*s3_pb.CORSRule, len(config.CORSRules))
	for i, rule := range config.CORSRules {
		protoRules[i] = corsRuleToProto(rule)
	}

	return &s3_pb.CORSConfiguration{
		CorsRules: protoRules,
	}
}

// corsConfigFromProto converts protobuf CORS configuration to standard format
func corsConfigFromProto(protoConfig *s3_pb.CORSConfiguration) *cors.CORSConfiguration {
	if protoConfig == nil {
		return nil
	}

	rules := make([]cors.CORSRule, len(protoConfig.CorsRules))
	for i, protoRule := range protoConfig.CorsRules {
		rules[i] = corsRuleFromProto(protoRule)
	}

	return &cors.CORSConfiguration{
		CORSRules: rules,
	}
}

// getMaxAgeSecondsValue safely extracts max age seconds value
func getMaxAgeSecondsValue(maxAge *int) int {
	if maxAge == nil {
		return 0
	}
	return *maxAge
}

// parseAndCachePublicReadStatus parses the ACL and caches the public-read status
func parseAndCachePublicReadStatus(acl []byte) bool {
	var grants []*s3.Grant
	if err := json.Unmarshal(acl, &grants); err != nil {
		return false
	}

	// Check if any grant gives read permission to "AllUsers" group
	for _, grant := range grants {
		if grant.Grantee != nil && grant.Grantee.URI != nil && grant.Permission != nil {
			// Check for AllUsers group with Read permission
			if *grant.Grantee.URI == s3_constants.GranteeGroupAllUsers &&
				(*grant.Permission == s3_constants.PermissionRead || *grant.Permission == s3_constants.PermissionFullControl) {
				return true
			}
		}
	}

	return false
}

// getBucketMetadata retrieves bucket metadata as a structured object with caching
func (s3a *S3ApiServer) getBucketMetadata(bucket string) (*BucketMetadata, error) {
	if s3a.bucketConfigCache != nil {
		// Check negative cache first
		if s3a.bucketConfigCache.IsNegativelyCached(bucket) {
			return nil, fmt.Errorf("bucket directory not found %s", bucket)
		}

		// Try to get from positive cache
		if config, found := s3a.bucketConfigCache.Get(bucket); found {
			// Extract metadata from cached config
			if metadata, err := s3a.extractMetadataFromConfig(config); err == nil {
				return metadata, nil
			}
			// If extraction fails, fall through to direct load
		}
	}

	// Load directly from filer
	return s3a.loadBucketMetadataFromFiler(bucket)
}

// extractMetadataFromConfig extracts BucketMetadata from cached BucketConfig
func (s3a *S3ApiServer) extractMetadataFromConfig(config *BucketConfig) (*BucketMetadata, error) {
	if config == nil || config.Entry == nil {
		return NewBucketMetadata(), nil
	}

	// Parse metadata from entry content if available
	if len(config.Entry.Content) > 0 {
		var protoMetadata s3_pb.BucketMetadata
		if err := proto.Unmarshal(config.Entry.Content, &protoMetadata); err != nil {
			glog.Errorf("extractMetadataFromConfig: failed to unmarshal protobuf metadata for bucket %s: %v", config.Name, err)
			return nil, err
		}
		// Convert protobuf to structured metadata
		metadata := &BucketMetadata{
			Tags:       protoMetadata.Tags,
			CORS:       corsConfigFromProto(protoMetadata.Cors),
			Encryption: protoMetadata.Encryption,
		}
		return metadata, nil
	}

	// Fallback: create metadata from cached CORS config
	metadata := NewBucketMetadata()
	if config.CORS != nil {
		metadata.CORS = config.CORS
	}

	return metadata, nil
}

// loadBucketMetadataFromFiler loads bucket metadata directly from the filer
func (s3a *S3ApiServer) loadBucketMetadataFromFiler(bucket string) (*BucketMetadata, error) {
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
		// Check if this is a "not found" error
		if errors.Is(err, filer_pb.ErrNotFound) {
			// Set negative cache for non-existent bucket
			if s3a.bucketConfigCache != nil {
				s3a.bucketConfigCache.SetNegativeCache(bucket)
			}
		}
		return nil, fmt.Errorf("error retrieving bucket directory %s: %w", bucket, err)
	}
	if entry == nil {
		// Set negative cache for non-existent bucket
		if s3a.bucketConfigCache != nil {
			s3a.bucketConfigCache.SetNegativeCache(bucket)
		}
		return nil, fmt.Errorf("bucket directory not found %s", bucket)
	}

	// If no content, return empty metadata
	if len(entry.Content) == 0 {
		return NewBucketMetadata(), nil
	}

	// Unmarshal metadata from protobuf
	var protoMetadata s3_pb.BucketMetadata
	if err := proto.Unmarshal(entry.Content, &protoMetadata); err != nil {
		glog.Errorf("getBucketMetadata: failed to unmarshal protobuf metadata for bucket %s: %v", bucket, err)
		return nil, fmt.Errorf("failed to unmarshal bucket metadata for %s: %w", bucket, err)
	}

	// Convert protobuf CORS to standard CORS
	corsConfig := corsConfigFromProto(protoMetadata.Cors)

	// Create and return structured metadata
	metadata := &BucketMetadata{
		Tags:       protoMetadata.Tags,
		CORS:       corsConfig,
		Encryption: protoMetadata.Encryption,
	}

	return metadata, nil
}

// setBucketMetadata stores bucket metadata from a structured object
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

	// Default to empty metadata if nil
	if metadata == nil {
		metadata = NewBucketMetadata()
	}

	// Create protobuf metadata
	protoMetadata := &s3_pb.BucketMetadata{
		Tags:       metadata.Tags,
		Cors:       corsConfigToProto(metadata.CORS),
		Encryption: metadata.Encryption,
	}

	// Marshal metadata to protobuf
	metadataBytes, err := proto.Marshal(protoMetadata)
	if err != nil {
		return fmt.Errorf("failed to marshal bucket metadata to protobuf: %w", err)
	}

	// Update the bucket entry with new content
	err = s3a.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
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

	// Invalidate cache after successful update
	if err == nil && s3a.bucketConfigCache != nil {
		s3a.bucketConfigCache.Remove(bucket)
		s3a.bucketConfigCache.RemoveNegativeCache(bucket) // Remove from negative cache too
	}

	return err
}

// New structured API functions using BucketMetadata

// GetBucketMetadata retrieves complete bucket metadata as a structured object
func (s3a *S3ApiServer) GetBucketMetadata(bucket string) (*BucketMetadata, error) {
	return s3a.getBucketMetadata(bucket)
}

// SetBucketMetadata stores complete bucket metadata from a structured object
func (s3a *S3ApiServer) SetBucketMetadata(bucket string, metadata *BucketMetadata) error {
	return s3a.setBucketMetadata(bucket, metadata)
}

// UpdateBucketMetadata updates specific parts of bucket metadata while preserving others
//
// DISTRIBUTED SYSTEM DESIGN NOTE:
// This function implements a read-modify-write pattern with "last write wins" semantics.
// In the rare case of concurrent updates to different parts of bucket metadata
// (e.g., simultaneous tag and CORS updates), the last write may overwrite previous changes.
//
// This is an acceptable trade-off because:
//  1. Bucket metadata updates are infrequent in typical S3 usage
//  2. Traditional locking doesn't work in distributed systems across multiple nodes
//  3. The complexity of distributed consensus (e.g., Raft) for metadata updates would
//     be disproportionate to the low frequency of bucket configuration changes
//  4. Most bucket operations (tags, CORS, encryption) are typically configured once
//     during setup rather than being frequently modified
//
// If stronger consistency is required, consider implementing optimistic concurrency
// control with version numbers or ETags at the storage layer.
func (s3a *S3ApiServer) UpdateBucketMetadata(bucket string, update func(*BucketMetadata) error) error {
	// Get current metadata
	metadata, err := s3a.GetBucketMetadata(bucket)
	if err != nil {
		return fmt.Errorf("failed to get current bucket metadata: %w", err)
	}

	// Apply update function
	if err := update(metadata); err != nil {
		return fmt.Errorf("failed to apply metadata update: %w", err)
	}

	// Store updated metadata (last write wins)
	return s3a.SetBucketMetadata(bucket, metadata)
}

// Helper functions for specific metadata operations using structured API

// UpdateBucketTags sets bucket tags using the structured API
func (s3a *S3ApiServer) UpdateBucketTags(bucket string, tags map[string]string) error {
	return s3a.UpdateBucketMetadata(bucket, func(metadata *BucketMetadata) error {
		metadata.Tags = tags
		return nil
	})
}

// UpdateBucketCORS sets bucket CORS configuration using the structured API
func (s3a *S3ApiServer) UpdateBucketCORS(bucket string, corsConfig *cors.CORSConfiguration) error {
	return s3a.UpdateBucketMetadata(bucket, func(metadata *BucketMetadata) error {
		metadata.CORS = corsConfig
		return nil
	})
}

// UpdateBucketEncryption sets bucket encryption configuration using the structured API
func (s3a *S3ApiServer) UpdateBucketEncryption(bucket string, encryptionConfig *s3_pb.EncryptionConfiguration) error {
	return s3a.UpdateBucketMetadata(bucket, func(metadata *BucketMetadata) error {
		metadata.Encryption = encryptionConfig
		return nil
	})
}

// ClearBucketTags removes all bucket tags using the structured API
func (s3a *S3ApiServer) ClearBucketTags(bucket string) error {
	return s3a.UpdateBucketMetadata(bucket, func(metadata *BucketMetadata) error {
		metadata.Tags = make(map[string]string)
		return nil
	})
}

// ClearBucketCORS removes bucket CORS configuration using the structured API
func (s3a *S3ApiServer) ClearBucketCORS(bucket string) error {
	return s3a.UpdateBucketMetadata(bucket, func(metadata *BucketMetadata) error {
		metadata.CORS = nil
		return nil
	})
}

// ClearBucketEncryption removes bucket encryption configuration using the structured API
func (s3a *S3ApiServer) ClearBucketEncryption(bucket string) error {
	return s3a.UpdateBucketMetadata(bucket, func(metadata *BucketMetadata) error {
		metadata.Encryption = nil
		return nil
	})
}
