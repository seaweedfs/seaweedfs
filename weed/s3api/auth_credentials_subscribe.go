package s3api

import (
	"errors"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func (s3a *S3ApiServer) subscribeMetaEvents(clientName string, lastTsNs int64, prefix string, directoriesToWatch []string) {

	processEventFn := func(resp *filer_pb.SubscribeMetadataResponse) error {

		message := resp.EventNotification
		if message.NewEntry == nil {
			return nil
		}

		dir := resp.Directory

		if message.NewParentPath != "" {
			dir = message.NewParentPath
		}
		fileName := message.NewEntry.Name
		content := message.NewEntry.Content

		_ = s3a.onIamConfigUpdate(dir, fileName, content)
		_ = s3a.onCircuitBreakerConfigUpdate(dir, fileName, content)
		_ = s3a.onBucketMetadataChange(dir, message.OldEntry, message.NewEntry)

		return nil
	}

	metadataFollowOption := &pb.MetadataFollowOption{
		ClientName:             clientName,
		ClientId:               s3a.randomClientId,
		ClientEpoch:            1,
		SelfSignature:          0,
		PathPrefix:             prefix,
		AdditionalPathPrefixes: nil,
		DirectoriesToWatch:     directoriesToWatch,
		StartTsNs:              lastTsNs,
		StopTsNs:               0,
		EventErrorType:         pb.FatalOnError,
	}
	util.RetryUntil("followIamChanges", func() error {
		metadataFollowOption.ClientEpoch++
		return pb.WithFilerClientFollowMetadata(s3a, metadataFollowOption, processEventFn)
	}, func(err error) bool {
		glog.V(0).Infof("iam follow metadata changes: %v", err)
		return true
	})
}

// reload iam config
func (s3a *S3ApiServer) onIamConfigUpdate(dir, filename string, content []byte) error {
	if dir == filer.IamConfigDirectory && filename == filer.IamIdentityFile {
		if err := s3a.iam.LoadS3ApiConfigurationFromBytes(content); err != nil {
			return err
		}
		glog.V(0).Infof("updated %s/%s", dir, filename)
	}
	return nil
}

// reload circuit breaker config
func (s3a *S3ApiServer) onCircuitBreakerConfigUpdate(dir, filename string, content []byte) error {
	if dir == s3_constants.CircuitBreakerConfigDir && filename == s3_constants.CircuitBreakerConfigFile {
		if err := s3a.cb.LoadS3ApiConfigurationFromBytes(content); err != nil {
			return err
		}
		glog.V(0).Infof("updated %s/%s", dir, filename)
	}
	return nil
}

// reload bucket metadata
func (s3a *S3ApiServer) onBucketMetadataChange(dir string, oldEntry *filer_pb.Entry, newEntry *filer_pb.Entry) error {
	if dir == s3a.option.BucketsPath {
		if newEntry != nil {
			// Update bucket registry (existing functionality)
			s3a.bucketRegistry.LoadBucketMetadata(newEntry)
			glog.V(0).Infof("updated bucketMetadata %s/%s", dir, newEntry.Name)

			// Update bucket configuration cache with new entry
			s3a.updateBucketConfigCacheFromEntry(newEntry)
		} else if oldEntry != nil {
			// Remove from bucket registry (existing functionality)
			s3a.bucketRegistry.RemoveBucketMetadata(oldEntry)
			glog.V(0).Infof("remove bucketMetadata %s/%s", dir, oldEntry.Name)

			// Remove from bucket configuration cache
			s3a.invalidateBucketConfigCache(oldEntry.Name)
		}
	}
	return nil
}

// updateBucketConfigCacheFromEntry updates the bucket config cache when a bucket entry changes
func (s3a *S3ApiServer) updateBucketConfigCacheFromEntry(entry *filer_pb.Entry) {
	if s3a.bucketConfigCache == nil {
		return
	}

	bucket := entry.Name

	glog.V(3).Infof("updateBucketConfigCacheFromEntry: called for bucket %s, ExtObjectLockEnabledKey=%s",
		bucket, string(entry.Extended[s3_constants.ExtObjectLockEnabledKey]))

	// Create new bucket config from the entry
	config := &BucketConfig{
		Name:         bucket,
		Entry:        entry,
		IsPublicRead: false, // Explicitly default to false for private buckets
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
			// Parse ACL and cache public-read status
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
			glog.V(2).Infof("updateBucketConfigCacheFromEntry: cached Object Lock configuration for bucket %s: %+v", bucket, objectLockConfig)
		} else {
			glog.V(3).Infof("updateBucketConfigCacheFromEntry: no Object Lock configuration found for bucket %s", bucket)
		}
		
		// Load bucket policy if present (for performance optimization)
		config.BucketPolicy = loadBucketPolicyFromExtended(entry, bucket)
	}

	// Sync bucket policy to the policy engine for evaluation
	if config.BucketPolicy != nil {
		s3a.policyEngine.LoadBucketPolicyFromCache(bucket, config.BucketPolicy)
	} else {
		// No policy - ensure it's removed from engine if it was there
		s3a.policyEngine.DeleteBucketPolicy(bucket)
	}

	// Load CORS configuration from bucket directory content
	if corsConfig, err := s3a.loadCORSFromBucketContent(bucket); err != nil {
		if !errors.Is(err, filer_pb.ErrNotFound) {
			glog.Errorf("updateBucketConfigCacheFromEntry: failed to load CORS configuration for bucket %s: %v", bucket, err)
		}
	} else {
		config.CORS = corsConfig
		glog.V(2).Infof("updateBucketConfigCacheFromEntry: loaded CORS config for bucket %s", bucket)
	}

	// Update timestamp
	config.LastModified = time.Now()

	// Update cache
	glog.V(3).Infof("updateBucketConfigCacheFromEntry: updating cache for bucket %s, ObjectLockConfig=%+v", bucket, config.ObjectLockConfig)
	s3a.bucketConfigCache.Set(bucket, config)
}

// invalidateBucketConfigCache removes a bucket from the configuration cache
func (s3a *S3ApiServer) invalidateBucketConfigCache(bucket string) {
	if s3a.bucketConfigCache == nil {
		return
	}

	s3a.bucketConfigCache.Remove(bucket)
	s3a.bucketConfigCache.RemoveNegativeCache(bucket) // Also remove from negative cache
	glog.V(2).Infof("invalidateBucketConfigCache: removed bucket %s from cache", bucket)
}
