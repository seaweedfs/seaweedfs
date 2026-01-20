package s3api

import (
	"errors"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func (s3a *S3ApiServer) subscribeMetaEvents(clientName string, lastTsNs int64, prefix string, directoriesToWatch []string) {
	glog.V(0).Infof("subscribeMetaEvents entry: client=%s prefix=%s filer=%v", clientName, prefix, s3a.option.Filers)

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
		_ = s3a.onIamRoleUpdate(dir, fileName, content)
		_ = s3a.onIamPolicyUpdate(dir, fileName, content)
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
		glog.V(1).Infof("iam follow metadata changes: %v", err)
		return true
	})
}

// reload iam role
func (s3a *S3ApiServer) onIamRoleUpdate(dir, filename string, content []byte) error {
	glog.V(1).Infof("onIamRoleUpdate: dir=%s, filename=%s", dir, filename)
	// Check if this is a role update in /etc/iam/roles
	if dir == filer.IamConfigDirectory+"/roles" && strings.HasSuffix(filename, ".json") {
		roleName := strings.TrimSuffix(filename, ".json")
		if s3a.iamIntegration != nil {
			s3a.iamIntegration.OnRoleUpdate(roleName)
			glog.V(1).Infof("active cache invalidation for role: %s", roleName)
		}
	}
	return nil
}

// reload iam policy
func (s3a *S3ApiServer) onIamPolicyUpdate(dir, filename string, content []byte) error {
	glog.V(1).Infof("onIamPolicyUpdate: dir=%s, filename=%s", dir, filename)
	// Check if this is a policy update in /etc/iam/policies
	if dir == filer.IamConfigDirectory+"/policies" && strings.HasSuffix(filename, ".json") {
		policyName := strings.TrimSuffix(filename, ".json")
		if s3a.iamIntegration != nil {
			s3a.iamIntegration.OnPolicyUpdate(policyName)
			glog.V(1).Infof("active cache invalidation for policy: %s", policyName)
		}
	}
	return nil
}

// reload iam config
func (s3a *S3ApiServer) onIamConfigUpdate(dir, filename string, content []byte) error {
	glog.V(1).Infof("onIamConfigUpdate: dir=%s, filename=%s, contentLength=%d", dir, filename, len(content))
	if dir == filer.IamConfigDirectory && filename == filer.IamIdentityFile {
		// If content is empty (file is chunked or not inline), reload from Filer
		if len(content) == 0 {
			glog.V(1).Infof("onIamConfigUpdate: content empty for %s/%s, reloading from credential manager", dir, filename)
			if err := s3a.iam.LoadS3ApiConfigurationFromCredentialManager(); err != nil {
				glog.Errorf("onIamConfigUpdate: failed to reload from credential manager: %v", err)
				return err
			}
			glog.Infof("✅ Successfully reloaded IAM configuration from %s/%s (via credential manager)", dir, filename)
			return nil
		}
		
		// Content is inline, use it directly
		if err := s3a.iam.LoadS3ApiConfigurationFromBytes(content); err != nil {
			glog.Errorf("onIamConfigUpdate: failed to load config: %v", err)
			return err
		}
		glog.Infof("✅ Successfully reloaded IAM configuration from %s/%s (inline content)", dir, filename)
	}
	return nil
}

// reload circuit breaker config
func (s3a *S3ApiServer) onCircuitBreakerConfigUpdate(dir, filename string, content []byte) error {
	if dir == s3_constants.CircuitBreakerConfigDir && filename == s3_constants.CircuitBreakerConfigFile {
		if err := s3a.cb.LoadS3ApiConfigurationFromBytes(content); err != nil {
			return err
		}
		glog.V(1).Infof("updated %s/%s", dir, filename)
	}
	return nil
}

// reload bucket metadata
func (s3a *S3ApiServer) onBucketMetadataChange(dir string, oldEntry *filer_pb.Entry, newEntry *filer_pb.Entry) error {
	if dir == s3a.option.BucketsPath {
		if newEntry != nil {
			// Update bucket registry (existing functionality)
			s3a.bucketRegistry.LoadBucketMetadata(newEntry)
			glog.V(1).Infof("updated bucketMetadata %s/%s", dir, newEntry.Name)

			// Update bucket configuration cache with new entry
			s3a.updateBucketConfigCacheFromEntry(newEntry)
		} else if oldEntry != nil {
			// Remove from bucket registry (existing functionality)
			s3a.bucketRegistry.RemoveBucketMetadata(oldEntry)
			glog.V(1).Infof("remove bucketMetadata %s/%s", dir, oldEntry.Name)

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
	s3a.syncBucketPolicyToEngine(bucket, config.BucketPolicy)

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
