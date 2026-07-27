package s3api

import (
	"context"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const oidcProvidersDir = filer.IamConfigDirectory + "/oidc-providers"

func (s3a *S3ApiServer) subscribeMetaEvents(clientName string, lastTsNs int64, prefix string, directoriesToWatch []string) {

	processEventFn := func(resp *filer_pb.SubscribeMetadataResponse) error {

		message := resp.EventNotification

		// For rename/move operations, NewParentPath contains the destination directory.
		// We process both source and destination dirs so moves out of watched
		// directories (e.g., IAM config dirs) are not missed.
		dir := resp.Directory
		if message.NewParentPath != "" {
			dir = message.NewParentPath
		}

		// Handle all metadata changes (create, update, delete, rename)
		// These handlers check for nil entries internally
		_ = s3a.onBucketMetadataChange(dir, message.OldEntry, message.NewEntry)
		_ = s3a.onIamConfigChange(dir, message.OldEntry, message.NewEntry)
		_ = s3a.onOIDCProviderChange(dir, message.OldEntry, message.NewEntry)
		_ = s3a.onCircuitBreakerConfigChange(dir, message.OldEntry, message.NewEntry)

		// For moves across directories, replay a delete event for the source directory
		if message.NewParentPath != "" && resp.Directory != message.NewParentPath {
			_ = s3a.onBucketMetadataChange(resp.Directory, message.OldEntry, nil)
			_ = s3a.onIamConfigChange(resp.Directory, message.OldEntry, nil)
			_ = s3a.onOIDCProviderChange(resp.Directory, message.OldEntry, nil)
			_ = s3a.onCircuitBreakerConfigChange(resp.Directory, message.OldEntry, nil)
		}

		// For same-directory renames, replay a delete event for the old name
		// so handlers can clean up stale state (e.g., old bucket names)
		if message.OldEntry != nil && message.NewEntry != nil &&
			(message.NewParentPath == "" || message.NewParentPath == resp.Directory) &&
			message.OldEntry.Name != message.NewEntry.Name {
			_ = s3a.onBucketMetadataChange(dir, message.OldEntry, nil)
			_ = s3a.onCircuitBreakerConfigChange(dir, message.OldEntry, nil)
		}

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

// onIamConfigChange handles IAM config file changes (create, update, delete).
// It reloads even with a static -config file: the merge protects the file's
// identities, and the filer->s3 push alone is best-effort.
func (s3a *S3ApiServer) onIamConfigChange(dir string, oldEntry *filer_pb.Entry, newEntry *filer_pb.Entry) error {
	if s3a.iam == nil {
		return nil
	}

	reloadIamConfig := func(reason string) error {
		glog.V(1).Infof("IAM change detected in %s, reloading configuration", reason)
		if err := s3a.iam.LoadS3ApiConfigurationFromCredentialManager(); err != nil {
			// the event stream moves on; retry state-based until a reload succeeds
			glog.Errorf("failed to reload IAM configuration after change in %s: %v", reason, err)
			s3a.iam.scheduleReload()
			return err
		}
		return nil
	}

	// 1. Handle traditional single identity.json file
	if dir == filer.IamConfigDirectory {
		// Handle create/update/delete events on legacy identity.json.
		// During migration this file is renamed, which emits a delete event.
		// Always reload from the credential manager so we keep the migrated identities.
		if (oldEntry != nil && oldEntry.Name == filer.IamIdentityFile) ||
			(newEntry != nil && newEntry.Name == filer.IamIdentityFile) {
			if err := reloadIamConfig(dir + "/" + filer.IamIdentityFile); err != nil {
				return err
			}
		}
		return nil
	}

	// 2. Handle multiple-file identities and policies
	// Watch /etc/iam/{identities,policies,service_accounts}
	isIdentityDir := dir == filer.IamConfigDirectory+"/identities" || strings.HasPrefix(dir, filer.IamConfigDirectory+"/identities/")
	isPolicyDir := dir == filer.IamConfigDirectory+"/policies" || strings.HasPrefix(dir, filer.IamConfigDirectory+"/policies/")
	isServiceAccountDir := dir == filer.IamConfigDirectory+"/service_accounts" || strings.HasPrefix(dir, filer.IamConfigDirectory+"/service_accounts/")
	isGroupDir := dir == filer.IamConfigDirectory+"/groups" || strings.HasPrefix(dir, filer.IamConfigDirectory+"/groups/")

	if isIdentityDir || isPolicyDir || isServiceAccountDir || isGroupDir {
		// For multiple-file mode, any change in these directories should trigger a full reload
		// from the credential manager (which handles the details of loading from multiple files).
		if err := reloadIamConfig(dir); err != nil {
			return err
		}
	}

	return nil
}

// onOIDCProviderChange refreshes the IAM-managed OIDC provider runtime view
// whenever the persisted store under /etc/iam/oidc-providers changes — both
// for mutations originated on this S3 server (the local IAM API also calls
// RefreshOIDCProvidersFromStore inline, but the subscribe path costs nothing
// extra) and for mutations originated on peer S3 servers, which this is the
// only mechanism to learn about. A single refresh covers create, update,
// delete, and rename because the store is small and a full reload is the
// safest way to reach a consistent view.
func (s3a *S3ApiServer) onOIDCProviderChange(dir string, oldEntry *filer_pb.Entry, newEntry *filer_pb.Entry) error {
	if dir != oidcProvidersDir && !strings.HasPrefix(dir, oidcProvidersDir+"/") {
		return nil
	}
	if s3a.iam == nil || s3a.iam.iamIntegration == nil {
		return nil
	}
	s3iam, ok := s3a.iam.iamIntegration.(*S3IAMIntegration)
	if !ok || s3iam.iamManager == nil {
		return nil
	}
	if err := s3iam.iamManager.RefreshOIDCProvidersFromStore(context.Background()); err != nil {
		glog.Warningf("OIDC provider refresh after %s change failed: %v", dir, err)
		return err
	}
	glog.V(2).Infof("Refreshed IAM-managed OIDC providers after %s change", dir)
	return nil
}

// onCircuitBreakerConfigChange handles circuit breaker config file changes (create, update, delete)
func (s3a *S3ApiServer) onCircuitBreakerConfigChange(dir string, oldEntry *filer_pb.Entry, newEntry *filer_pb.Entry) error {
	if dir != s3_constants.CircuitBreakerConfigDir {
		return nil
	}

	// Handle deletion: reset to empty config
	if newEntry == nil && oldEntry != nil && oldEntry.Name == s3_constants.CircuitBreakerConfigFile {
		glog.V(1).Infof("Circuit breaker config file deleted, resetting to defaults")
		if err := s3a.cb.LoadS3ApiConfigurationFromBytes([]byte{}); err != nil {
			glog.Warningf("failed to reset circuit breaker config on deletion: %v", err)
			return err
		}
		return nil
	}

	// Handle create/update
	if newEntry != nil && newEntry.Name == s3_constants.CircuitBreakerConfigFile {
		if err := s3a.cb.LoadS3ApiConfigurationFromBytes(newEntry.Content); err != nil {
			return err
		}
		glog.V(1).Infof("updated %s/%s", dir, newEntry.Name)
	}
	return nil
}

// reload bucket metadata
func (s3a *S3ApiServer) onBucketMetadataChange(dir string, oldEntry *filer_pb.Entry, newEntry *filer_pb.Entry) error {
	if dir == s3a.option.BucketsPath {
		s3a.maintainBucketOwnerIndex(oldEntry, newEntry)
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

	// Remove from negative cache since bucket now exists
	// This is important for buckets created via weed shell or other external means
	s3a.bucketConfigCache.RemoveNegativeCache(bucket)

	// Only refresh buckets already resident in the cache; cold buckets
	// lazy-load on first access so the cache holds this gateway's working
	// set, not every bucket in the cluster.
	if !s3a.bucketConfigCache.Contains(bucket) {
		return
	}

	// newBucketConfigFromEntry is the single source of truth for mapping
	// Entry.Extended → cached fields (incl. LifecycleTTL), so a meta-log
	// Put/DeleteBucketLifecycle here can't leave a stale resolver in cache.
	config := s3a.newBucketConfigFromEntry(bucket, entry)

	glog.V(3).Infof("updateBucketConfigCacheFromEntry: refreshing cache for bucket %s, ObjectLockConfig=%+v", bucket, config.ObjectLockConfig)
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
