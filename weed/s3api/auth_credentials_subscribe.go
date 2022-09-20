package s3api

import (
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func (s3a *S3ApiServer) subscribeMetaEvents(clientName string, lastTsNs int64, prefixes ...string) {

	processEventFn := func(resp *filer_pb.SubscribeMetadataResponse) error {

		message := resp.EventNotification
		if message.NewEntry == nil {
			return nil
		}

		dir := resp.Directory

		if message.NewParentPath != "" {
			dir = message.NewParentPath
		}

		_ = s3a.onIamConfigUpdate(dir, message.NewEntry)
		_ = s3a.onCircuitBreakerConfigUpdate(dir, message.NewEntry)
		_ = s3a.onBucketMetadataChange(dir, message.OldEntry, message.NewEntry)

		return nil
	}

	var clientEpoch int32
	util.RetryForever("followIamChanges", func() error {
		clientEpoch++
		return pb.WithFilerClientFollowMetadataInPrefixes(s3a, clientName, s3a.randomClientId, clientEpoch, prefixes, &lastTsNs, 0, 0, processEventFn, pb.FatalOnError)
	}, func(err error) bool {
		glog.V(0).Infof("iam follow metadata changes: %v", err)
		return true
	})
}

//reload iam config
func (s3a *S3ApiServer) onIamConfigUpdate(dir string, entry *filer_pb.Entry) error {
	if dir == filer.IamConfigDirectory && entry.Name == filer.IamIdentityFile {
		if err := s3a.iam.LoadS3ApiConfigurationFromBytes(entry.Content); err != nil {
			return err
		}
		glog.V(0).Infof("updated %s/%s", dir, entry.Name)
	}
	return nil
}

//reload circuit breaker config
func (s3a *S3ApiServer) onCircuitBreakerConfigUpdate(dir string, entry *filer_pb.Entry) error {
	if dir == s3_constants.CircuitBreakerConfigDir && entry.Name == s3_constants.CircuitBreakerConfigFile {
		if err := s3a.cb.LoadS3ApiConfigurationFromBytes(entry.Content); err != nil {
			return err
		}
		glog.V(0).Infof("updated %s/%s", dir, entry.Name)
	}
	return nil
}

//reload bucket metadata
func (s3a *S3ApiServer) onBucketMetadataChange(dir string, oldEntry *filer_pb.Entry, newEntry *filer_pb.Entry) error {
	if dir == s3a.option.BucketsPath {
		if newEntry != nil {
			s3a.LoadBucketMetadata(newEntry)
			glog.V(0).Infof("updated bucketMetadata %s/%s", dir, newEntry)
		} else {
			s3a.RemoveBucketMetadata(oldEntry)
			glog.V(0).Infof("remove bucketMetadata  %s/%s", dir, newEntry)
		}
	}
	return nil
}
