package s3api

import (
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/s3api/s3_constants"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func (s3a *S3ApiServer) subscribeMetaEvents(clientName string, prefix string, lastTsNs int64) {

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

		return nil
	}

	util.RetryForever("followIamChanges", func() error {
		return pb.WithFilerClientFollowMetadata(s3a, clientName, s3a.randomClientId, prefix, &lastTsNs, 0, 0, processEventFn, pb.FatalOnError)
	}, func(err error) bool {
		glog.V(0).Infof("iam follow metadata changes: %v", err)
		return true
	})
}

//reload iam config
func (s3a *S3ApiServer) onIamConfigUpdate(dir, filename string, content []byte) error {
	if dir == filer.IamConfigDirecotry && filename == filer.IamIdentityFile {
		if err := s3a.iam.LoadS3ApiConfigurationFromBytes(content); err != nil {
			return err
		}
		glog.V(0).Infof("updated %s/%s", dir, filename)
	}
	return nil
}

//reload circuit breaker config
func (s3a *S3ApiServer) onCircuitBreakerConfigUpdate(dir, filename string, content []byte) error {
	if dir == s3_constants.CircuitBreakerConfigDir && filename == s3_constants.CircuitBreakerConfigFile {
		if err := s3a.cb.LoadS3ApiConfigurationFromBytes(content); err != nil {
			return err
		}
		glog.V(0).Infof("updated %s/%s", dir, filename)
	}
	return nil
}
