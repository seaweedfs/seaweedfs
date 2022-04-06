package s3api

import (
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
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
		if dir == filer.IamConfigDirecotry && message.NewEntry.Name == filer.IamIdentityFile {
			if err := s3a.iam.loadS3ApiConfigurationFromBytes(message.NewEntry.Content); err != nil {
				return err
			}
			glog.V(0).Infof("updated %s/%s", filer.IamConfigDirecotry, filer.IamIdentityFile)
		}

		return nil
	}

	util.RetryForever("followIamChanges", func() error {
		return pb.WithFilerClientFollowMetadata(s3a, clientName, s3a.randomClientId, prefix, &lastTsNs, 0, processEventFn, true)
	}, func(err error) bool {
		glog.V(0).Infof("iam follow metadata changes: %v", err)
		return true
	})

}
