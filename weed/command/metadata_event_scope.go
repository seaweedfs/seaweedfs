package command

import (
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func metadataEventDirectoryMembership(resp *filer_pb.SubscribeMetadataResponse, dir string) (sourceInDir, targetInDir bool) {
	if resp == nil || resp.EventNotification == nil {
		return false, false
	}

	sourceInDir = util.IsEqualOrUnder(resp.Directory, dir)
	targetInDir = resp.EventNotification.NewEntry != nil &&
		util.IsEqualOrUnder(filer_pb.MetadataEventTargetDirectory(resp), dir)

	return sourceInDir, targetInDir
}

func metadataEventUpdatesDirectory(resp *filer_pb.SubscribeMetadataResponse, dir string) bool {
	if resp == nil || resp.EventNotification == nil || resp.EventNotification.NewEntry == nil {
		return false
	}

	_, targetInDir := metadataEventDirectoryMembership(resp, dir)
	return targetInDir
}

func metadataEventRemovesFromDirectory(resp *filer_pb.SubscribeMetadataResponse, dir string) bool {
	if resp == nil || resp.EventNotification == nil || resp.EventNotification.OldEntry == nil {
		return false
	}

	sourceInDir, targetInDir := metadataEventDirectoryMembership(resp, dir)
	return sourceInDir && !targetInDir
}
