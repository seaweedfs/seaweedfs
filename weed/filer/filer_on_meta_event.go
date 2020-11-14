package filer

import (
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
)

const (
	DirectoryEtc = "/etc"
)

// onMetadataChangeEvent is triggered after filer processed change events from local or remote filers
func (f *Filer) onMetadataChangeEvent(event *filer_pb.SubscribeMetadataResponse) {
	if DirectoryEtc != event.Directory {
		return
	}

	glog.V(0).Infof("procesing %v", event)


}
