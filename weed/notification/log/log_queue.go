package kafka

import (
	"path"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/notification"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/protobuf/proto"
)

func init() {
	notification.MessageQueues = append(notification.MessageQueues, &LogQueue{})
}

type LogQueue struct {
}

func (k *LogQueue) GetName() string {
	return "log"
}

func (k *LogQueue) Initialize(configuration util.Configuration, prefix string) (err error) {
	return nil
}

func (k *LogQueue) SendMessage(key string, message proto.Message) (err error) {

	// entries carry per-chunk cipher keys; log names only
	if resp, ok := message.(*filer_pb.SubscribeMetadataResponse); ok {
		if notification := resp.GetEventNotification(); notification != nil {
			_ = k.SendMessage(key, notification)
		}
		for _, event := range resp.GetEvents() {
			_ = k.SendMessage(key, event)
		}
		return nil
	}
	if event, ok := message.(*filer_pb.EventNotification); ok {
		name := event.GetNewEntry().GetName()
		if parent := event.GetNewParentPath(); parent != "" {
			name = path.Join(parent, name)
		}
		glog.V(0).Infof("%v: %q -> %q", key, event.GetOldEntry().GetName(), name)
		return nil
	}
	glog.V(0).Infof("%v: %T", key, message)
	return nil
}
