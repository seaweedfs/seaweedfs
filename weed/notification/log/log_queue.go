package kafka

import (
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
	if event, ok := message.(*filer_pb.EventNotification); ok {
		glog.V(0).Infof("%v: %q -> %q", key, event.GetOldEntry().GetName(), event.GetNewEntry().GetName())
		return nil
	}
	glog.V(0).Infof("%v: %+v", key, message)
	return nil
}
