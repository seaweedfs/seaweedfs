package kafka

import (
	"github.com/joeslay/seaweedfs/weed/glog"
	"github.com/joeslay/seaweedfs/weed/notification"
	"github.com/joeslay/seaweedfs/weed/util"
	"github.com/golang/protobuf/proto"
)

func init() {
	notification.MessageQueues = append(notification.MessageQueues, &LogQueue{})
}

type LogQueue struct {
}

func (k *LogQueue) GetName() string {
	return "log"
}

func (k *LogQueue) Initialize(configuration util.Configuration) (err error) {
	return nil
}

func (k *LogQueue) SendMessage(key string, message proto.Message) (err error) {

	glog.V(0).Infof("%v: %+v", key, message)
	return nil
}
