package kafka

import (
	"github.com/HZ89/seaweedfs/weed/glog"
	"github.com/HZ89/seaweedfs/weed/notification"
	"github.com/HZ89/seaweedfs/weed/util"
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
