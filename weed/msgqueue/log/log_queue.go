package kafka

import (
	_ "github.com/go-sql-driver/mysql"
	"github.com/chrislusf/seaweedfs/weed/msgqueue"
	"github.com/golang/protobuf/proto"
	"github.com/chrislusf/seaweedfs/weed/glog"
)

func init() {
	msgqueue.MessageQueues = append(msgqueue.MessageQueues, &LogQueue{})
}

type LogQueue struct {
}

func (k *LogQueue) GetName() string {
	return "log"
}

func (k *LogQueue) Initialize(configuration msgqueue.Configuration) (err error) {
	return nil
}

func (k *LogQueue) SendMessage(key string, message proto.Message) (err error) {

	glog.V(0).Infof("%v: %+v", key, message)
	return nil
}
