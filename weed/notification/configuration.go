package notification

import (
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/golang/protobuf/proto"
	"github.com/spf13/viper"
)

type MessageQueue interface {
	// GetName gets the name to locate the configuration in filer.toml file
	GetName() string
	// Initialize initializes the file store
	Initialize(configuration util.Configuration) error
	SendMessage(key string, message proto.Message) error
}

var (
	MessageQueues []MessageQueue

	Queue MessageQueue
)

func LoadConfiguration(config *viper.Viper) {

	if config == nil {
		return
	}

	validateOneEnabledQueue(config)

	for _, queue := range MessageQueues {
		if config.GetBool(queue.GetName() + ".enabled") {
			viperSub := config.Sub(queue.GetName())
			if err := queue.Initialize(viperSub); err != nil {
				glog.Fatalf("Failed to initialize notification for %s: %+v",
					queue.GetName(), err)
			}
			Queue = queue
			glog.V(0).Infof("Configure notification message queue for %s", queue.GetName())
			return
		}
	}

}

func validateOneEnabledQueue(config *viper.Viper) {
	enabledQueue := ""
	for _, queue := range MessageQueues {
		if config.GetBool(queue.GetName() + ".enabled") {
			if enabledQueue == "" {
				enabledQueue = queue.GetName()
			} else {
				glog.Fatalf("Notification message queue is enabled for both %s and %s", enabledQueue, queue.GetName())
			}
		}
	}
}
