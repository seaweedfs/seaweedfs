package notification

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/protobuf/proto"
)

type MessageQueue interface {
	// GetName gets the name to locate the configuration in filer.toml file
	GetName() string
	// Initialize initializes the file store
	Initialize(configuration util.Configuration, prefix string) error
	SendMessage(key string, message proto.Message) error
}

var (
	MessageQueues []MessageQueue

	Queue MessageQueue
)

func LoadConfiguration(config *util.ViperProxy, prefix string) {

	if config == nil {
		return
	}

	validateOneEnabledQueue(config)

	for _, queue := range MessageQueues {
		if config.GetBool(prefix + queue.GetName() + ".enabled") {
			if err := queue.Initialize(config, prefix+queue.GetName()+"."); err != nil {
				glog.Fatalf("Failed to initialize notification for %s: %+v",
					queue.GetName(), err)
			}
			Queue = queue
			glog.V(0).Infof("Configure notification message queue for %s", queue.GetName())
			return
		}
	}

}

func validateOneEnabledQueue(config *util.ViperProxy) {
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
