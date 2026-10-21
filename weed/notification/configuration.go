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

	validateOneEnabledQueue(config, prefix)

	for _, queue := range MessageQueues {
		if queueEnabled(config, prefix, queue) {
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

// queueEnabled reports whether a queue is switched on — by the flat
// "<prefix><name>.enabled" flag or, for queues accepting named sub-instances
// ([notification.<queue>.<target>]), by any enabled subconfig.
func queueEnabled(config util.Configuration, prefix string, queue MessageQueue) bool {
	key := prefix + queue.GetName() + "."
	if config.GetBool(key + "enabled") {
		return true
	}
	if q, ok := queue.(interface {
		HasEnabledSubconfig(util.Configuration, string) bool
	}); ok {
		return q.HasEnabledSubconfig(config, key)
	}
	return false
}

func validateOneEnabledQueue(config *util.ViperProxy, prefix string) {
	enabledQueue := ""
	for _, queue := range MessageQueues {
		if queueEnabled(config, prefix, queue) {
			if enabledQueue == "" {
				enabledQueue = queue.GetName()
			} else {
				glog.Fatalf("Notification message queue is enabled for both %s and %s", enabledQueue, queue.GetName())
			}
		}
	}
}
