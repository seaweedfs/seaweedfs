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

	for _, store := range MessageQueues {
		if config.GetBool(store.GetName() + ".enabled") {
			viperSub := config.Sub(store.GetName())
			if err := store.Initialize(viperSub); err != nil {
				glog.Fatalf("Failed to initialize notification for %s: %+v",
					store.GetName(), err)
			}
			Queue = store
			glog.V(0).Infof("Configure notification message queue for %s", store.GetName())
			return
		}
	}

}
