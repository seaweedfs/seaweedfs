package msgqueue

import (
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/spf13/viper"
)

var (
	MessageQueues []MessageQueue

	Queue MessageQueue
)

func LoadConfiguration(config *viper.Viper) {

	for _, store := range MessageQueues {
		if config.GetBool(store.GetName() + ".enabled") {
			viperSub := config.Sub(store.GetName())
			if err := store.Initialize(viperSub); err != nil {
				glog.Fatalf("Failed to initialize store for %s: %+v",
					store.GetName(), err)
			}
			Queue = store
			glog.V(0).Infof("Configure message queue for %s", store.GetName())
			return
		}
	}

}
