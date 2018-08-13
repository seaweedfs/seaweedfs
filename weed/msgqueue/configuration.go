package msgqueue

import (
	"os"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/spf13/viper"
)

const (
	MSG_QUEUE_TOML_EXAMPLE = `
# A sample TOML config file for SeaweedFS message queue

[log]
enabled = true

[kafka]
enabled = false
hosts = [
  "localhost:9092"
]
topic = "seaweedfs_filer"

`
)

var (
	MessageQueues []MessageQueue

	Queue MessageQueue
)

func LoadConfiguration() {

	// find a filer store
	viper.SetConfigName("message_queue")    // name of config file (without extension)
	viper.AddConfigPath(".")                // optionally look for config in the working directory
	viper.AddConfigPath("$HOME/.seaweedfs") // call multiple times to add many search paths
	viper.AddConfigPath("/etc/seaweedfs/")  // path to look for the config file in
	if err := viper.ReadInConfig(); err != nil { // Handle errors reading the config file
		glog.Fatalf("Failed to load message_queue.toml file from current directory, or $HOME/.seaweedfs/, "+
			"or /etc/seaweedfs/"+
			"\n\nPlease follow this example and add a message_queue.toml file to "+
			"current directory, or $HOME/.seaweedfs/, or /etc/seaweedfs/:\n"+MSG_QUEUE_TOML_EXAMPLE, err)
	}

	glog.V(0).Infof("Reading message queue configuration from %s", viper.ConfigFileUsed())
	for _, store := range MessageQueues {
		if viper.GetBool(store.GetName() + ".enabled") {
			viperSub := viper.Sub(store.GetName())
			if err := store.Initialize(viperSub); err != nil {
				glog.Fatalf("Failed to initialize store for %s: %+v",
					store.GetName(), err)
			}
			Queue = store
			glog.V(0).Infof("Configure message queue for %s from %s", store.GetName(), viper.ConfigFileUsed())
			return
		}
	}

	println()
	println("Supported message queues are:")
	for _, store := range MessageQueues {
		println("    " + store.GetName())
	}

	println()
	println("Please configure a supported message queue in", viper.ConfigFileUsed())
	println()

	os.Exit(-1)
}

// A simplified interface to decouple from Viper
type Configuration interface {
	GetString(key string) string
	GetBool(key string) bool
	GetInt(key string) int
	GetInt64(key string) int64
	GetFloat64(key string) float64
	GetStringSlice(key string) []string
}
