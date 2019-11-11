package util

import (
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/spf13/viper"
)

type Configuration interface {
	GetString(key string) string
	GetBool(key string) bool
	GetInt(key string) int
	GetInt64(key string) int64
	GetFloat64(key string) float64
	GetStringSlice(key string) []string
}

func LoadConfiguration(configFileName string, required bool) (loaded bool) {

	// find a filer store
	viper.SetConfigName(configFileName)     // name of config file (without extension)
	viper.AddConfigPath(".")                // optionally look for config in the working directory
	viper.AddConfigPath("$HOME/.seaweedfs") // call multiple times to add many search paths
	viper.AddConfigPath("/etc/seaweedfs/")  // path to look for the config file in

	glog.V(1).Infof("Reading %s.toml from %s", configFileName, viper.ConfigFileUsed())

	if err := viper.MergeInConfig(); err != nil { // Handle errors reading the config file
		glog.V(0).Infof("Reading %s: %v", viper.ConfigFileUsed(), err)
		if required {
			glog.Fatalf("Failed to load %s.toml file from current directory, or $HOME/.seaweedfs/, or /etc/seaweedfs/"+
				"\n\nPlease follow this example and add a filer.toml file to "+
				"current directory, or $HOME/.seaweedfs/, or /etc/seaweedfs/:\n"+
				"    https://github.com/chrislusf/seaweedfs/blob/master/weed/%s.toml\n"+
				"\nOr use this command to generate the default toml file\n"+
				"    weed scaffold -config=%s -output=.\n\n\n",
				configFileName, configFileName, configFileName)
		} else {
			return false
		}
	}

	return true
}

func Config() Configuration {
	return viper.GetViper()
}

