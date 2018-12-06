package filer2

import (
	"os"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/spf13/viper"
)

var (
	Stores []FilerStore
)

func (f *Filer) LoadConfiguration(config *viper.Viper) {

	validateOneEnabledStore(config)

	for _, store := range Stores {
		if config.GetBool(store.GetName() + ".enabled") {
			viperSub := config.Sub(store.GetName())
			if err := store.Initialize(viperSub); err != nil {
				glog.Fatalf("Failed to initialize store for %s: %+v",
					store.GetName(), err)
			}
			f.SetStore(store)
			glog.V(0).Infof("Configure filer for %s", store.GetName())
			return
		}
	}

	println()
	println("Supported filer stores are:")
	for _, store := range Stores {
		println("    " + store.GetName())
	}

	os.Exit(-1)
}

func validateOneEnabledStore(config *viper.Viper) {
	enabledStore := ""
	for _, store := range Stores {
		if config.GetBool(store.GetName() + ".enabled") {
			if enabledStore == "" {
				enabledStore = store.GetName()
			} else {
				glog.Fatalf("Filer store is enabled for both %s and %s", enabledStore, store.GetName())
			}
		}
	}
}
