package filer

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"os"
	"reflect"
	"strings"
)

var (
	Stores []FilerStore
)

func (f *Filer) LoadConfiguration(config *util.ViperProxy) (isFresh bool) {

	validateOneEnabledStore(config)

	// load configuration for default filer store
	hasDefaultStoreConfigured := false
	for _, store := range Stores {
		if config.GetBool(store.GetName() + ".enabled") {
			store = reflect.New(reflect.ValueOf(store).Elem().Type()).Interface().(FilerStore)
			if err := store.Initialize(config, store.GetName()+"."); err != nil {
				glog.Fatalf("failed to initialize store for %s: %+v", store.GetName(), err)
			}
			isFresh = f.SetStore(store)
			glog.V(0).Infof("configured filer store to %s", store.GetName())
			hasDefaultStoreConfigured = true
			break
		}
	}

	if !hasDefaultStoreConfigured {
		println()
		println("Supported filer stores are the following. If not found, check the full version.")
		for _, store := range Stores {
			println("    " + store.GetName())
		}
		os.Exit(-1)
	}

	// load path-specific filer store here
	// f.Store.AddPathSpecificStore(path, store)
	storeNames := make(map[string]FilerStore)
	for _, store := range Stores {
		storeNames[store.GetName()] = store
	}
	allKeys := config.AllKeys()
	for _, key := range allKeys {
		if !strings.HasSuffix(key, ".enabled") {
			continue
		}
		key = key[:len(key)-len(".enabled")]
		if !strings.Contains(key, ".") {
			continue
		}

		parts := strings.Split(key, ".")
		storeName, storeId := parts[0], parts[1]

		store, found := storeNames[storeName]
		if !found {
			continue
		}

		if !config.GetBool(key + ".enabled") {
			continue
		}

		store = reflect.New(reflect.ValueOf(store).Elem().Type()).Interface().(FilerStore)
		if err := store.Initialize(config, key+"."); err != nil {
			glog.Fatalf("Failed to initialize store for %s: %+v", key, err)
		}
		location := config.GetString(key + ".location")
		if location == "" {
			glog.Errorf("path-specific filer store needs %s", key+".location")
			os.Exit(-1)
		}
		f.Store.AddPathSpecificStore(location, storeId, store)

		glog.V(0).Infof("configure filer %s for %s", store.GetName(), location)
	}

	return
}

func validateOneEnabledStore(config *util.ViperProxy) {
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
