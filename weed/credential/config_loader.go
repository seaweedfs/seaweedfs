package credential

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// CredentialConfig represents the credential configuration from credential.toml
type CredentialConfig struct {
	Store  string
	Config util.Configuration
	Prefix string
}

// LoadCredentialConfiguration loads credential configuration from credential.toml
// Returns the store type, configuration, and prefix for credential management
func LoadCredentialConfiguration() (*CredentialConfig, error) {
	// Try to load credential.toml configuration
	loaded := util.LoadConfiguration("credential", false)
	if !loaded {
		glog.V(1).Info("No credential.toml found, credential store disabled")
		return nil, nil
	}

	viper := util.GetViper()

	// Find which credential store is enabled
	var enabledStore string
	var storePrefix string

	// Get available store types from registered stores
	storeTypes := GetAvailableStores()
	for _, storeType := range storeTypes {
		key := fmt.Sprintf("credential.%s.enabled", storeType)
		if viper.GetBool(key) {
			if enabledStore != "" {
				return nil, fmt.Errorf("multiple credential stores enabled: %s and %s. Only one store can be enabled", enabledStore, storeType)
			}
			enabledStore = storeType
			storePrefix = fmt.Sprintf("credential.%s.", storeType)
		}
	}

	if enabledStore == "" {
		glog.V(1).Info("No credential store enabled in credential.toml")
		return nil, nil
	}

	glog.V(0).Infof("Loaded credential configuration: store=%s", enabledStore)

	return &CredentialConfig{
		Store:  enabledStore,
		Config: viper,
		Prefix: storePrefix,
	}, nil
}

// GetCredentialStoreConfig extracts credential store configuration from command line flags
// This is used when credential store is configured via command line instead of credential.toml
func GetCredentialStoreConfig(store string, config util.Configuration, prefix string) *CredentialConfig {
	if store == "" {
		return nil
	}

	return &CredentialConfig{
		Store:  store,
		Config: config,
		Prefix: prefix,
	}
}

// MergeCredentialConfig merges command line credential config with credential.toml config
// Command line flags take priority over credential.toml
func MergeCredentialConfig(cmdLineStore string, cmdLineConfig util.Configuration, cmdLinePrefix string) (*CredentialConfig, error) {
	// If command line credential store is specified, use it
	if cmdLineStore != "" {
		glog.V(0).Infof("Using command line credential configuration: store=%s", cmdLineStore)
		return GetCredentialStoreConfig(cmdLineStore, cmdLineConfig, cmdLinePrefix), nil
	}

	// Otherwise, try to load from credential.toml
	config, err := LoadCredentialConfiguration()
	if err != nil {
		return nil, err
	}

	if config == nil {
		glog.V(1).Info("No credential store configured")
	}

	return config, nil
}
