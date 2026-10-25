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
		key := fmt.Sprintf("credential.%s.enabled", string(storeType))
		if viper.GetBool(key) {
			if enabledStore != "" {
				return nil, fmt.Errorf("multiple credential stores enabled: %s and %s. Only one store can be enabled", enabledStore, string(storeType))
			}
			enabledStore = string(storeType)
			storePrefix = fmt.Sprintf("credential.%s.", string(storeType))
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

// NewCredentialManagerWithDefaults creates a credential manager with fallback to defaults
// If explicitStore is provided, it will be used regardless of credential.toml
// If explicitStore is empty, it tries credential.toml first, then defaults to "filer_etc"
func NewCredentialManagerWithDefaults(explicitStore CredentialStoreTypeName) (*CredentialManager, error) {
	var storeName CredentialStoreTypeName
	var config util.Configuration
	var prefix string

	// If explicit store is provided, use it
	if explicitStore != "" {
		storeName = explicitStore
		config = nil
		prefix = ""
		glog.V(0).Infof("Using explicit credential store: %s", storeName)
	} else {
		// Try to load from credential.toml first
		if credConfig, err := LoadCredentialConfiguration(); err == nil && credConfig != nil {
			storeName = CredentialStoreTypeName(credConfig.Store)
			config = credConfig.Config
			prefix = credConfig.Prefix
			glog.V(0).Infof("Loaded credential configuration from credential.toml: store=%s", storeName)
		} else {
			// Default to filer_etc store
			storeName = StoreTypeFilerEtc
			config = nil
			prefix = ""
			glog.V(1).Info("No credential.toml found, defaulting to filer_etc store")
		}
	}

	// Create the credential manager
	credentialManager, err := NewCredentialManager(storeName, config, prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize credential manager with store '%s': %v", storeName, err)
	}

	return credentialManager, nil
}
