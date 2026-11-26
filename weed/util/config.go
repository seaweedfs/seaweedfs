package util

import (
	"strings"
	"sync"
	"time"

	"github.com/spf13/viper"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

var (
	ConfigurationFileDirectory DirectoryValueType
	loadSecurityConfigOnce     sync.Once
)

type DirectoryValueType string

func (s *DirectoryValueType) Set(value string) error {
	*s = DirectoryValueType(value)
	return nil
}
func (s *DirectoryValueType) String() string {
	return string(*s)
}

type Configuration interface {
	GetString(key string) string
	GetBool(key string) bool
	GetInt(key string) int
	GetStringSlice(key string) []string
	SetDefault(key string, value interface{})
}

// securityConfigFile names the security.toml that LoadConfiguration read, or
// "security.toml" when the settings came from the environment only.
func securityConfigFile() string {
	if f := viper.ConfigFileUsed(); f != "" {
		return f
	}
	return "security.toml"
}

// LoadSecurityConfiguration merges security.toml once per process and exits
// when a JWT signing key is too short for GODEBUG=fips140=only; outside that
// mode it warns about such a key.
func LoadSecurityConfiguration() {
	loadSecurityConfigOnce.Do(func() {
		LoadConfiguration("security", false)
		if err := CheckFips140SigningKeys(securityConfigFile(), GetViper()); err != nil {
			glog.Fatalf("%s: this process runs with GODEBUG=fips140=only, and stops because %v; replace them on every server that uses them, with or without downtime, see %s",
				securityConfigFile(), err, JwtSigningKeysWiki)
		}
	})
}

func LoadConfiguration(configFileName string, required bool) (loaded bool) {
	// MergeInConfig mutates the shared viper that ViperProxy serializes;
	// take the same lock so a merge cannot race a reader or SetDefault.
	vp.Lock()
	defer vp.Unlock()

	setConfigurationPaths(viper.GetViper(), configFileName)
	if err := viper.MergeInConfig(); err != nil { // Handle errors reading the config file
		if strings.Contains(err.Error(), "Not Found") {
			glog.V(1).Infof("Reading %s: %v", viper.ConfigFileUsed(), err)
		} else {
			// If the config is required, fail immediately
			if required {
				glog.Fatalf("Reading %s: %v", viper.ConfigFileUsed(), err)
			}
			// If the config is optional, log a warning but don't crash
			glog.Warningf("Reading %s: %v. Skipping optional configuration.", viper.ConfigFileUsed(), err)
		}
		if required {
			glog.Fatalf("Failed to load %s.toml file from current directory, or $HOME/.seaweedfs/, or /etc/seaweedfs/"+
				"\n\nPlease use this command to generate the default %s.toml file\n"+
				"    weed scaffold -config=%s -output=.\n\n\n",
				configFileName, configFileName, configFileName)
		}
		return false
	}
	glog.V(1).Infof("Reading %s.toml from %s", configFileName, viper.ConfigFileUsed())

	return true
}

// ReadConfiguration reads configFileName into a new viper with the WEED_
// environment overrides. Unlike LoadConfiguration it does not merge, so a key
// the file no longer sets reads as unset. A non-empty configFile is read in
// place of searching the configuration directories. used is the file read, or
// empty when none could be; the environment still applies then.
func ReadConfiguration(configFileName, configFile string) (v *ViperProxy, used string) {
	fresh := viper.New()
	setEnvOverrides(fresh)
	if configFile != "" {
		fresh.SetConfigFile(configFile)
	} else {
		setConfigurationPaths(fresh, configFileName)
	}
	if err := fresh.ReadInConfig(); err != nil {
		glog.V(1).Infof("Reading %s: %v", configFileName, err)
		return NewViperProxy(fresh), ""
	}
	return NewViperProxy(fresh), fresh.ConfigFileUsed()
}

func setConfigurationPaths(v *viper.Viper, configFileName string) {
	v.SetConfigName(configFileName)
	v.AddConfigPath(ResolvePath(ConfigurationFileDirectory.String()))
	v.AddConfigPath(".")
	v.AddConfigPath("$HOME/.seaweedfs")
	v.AddConfigPath("/usr/local/etc/seaweedfs/")
	v.AddConfigPath("/etc/seaweedfs/")
}

func setEnvOverrides(v *viper.Viper) {
	v.AutomaticEnv()
	v.SetEnvPrefix("weed")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
}

// ViperProxy serializes access to the global viper. The wrapped Viper is a
// named field, not embedded, so every method must be declared here under the
// mutex — a promoted method would take no lock and race, e.g. GetStringMap
// against a concurrent SetDefault during `weed server` startup.
type ViperProxy struct {
	v *viper.Viper
	sync.Mutex
}

var (
	vp = &ViperProxy{}
)

func (vp *ViperProxy) SetDefault(key string, value interface{}) {
	vp.Lock()
	defer vp.Unlock()
	vp.v.SetDefault(key, value)
}

func (vp *ViperProxy) AutomaticEnv() {
	vp.Lock()
	defer vp.Unlock()
	vp.v.AutomaticEnv()
}

func (vp *ViperProxy) GetString(key string) string {
	vp.Lock()
	defer vp.Unlock()
	return vp.v.GetString(key)
}

func (vp *ViperProxy) GetBool(key string) bool {
	vp.Lock()
	defer vp.Unlock()
	return vp.v.GetBool(key)
}

func (vp *ViperProxy) GetInt(key string) int {
	vp.Lock()
	defer vp.Unlock()
	return vp.v.GetInt(key)
}

func (vp *ViperProxy) GetStringSlice(key string) []string {
	vp.Lock()
	defer vp.Unlock()
	// a []string value comes back uncast — the same shared subtree issue
	// GetStringMap has, so hand the caller a copy
	return append([]string(nil), vp.v.GetStringSlice(key)...)
}

func (vp *ViperProxy) GetStringMap(key string) map[string]interface{} {
	vp.Lock()
	defer vp.Unlock()
	// viper hands back its internal subtree, so a caller iterating it after
	// the lock is released would race the next SetDefault — copy it out.
	return deepCopyStringMap(vp.v.GetStringMap(key))
}

func deepCopyStringMap(m map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(m))
	for k, v := range m {
		out[k] = deepCopyValue(v)
	}
	return out
}

func deepCopyValue(v interface{}) interface{} {
	switch nested := v.(type) {
	case map[string]interface{}:
		return deepCopyStringMap(nested)
	case []interface{}:
		out := make([]interface{}, len(nested))
		for i, item := range nested {
			out[i] = deepCopyValue(item)
		}
		return out
	default:
		return v
	}
}

func (vp *ViperProxy) GetUint32(key string) uint32 {
	vp.Lock()
	defer vp.Unlock()
	return vp.v.GetUint32(key)
}

func (vp *ViperProxy) GetFloat64(key string) float64 {
	vp.Lock()
	defer vp.Unlock()
	return vp.v.GetFloat64(key)
}

func (vp *ViperProxy) GetDuration(key string) time.Duration {
	vp.Lock()
	defer vp.Unlock()
	return vp.v.GetDuration(key)
}

func (vp *ViperProxy) Set(key string, value interface{}) {
	vp.Lock()
	defer vp.Unlock()
	vp.v.Set(key, value)
}

func (vp *ViperProxy) IsSet(key string) bool {
	vp.Lock()
	defer vp.Unlock()
	return vp.v.IsSet(key)
}

func (vp *ViperProxy) AllKeys() []string {
	vp.Lock()
	defer vp.Unlock()
	return vp.v.AllKeys()
}

// NewViperProxy wraps a specific viper — for configuration loaded from a
// source other than the shared instance GetViper returns.
func NewViperProxy(v *viper.Viper) *ViperProxy {
	return &ViperProxy{v: v}
}

func GetViper() *ViperProxy {
	vp.Lock()
	defer vp.Unlock()

	if vp.v == nil {
		vp.v = viper.GetViper()
		setEnvOverrides(vp.v)
	}

	return vp
}
