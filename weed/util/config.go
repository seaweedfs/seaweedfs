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

func LoadSecurityConfiguration() {
	loadSecurityConfigOnce.Do(func() {
		LoadConfiguration("security", false)
	})
}

func LoadConfiguration(configFileName string, required bool) (loaded bool) {
	// MergeInConfig mutates the shared viper that ViperProxy serializes;
	// take the same lock so a merge cannot race a reader or SetDefault.
	vp.Lock()
	defer vp.Unlock()

	// find a filer store
	viper.SetConfigName(configFileName)                                   // name of config file (without extension)
	viper.AddConfigPath(ResolvePath(ConfigurationFileDirectory.String())) // path to look for the config file in
	viper.AddConfigPath(".")                                              // optionally look for config in the working directory
	viper.AddConfigPath("$HOME/.seaweedfs")                               // call multiple times to add many search paths
	viper.AddConfigPath("/usr/local/etc/seaweedfs/")                      // search path for bsd-style config directory in
	viper.AddConfigPath("/etc/seaweedfs/")                                // path to look for the config file in

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
	return vp.v.GetStringSlice(key)
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
		switch nested := v.(type) {
		case map[string]interface{}:
			out[k] = deepCopyStringMap(nested)
		case []interface{}:
			out[k] = append([]interface{}(nil), nested...)
		default:
			out[k] = v
		}
	}
	return out
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
		vp.v.AutomaticEnv()
		vp.v.SetEnvPrefix("weed")
		vp.v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	}

	return vp
}
