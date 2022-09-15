package meta_cache

import "github.com/seaweedfs/seaweedfs/weed/util"

var (
	_ = util.Configuration(&cacheConfig{})
)

// implementing util.Configuration
type cacheConfig struct {
	dir string
}

func (c cacheConfig) GetString(key string) string {
	return c.dir
}

func (c cacheConfig) GetBool(key string) bool {
	panic("implement me")
}

func (c cacheConfig) GetInt(key string) int {
	panic("implement me")
}

func (c cacheConfig) GetStringSlice(key string) []string {
	panic("implement me")
}

func (c cacheConfig) SetDefault(key string, value interface{}) {
	panic("implement me")
}
