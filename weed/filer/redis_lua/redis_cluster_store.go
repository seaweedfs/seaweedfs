package redis_lua

import (
	"github.com/redis/go-redis/v9"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	filer.Stores = append(filer.Stores, &RedisLuaClusterStore{})
}

type RedisLuaClusterStore struct {
	UniversalRedisLuaStore
}

func (store *RedisLuaClusterStore) GetName() string {
	return "redis_lua_cluster"
}

func (store *RedisLuaClusterStore) Initialize(configuration util.Configuration, prefix string) (err error) {

	configuration.SetDefault(prefix+"useReadOnly", false)
	configuration.SetDefault(prefix+"routeByLatency", false)

	return store.initialize(
		configuration.GetStringSlice(prefix+"addresses"),
		configuration.GetString(prefix+"username"),
		configuration.GetString(prefix+"password"),
		configuration.GetString(prefix+"keyPrefix"),
		configuration.GetBool(prefix+"useReadOnly"),
		configuration.GetBool(prefix+"routeByLatency"),
		configuration.GetStringSlice(prefix+"superLargeDirectories"),
	)
}

func (store *RedisLuaClusterStore) initialize(addresses []string, username string, password string, keyPrefix string, readOnly, routeByLatency bool, superLargeDirectories []string) (err error) {
	store.Client = redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:          addresses,
		Username:       username,
		Password:       password,
		ReadOnly:       readOnly,
		RouteByLatency: routeByLatency,
	})
	store.keyPrefix = keyPrefix
	store.loadSuperLargeDirectories(superLargeDirectories)
	return
}
