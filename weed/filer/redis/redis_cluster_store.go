package redis

import (
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/go-redis/redis/v8"
)

func init() {
	filer.Stores = append(filer.Stores, &RedisClusterStore{})
}

type RedisClusterStore struct {
	UniversalRedisStore
}

func (store *RedisClusterStore) GetName() string {
	return "redis_cluster"
}

func (store *RedisClusterStore) Initialize(configuration util.Configuration, prefix string) (err error) {

	configuration.SetDefault(prefix+"useReadOnly", false)
	configuration.SetDefault(prefix+"routeByLatency", false)

	return store.initialize(
		configuration.GetStringSlice(prefix+"addresses"),
		configuration.GetString(prefix+"password"),
		configuration.GetBool(prefix+"useReadOnly"),
		configuration.GetBool(prefix+"routeByLatency"),
	)
}

func (store *RedisClusterStore) initialize(addresses []string, password string, readOnly, routeByLatency bool) (err error) {
	store.Client = redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:          addresses,
		Password:       password,
		ReadOnly:       readOnly,
		RouteByLatency: routeByLatency,
	})
	return
}
