package redis

import (
	"github.com/redis/go-redis/v9"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/filer/redis_conf"
	"github.com/seaweedfs/seaweedfs/weed/util"
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
		redis_conf.Read(configuration, prefix),
	)
}

func (store *RedisClusterStore) initialize(addresses []string, password string, readOnly, routeByLatency bool, settings redis_conf.Settings) (err error) {
	options := &redis.ClusterOptions{
		Addrs:          addresses,
		Password:       password,
		ReadOnly:       readOnly,
		RouteByLatency: routeByLatency,
	}
	settings.ApplyToCluster(options)
	store.Client = redis.NewClusterClient(options)
	return
}
