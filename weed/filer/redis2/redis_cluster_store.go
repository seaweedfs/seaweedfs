package redis2

import (
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/go-redis/redis/v8"
)

func init() {
	filer.Stores = append(filer.Stores, &RedisCluster2Store{})
}

type RedisCluster2Store struct {
	UniversalRedis2Store
}

func (store *RedisCluster2Store) GetName() string {
	return "redis_cluster2"
}

func (store *RedisCluster2Store) Initialize(configuration util.Configuration, prefix string) (err error) {

	configuration.SetDefault(prefix+"useReadOnly", false)
	configuration.SetDefault(prefix+"routeByLatency", false)

	return store.initialize(
		configuration.GetStringSlice(prefix+"addresses"),
		configuration.GetString(prefix+"password"),
		configuration.GetBool(prefix+"useReadOnly"),
		configuration.GetBool(prefix+"routeByLatency"),
		configuration.GetStringSlice(prefix+"superLargeDirectories"),
	)
}

func (store *RedisCluster2Store) initialize(addresses []string, password string, readOnly, routeByLatency bool, superLargeDirectories []string) (err error) {
	store.Client = redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:          addresses,
		Password:       password,
		ReadOnly:       readOnly,
		RouteByLatency: routeByLatency,
	})
	store.loadSuperLargeDirectories(superLargeDirectories)
	return
}
