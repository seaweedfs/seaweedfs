package redis2

import (
	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/go-redis/redis"
)

func init() {
	filer2.Stores = append(filer2.Stores, &RedisCluster2Store{})
}

type RedisCluster2Store struct {
	UniversalRedis2Store
}

func (store *RedisCluster2Store) GetName() string {
	return "redis_cluster2"
}

func (store *RedisCluster2Store) Initialize(configuration util.Configuration, prefix string) (err error) {

	configuration.SetDefault(prefix+"useReadOnly", true)
	configuration.SetDefault(prefix+"routeByLatency", true)

	return store.initialize(
		configuration.GetStringSlice(prefix+"addresses"),
		configuration.GetString(prefix+"password"),
		configuration.GetBool(prefix+"useReadOnly"),
		configuration.GetBool(prefix+"routeByLatency"),
	)
}

func (store *RedisCluster2Store) initialize(addresses []string, password string, readOnly, routeByLatency bool) (err error) {
	store.Client = redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:          addresses,
		Password:       password,
		ReadOnly:       readOnly,
		RouteByLatency: routeByLatency,
	})
	return
}
