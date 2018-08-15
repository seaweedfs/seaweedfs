package redis

import (
	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/go-redis/redis"
)

func init() {
	filer2.Stores = append(filer2.Stores, &RedisClusterStore{})
}

type RedisClusterStore struct {
	UniversalRedisStore
}

func (store *RedisClusterStore) GetName() string {
	return "redis_cluster"
}

func (store *RedisClusterStore) Initialize(configuration filer2.Configuration) (err error) {
	return store.initialize(
		configuration.GetStringSlice("addresses"),
	)
}

func (store *RedisClusterStore) initialize(addresses []string) (err error) {
	store.Client = redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: addresses,
	})
	return
}
