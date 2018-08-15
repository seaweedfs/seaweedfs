package redis

import (
	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/go-redis/redis"
)

func init() {
	filer2.Stores = append(filer2.Stores, &RedisStore{})
}

type RedisStore struct {
	UniversalRedisStore
}

func (store *RedisStore) GetName() string {
	return "redis"
}

func (store *RedisStore) Initialize(configuration filer2.Configuration) (err error) {
	return store.initialize(
		configuration.GetString("address"),
		configuration.GetString("password"),
		configuration.GetInt("database"),
	)
}

func (store *RedisStore) initialize(hostPort string, password string, database int) (err error) {
	store.Client = redis.NewClient(&redis.Options{
		Addr:     hostPort,
		Password: password,
		DB:       database,
	})
	return
}
