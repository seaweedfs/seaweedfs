package redis

import (
	"github.com/redis/go-redis/v9"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/filer/redis_conf"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	filer.Stores = append(filer.Stores, &RedisStore{})
}

type RedisStore struct {
	UniversalRedisStore
}

func (store *RedisStore) GetName() string {
	return "redis"
}

func (store *RedisStore) Initialize(configuration util.Configuration, prefix string) (err error) {
	return store.initialize(
		configuration.GetString(prefix+"address"),
		configuration.GetString(prefix+"password"),
		configuration.GetInt(prefix+"database"),
		redis_conf.Read(configuration, prefix),
	)
}

func (store *RedisStore) initialize(hostPort string, password string, database int, settings redis_conf.Settings) (err error) {
	options := &redis.Options{
		Addr:     hostPort,
		Password: password,
		DB:       database,
	}
	settings.ApplyTo(options)
	store.Client = redis.NewClient(options)
	return
}
