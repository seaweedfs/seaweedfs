package redis_lua

import (
	"github.com/redis/go-redis/v9"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	filer.Stores = append(filer.Stores, &RedisLuaStore{})
}

type RedisLuaStore struct {
	UniversalRedisLuaStore
}

func (store *RedisLuaStore) GetName() string {
	return "redis_lua"
}

func (store *RedisLuaStore) Initialize(configuration util.Configuration, prefix string) (err error) {
	return store.initialize(
		configuration.GetString(prefix+"address"),
		configuration.GetString(prefix+"password"),
		configuration.GetInt(prefix+"database"),
		configuration.GetStringSlice(prefix+"superLargeDirectories"),
	)
}

func (store *RedisLuaStore) initialize(hostPort string, password string, database int, superLargeDirectories []string) (err error) {
	store.Client = redis.NewClient(&redis.Options{
		Addr:     hostPort,
		Password: password,
		DB:       database,
	})
	store.loadSuperLargeDirectories(superLargeDirectories)
	return
}
