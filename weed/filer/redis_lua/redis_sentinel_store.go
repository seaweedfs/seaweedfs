package redis_lua

import (
	"github.com/redis/go-redis/v9"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"time"
)

func init() {
	filer.Stores = append(filer.Stores, &RedisLuaSentinelStore{})
}

type RedisLuaSentinelStore struct {
	UniversalRedisLuaStore
}

func (store *RedisLuaSentinelStore) GetName() string {
	return "redis_lua_sentinel"
}

func (store *RedisLuaSentinelStore) Initialize(configuration util.Configuration, prefix string) (err error) {
	return store.initialize(
		configuration.GetStringSlice(prefix+"addresses"),
		configuration.GetString(prefix+"masterName"),
		configuration.GetString(prefix+"username"),
		configuration.GetString(prefix+"password"),
		configuration.GetInt(prefix+"database"),
	)
}

func (store *RedisLuaSentinelStore) initialize(addresses []string, masterName string, username string, password string, database int) (err error) {
	store.Client = redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:      masterName,
		SentinelAddrs:   addresses,
		Username:        username,
		Password:        password,
		DB:              database,
		MinRetryBackoff: time.Millisecond * 100,
		MaxRetryBackoff: time.Minute * 1,
		ReadTimeout:     time.Second * 30,
		WriteTimeout:    time.Second * 5,
	})
	return
}
