package redis3

import (
	"time"

	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/go-redis/redis/v8"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v8"
)

func init() {
	filer.Stores = append(filer.Stores, &Redis3SentinelStore{})
}

type Redis3SentinelStore struct {
	UniversalRedis3Store
}

func (store *Redis3SentinelStore) GetName() string {
	return "redis3_sentinel"
}

func (store *Redis3SentinelStore) Initialize(configuration util.Configuration, prefix string) (err error) {
	return store.initialize(
		configuration.GetStringSlice(prefix+"addresses"),
		configuration.GetString(prefix+"masterName"),
		configuration.GetString(prefix+"username"),
		configuration.GetString(prefix+"password"),
		configuration.GetInt(prefix+"database"),
	)
}

func (store *Redis3SentinelStore) initialize(addresses []string, masterName string, username string, password string, database int) (err error) {
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
	store.redsync = redsync.New(goredis.NewPool(store.Client))
	return
}
