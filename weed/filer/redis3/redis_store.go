package redis3

import (
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/go-redis/redis/v8"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v8"
)

func init() {
	filer.Stores = append(filer.Stores, &Redis3Store{})
}

type Redis3Store struct {
	UniversalRedis3Store
}

func (store *Redis3Store) GetName() string {
	return "redis3"
}

func (store *Redis3Store) Initialize(configuration util.Configuration, prefix string) (err error) {
	return store.initialize(
		configuration.GetString(prefix+"address"),
		configuration.GetString(prefix+"password"),
		configuration.GetInt(prefix+"database"),
	)
}

func (store *Redis3Store) initialize(hostPort string, password string, database int) (err error) {
	store.Client = redis.NewClient(&redis.Options{
		Addr:     hostPort,
		Password: password,
		DB:       database,
	})
	store.redsync = redsync.New(goredis.NewPool(store.Client))
	return
}
