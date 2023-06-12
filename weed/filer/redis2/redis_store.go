package redis2

import (
	"github.com/redis/go-redis/v9"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	filer.Stores = append(filer.Stores, &Redis2Store{})
}

type Redis2Store struct {
	UniversalRedis2Store
}

func (store *Redis2Store) GetName() string {
	return "redis2"
}

func (store *Redis2Store) Initialize(configuration util.Configuration, prefix string) (err error) {
	return store.initialize(
		configuration.GetString(prefix+"address"),
		configuration.GetString(prefix+"password"),
		configuration.GetInt(prefix+"database"),
		configuration.GetStringSlice(prefix+"superLargeDirectories"),
	)
}

func (store *Redis2Store) initialize(hostPort string, password string, database int, superLargeDirectories []string) (err error) {
	store.Client = redis.NewClient(&redis.Options{
		Addr:     hostPort,
		Password: password,
		DB:       database,
	})
	store.loadSuperLargeDirectories(superLargeDirectories)
	return
}
