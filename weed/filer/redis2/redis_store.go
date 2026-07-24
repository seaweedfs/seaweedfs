package redis2

import (
	"crypto/tls"

	"github.com/redis/go-redis/v9"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/filer/redis_tls"
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
	tlsConfig, err := redis_tls.Config(configuration, prefix)
	if err != nil {
		return err
	}
	return store.initialize(
		configuration.GetString(prefix+"address"),
		configuration.GetString(prefix+"username"),
		configuration.GetString(prefix+"password"),
		configuration.GetInt(prefix+"database"),
		configuration.GetString(prefix+"keyPrefix"),
		configuration.GetStringSlice(prefix+"superLargeDirectories"),
		tlsConfig,
	)
}

func (store *Redis2Store) initialize(hostPort string, username string, password string, database int, keyPrefix string, superLargeDirectories []string, tlsConfig *tls.Config) (err error) {
	store.Client = redis.NewClient(&redis.Options{
		Addr:      hostPort,
		Username:  username,
		Password:  password,
		DB:        database,
		TLSConfig: tlsConfig,
	})
	store.keyPrefix = keyPrefix
	store.loadSuperLargeDirectories(superLargeDirectories)
	return
}
