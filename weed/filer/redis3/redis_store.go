package redis3

import (
	"crypto/tls"

	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/filer/redis_conf"
	"github.com/seaweedfs/seaweedfs/weed/filer/redis_tls"
	"github.com/seaweedfs/seaweedfs/weed/util"
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
	tlsConfig, err := redis_tls.Config(configuration, prefix)
	if err != nil {
		return err
	}
	return store.initialize(
		configuration.GetString(prefix+"address"),
		configuration.GetString(prefix+"password"),
		configuration.GetInt(prefix+"database"),
		tlsConfig,
		redis_conf.Read(configuration, prefix),
	)
}

func (store *Redis3Store) initialize(hostPort string, password string, database int, tlsConfig *tls.Config, settings redis_conf.Settings) (err error) {
	options := &redis.Options{
		Addr:      hostPort,
		Password:  password,
		DB:        database,
		TLSConfig: tlsConfig,
	}
	settings.ApplyTo(options)
	store.Client = redis.NewClient(options)
	store.redsync = redsync.New(goredis.NewPool(store.Client))
	return
}
