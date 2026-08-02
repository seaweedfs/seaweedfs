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
	filer.Stores = append(filer.Stores, &RedisCluster3Store{})
}

type RedisCluster3Store struct {
	UniversalRedis3Store
}

func (store *RedisCluster3Store) GetName() string {
	return "redis_cluster3"
}

func (store *RedisCluster3Store) Initialize(configuration util.Configuration, prefix string) (err error) {

	configuration.SetDefault(prefix+"useReadOnly", false)
	configuration.SetDefault(prefix+"routeByLatency", false)

	tlsConfig, err := redis_tls.Config(configuration, prefix)
	if err != nil {
		return err
	}

	return store.initialize(
		configuration.GetStringSlice(prefix+"addresses"),
		configuration.GetString(prefix+"password"),
		configuration.GetBool(prefix+"useReadOnly"),
		configuration.GetBool(prefix+"routeByLatency"),
		tlsConfig,
		redis_conf.Read(configuration, prefix),
	)
}

func (store *RedisCluster3Store) initialize(addresses []string, password string, readOnly, routeByLatency bool, tlsConfig *tls.Config, settings redis_conf.Settings) (err error) {
	options := &redis.ClusterOptions{
		Addrs:          addresses,
		Password:       password,
		ReadOnly:       readOnly,
		RouteByLatency: routeByLatency,
		TLSConfig:      tlsConfig,
	}
	settings.ApplyToCluster(options)
	store.Client = redis.NewClusterClient(options)
	store.redsync = redsync.New(goredis.NewPool(store.Client))
	return
}
