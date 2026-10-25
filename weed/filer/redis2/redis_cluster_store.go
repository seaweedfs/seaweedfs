package redis2

import (
	"crypto/tls"

	"github.com/redis/go-redis/v9"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/filer/redis_conf"
	"github.com/seaweedfs/seaweedfs/weed/filer/redis_tls"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	filer.Stores = append(filer.Stores, &RedisCluster2Store{})
}

type RedisCluster2Store struct {
	UniversalRedis2Store
}

func (store *RedisCluster2Store) GetName() string {
	return "redis_cluster2"
}

func (store *RedisCluster2Store) Initialize(configuration util.Configuration, prefix string) (err error) {

	configuration.SetDefault(prefix+"useReadOnly", false)
	configuration.SetDefault(prefix+"routeByLatency", false)

	tlsConfig, err := redis_tls.Config(configuration, prefix)
	if err != nil {
		return err
	}

	return store.initialize(
		configuration.GetStringSlice(prefix+"addresses"),
		configuration.GetString(prefix+"username"),
		configuration.GetString(prefix+"password"),
		configuration.GetString(prefix+"keyPrefix"),
		configuration.GetBool(prefix+"useReadOnly"),
		configuration.GetBool(prefix+"routeByLatency"),
		configuration.GetStringSlice(prefix+"superLargeDirectories"),
		tlsConfig,
		redis_conf.Read(configuration, prefix),
	)
}

func (store *RedisCluster2Store) initialize(addresses []string, username string, password string, keyPrefix string, readOnly, routeByLatency bool, superLargeDirectories []string, tlsConfig *tls.Config, settings redis_conf.Settings) (err error) {
	options := &redis.ClusterOptions{
		Addrs:          addresses,
		Username:       username,
		Password:       password,
		ReadOnly:       readOnly,
		RouteByLatency: routeByLatency,
		TLSConfig:      tlsConfig,
	}
	settings.ApplyToCluster(options)
	store.Client = redis.NewClusterClient(options)
	store.keyPrefix = keyPrefix
	store.loadSuperLargeDirectories(superLargeDirectories)
	return
}
