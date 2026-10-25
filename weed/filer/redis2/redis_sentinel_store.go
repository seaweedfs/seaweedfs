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
	filer.Stores = append(filer.Stores, &Redis2SentinelStore{})
}

type Redis2SentinelStore struct {
	UniversalRedis2Store
}

func (store *Redis2SentinelStore) GetName() string {
	return "redis2_sentinel"
}

func (store *Redis2SentinelStore) Initialize(configuration util.Configuration, prefix string) (err error) {
	tlsConfig, err := redis_tls.Config(configuration, prefix)
	if err != nil {
		return err
	}
	return store.initialize(
		configuration.GetStringSlice(prefix+"addresses"),
		configuration.GetString(prefix+"masterName"),
		configuration.GetString(prefix+"username"),
		configuration.GetString(prefix+"password"),
		configuration.GetString(prefix+"sentinel_username"),
		configuration.GetString(prefix+"sentinel_password"),
		configuration.GetInt(prefix+"database"),
		configuration.GetString(prefix+"keyPrefix"),
		tlsConfig,
		redis_conf.Read(configuration, prefix),
	)
}

func (store *Redis2SentinelStore) initialize(addresses []string, masterName string, username string, password string, sentinelUsername string, sentinelPassword string, database int, keyPrefix string, tlsConfig *tls.Config, settings redis_conf.Settings) (err error) {
	options := &redis.FailoverOptions{
		MasterName:       masterName,
		SentinelAddrs:    addresses,
		Username:         username,
		Password:         password,
		SentinelUsername: sentinelUsername,
		SentinelPassword: sentinelPassword,
		DB:               database,
		TLSConfig:        tlsConfig,
	}
	settings.ApplyToFailover(options)
	store.Client = redis.NewFailoverClient(options)
	store.keyPrefix = keyPrefix
	return
}
