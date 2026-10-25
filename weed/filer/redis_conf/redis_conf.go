// Package redis_conf reads the dial, timeout and connection pool settings shared by the redis
// filer stores.
package redis_conf

import (
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// Settings carries the go-redis tuning a filer store section may override. A zero field keeps the
// go-redis default, so an untouched configuration behaves the way it did before these keys existed.
type Settings struct {
	MaxRetries      int
	MinRetryBackoff time.Duration
	MaxRetryBackoff time.Duration
	DialTimeout     time.Duration
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	PoolSize        int
	PoolTimeout     time.Duration
	MinIdleConns    int
	MaxIdleConns    int
	ConnMaxIdleTime time.Duration
	ConnMaxLifetime time.Duration
}

// Read parses the tuning keys of a redis filer store section.
func Read(configuration util.Configuration, prefix string) Settings {
	return Settings{
		// go-redis reads 0 as three retries and -1 as no retry, so hand the value over untouched
		MaxRetries:      configuration.GetInt(prefix + "max_retries"),
		MinRetryBackoff: milliseconds(configuration, prefix+"min_retry_backoff_millisecond"),
		MaxRetryBackoff: milliseconds(configuration, prefix+"max_retry_backoff_millisecond"),
		DialTimeout:     milliseconds(configuration, prefix+"dial_timeout_millisecond"),
		ReadTimeout:     milliseconds(configuration, prefix+"read_timeout_millisecond"),
		WriteTimeout:    milliseconds(configuration, prefix+"write_timeout_millisecond"),
		PoolSize:        configuration.GetInt(prefix + "pool_size"),
		PoolTimeout:     milliseconds(configuration, prefix+"pool_timeout_millisecond"),
		MinIdleConns:    configuration.GetInt(prefix + "min_idle_conns"),
		MaxIdleConns:    configuration.GetInt(prefix + "max_idle_conns"),
		ConnMaxIdleTime: seconds(configuration, prefix+"conn_max_idle_time_seconds"),
		ConnMaxLifetime: seconds(configuration, prefix+"conn_max_lifetime_seconds"),
	}
}

func (settings Settings) ApplyTo(options *redis.Options) {
	options.MaxRetries = settings.MaxRetries
	options.MinRetryBackoff = settings.MinRetryBackoff
	options.MaxRetryBackoff = settings.MaxRetryBackoff
	options.DialTimeout = settings.DialTimeout
	options.ReadTimeout = settings.ReadTimeout
	options.WriteTimeout = settings.WriteTimeout
	options.PoolSize = settings.PoolSize
	options.PoolTimeout = settings.PoolTimeout
	options.MinIdleConns = settings.MinIdleConns
	options.MaxIdleConns = settings.MaxIdleConns
	options.ConnMaxIdleTime = settings.ConnMaxIdleTime
	options.ConnMaxLifetime = settings.ConnMaxLifetime
}

func (settings Settings) ApplyToCluster(options *redis.ClusterOptions) {
	options.MaxRetries = settings.MaxRetries
	options.MinRetryBackoff = settings.MinRetryBackoff
	options.MaxRetryBackoff = settings.MaxRetryBackoff
	options.DialTimeout = settings.DialTimeout
	options.ReadTimeout = settings.ReadTimeout
	options.WriteTimeout = settings.WriteTimeout
	options.PoolSize = settings.PoolSize
	options.PoolTimeout = settings.PoolTimeout
	options.MinIdleConns = settings.MinIdleConns
	options.MaxIdleConns = settings.MaxIdleConns
	options.ConnMaxIdleTime = settings.ConnMaxIdleTime
	options.ConnMaxLifetime = settings.ConnMaxLifetime
}

func (settings Settings) ApplyToFailover(options *redis.FailoverOptions) {
	options.MaxRetries = settings.MaxRetries
	options.MinRetryBackoff = settings.MinRetryBackoff
	options.MaxRetryBackoff = settings.MaxRetryBackoff
	options.DialTimeout = settings.DialTimeout
	options.ReadTimeout = settings.ReadTimeout
	options.WriteTimeout = settings.WriteTimeout
	options.PoolSize = settings.PoolSize
	options.PoolTimeout = settings.PoolTimeout
	options.MinIdleConns = settings.MinIdleConns
	options.MaxIdleConns = settings.MaxIdleConns
	options.ConnMaxIdleTime = settings.ConnMaxIdleTime
	options.ConnMaxLifetime = settings.ConnMaxLifetime
}

// milliseconds and seconds keep the go-redis default for anything non-positive. go-redis spells
// "no timeout" as a negative nanosecond count, which these keys deliberately cannot reach: a filer
// that never times out a read is what leaves a failover hanging.
func milliseconds(configuration util.Configuration, key string) time.Duration {
	if value := configuration.GetInt(key); value > 0 {
		return time.Duration(value) * time.Millisecond
	}
	return 0
}

func seconds(configuration util.Configuration, key string) time.Duration {
	if value := configuration.GetInt(key); value > 0 {
		return time.Duration(value) * time.Second
	}
	return 0
}
