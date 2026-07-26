package redis_conf

import (
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

type fakeConfiguration map[string]interface{}

func (c fakeConfiguration) GetString(key string) string {
	value, _ := c[key].(string)
	return value
}

func (c fakeConfiguration) GetBool(key string) bool {
	value, _ := c[key].(bool)
	return value
}

func (c fakeConfiguration) GetInt(key string) int {
	value, _ := c[key].(int)
	return value
}

func (c fakeConfiguration) GetStringSlice(key string) []string {
	value, _ := c[key].([]string)
	return value
}

func (c fakeConfiguration) SetDefault(key string, value interface{}) {
	if _, found := c[key]; !found {
		c[key] = value
	}
}

func TestUnsetKeepsGoRedisDefaults(t *testing.T) {
	settings := Read(fakeConfiguration{}, "redis2_sentinel.")
	if settings != (Settings{}) {
		t.Fatalf("expected zero settings, got %+v", settings)
	}

	options := &redis.FailoverOptions{MasterName: "master"}
	settings.ApplyToFailover(options)

	client := redis.NewFailoverClient(options)
	defer client.Close()

	if got := client.Options().ReadTimeout; got != 3*time.Second {
		t.Fatalf("read timeout %v, want the go-redis default of 3s", got)
	}
	if got := client.Options().MaxRetries; got != 3 {
		t.Fatalf("max retries %d, want the go-redis default of 3", got)
	}
}

func TestRead(t *testing.T) {
	settings := Read(fakeConfiguration{
		"redis2.max_retries":                   5,
		"redis2.min_retry_backoff_millisecond": 10,
		"redis2.max_retry_backoff_millisecond": 500,
		"redis2.dial_timeout_millisecond":      2000,
		"redis2.read_timeout_millisecond":      1500,
		"redis2.write_timeout_millisecond":     1500,
		"redis2.pool_size":                     64,
		"redis2.pool_timeout_millisecond":      2500,
		"redis2.min_idle_conns":                4,
		"redis2.max_idle_conns":                16,
		"redis2.conn_max_idle_time_seconds":    300,
		"redis2.conn_max_lifetime_seconds":     900,
	}, "redis2.")

	want := Settings{
		MaxRetries:      5,
		MinRetryBackoff: 10 * time.Millisecond,
		MaxRetryBackoff: 500 * time.Millisecond,
		DialTimeout:     2 * time.Second,
		ReadTimeout:     1500 * time.Millisecond,
		WriteTimeout:    1500 * time.Millisecond,
		PoolSize:        64,
		PoolTimeout:     2500 * time.Millisecond,
		MinIdleConns:    4,
		MaxIdleConns:    16,
		ConnMaxIdleTime: 5 * time.Minute,
		ConnMaxLifetime: 15 * time.Minute,
	}
	if settings != want {
		t.Fatalf("got %+v, want %+v", settings, want)
	}

	options := &redis.Options{Addr: "localhost:6379"}
	settings.ApplyTo(options)
	if options.ReadTimeout != want.ReadTimeout || options.PoolSize != want.PoolSize || options.ConnMaxLifetime != want.ConnMaxLifetime {
		t.Fatalf("options not applied: %+v", options)
	}

	clusterOptions := &redis.ClusterOptions{Addrs: []string{"localhost:6379"}}
	settings.ApplyToCluster(clusterOptions)
	if clusterOptions.ReadTimeout != want.ReadTimeout || clusterOptions.PoolSize != want.PoolSize {
		t.Fatalf("cluster options not applied: %+v", clusterOptions)
	}
}

func TestNegativeDurationIsIgnored(t *testing.T) {
	settings := Read(fakeConfiguration{
		"redis2.read_timeout_millisecond":  -1,
		"redis2.conn_max_lifetime_seconds": -1,
		"redis2.max_retries":               -1,
	}, "redis2.")

	if settings.ReadTimeout != 0 || settings.ConnMaxLifetime != 0 {
		t.Fatalf("negative durations should keep the go-redis default, got %+v", settings)
	}
	// -1 is how go-redis spells "no retry", so it has to survive
	if settings.MaxRetries != -1 {
		t.Fatalf("max retries %d, want -1", settings.MaxRetries)
	}
}
