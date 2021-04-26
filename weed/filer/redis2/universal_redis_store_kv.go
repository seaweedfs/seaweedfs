package redis2

import (
	"context"
	"fmt"

	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/go-redis/redis/v8"
)

func (store *UniversalRedis2Store) KvPut(ctx context.Context, key []byte, value []byte) (err error) {

	_, err = store.Client.Set(ctx, string(key), value, 0).Result()

	if err != nil {
		return fmt.Errorf("kv put: %v", err)
	}

	return nil
}

func (store *UniversalRedis2Store) KvGet(ctx context.Context, key []byte) (value []byte, err error) {

	data, err := store.Client.Get(ctx, string(key)).Result()

	if err == redis.Nil {
		return nil, filer.ErrKvNotFound
	}

	return []byte(data), err
}

func (store *UniversalRedis2Store) KvDelete(ctx context.Context, key []byte) (err error) {

	_, err = store.Client.Del(ctx, string(key)).Result()

	if err != nil {
		return fmt.Errorf("kv delete: %v", err)
	}

	return nil
}
