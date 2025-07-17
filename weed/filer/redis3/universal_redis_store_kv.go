package redis3

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
	"github.com/seaweedfs/seaweedfs/weed/filer"
)

func (store *UniversalRedis3Store) KvPut(ctx context.Context, key []byte, value []byte) (err error) {

	_, err = store.Client.Set(ctx, string(key), value, 0).Result()

	if err != nil {
		return fmt.Errorf("kv put: %w", err)
	}

	return nil
}

func (store *UniversalRedis3Store) KvGet(ctx context.Context, key []byte) (value []byte, err error) {

	data, err := store.Client.Get(ctx, string(key)).Result()

	if err == redis.Nil {
		return nil, filer.ErrKvNotFound
	}

	return []byte(data), err
}

func (store *UniversalRedis3Store) KvDelete(ctx context.Context, key []byte) (err error) {

	_, err = store.Client.Del(ctx, string(key)).Result()

	if err != nil {
		return fmt.Errorf("kv delete: %w", err)
	}

	return nil
}
