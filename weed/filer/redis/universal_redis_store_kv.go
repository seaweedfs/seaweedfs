package redis

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
	"github.com/seaweedfs/seaweedfs/weed/filer"
)

func (store *UniversalRedisStore) KvPut(ctx context.Context, key []byte, value []byte) (err error) {

	_, err = store.Client.Set(ctx, string(key), value, 0).Result()

	if err != nil {
		return fmt.Errorf("kv put: %v", err)
	}

	return nil
}

func (store *UniversalRedisStore) KvGet(ctx context.Context, key []byte) (value []byte, err error) {

	data, err := store.Client.Get(ctx, string(key)).Result()

	if err == redis.Nil {
		return nil, filer.ErrKvNotFound
	}

	return []byte(data), err
}

func (store *UniversalRedisStore) KvDelete(ctx context.Context, key []byte) (err error) {

	_, err = store.Client.Del(ctx, string(key)).Result()

	if err != nil {
		return fmt.Errorf("kv delete: %v", err)
	}

	return nil
}
