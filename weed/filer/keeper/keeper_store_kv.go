package keeper

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/go-zookeeper/zk"

	"github.com/seaweedfs/seaweedfs/weed/filer"
)

func (store *KeeperStore) KvPut(ctx context.Context, key []byte, value []byte) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	zkPath := store.kvPath(key)
	if err := store.createOrSet(zkPath, value); err != nil {
		return fmt.Errorf("kv put: %w", err)
	}
	return nil
}

func (store *KeeperStore) KvGet(ctx context.Context, key []byte) (value []byte, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data, _, err := store.conn.Get(store.kvPath(key))
	if err != nil {
		if err == zk.ErrNoNode {
			return nil, filer.ErrKvNotFound
		}
		return nil, fmt.Errorf("kv get: %w", err)
	}
	return data, nil
}

func (store *KeeperStore) KvDelete(ctx context.Context, key []byte) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	err = store.conn.Delete(store.kvPath(key), -1)
	if err == zk.ErrNoNode {
		return nil
	}
	if err != nil {
		return fmt.Errorf("kv delete: %w", err)
	}
	return nil
}

func (store *KeeperStore) kvPath(key []byte) string {
	return store.kvPrefix + "/" + hex.EncodeToString(key)
}
