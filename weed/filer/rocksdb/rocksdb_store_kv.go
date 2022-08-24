//go:build rocksdb
// +build rocksdb

package rocksdb

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/filer"
)

func (store *RocksDBStore) KvPut(ctx context.Context, key []byte, value []byte) (err error) {

	err = store.db.Put(store.wo, key, value)

	if err != nil {
		return fmt.Errorf("kv put: %v", err)
	}

	return nil
}

func (store *RocksDBStore) KvGet(ctx context.Context, key []byte) (value []byte, err error) {

	value, err = store.db.GetBytes(store.ro, key)

	if value == nil {
		return nil, filer.ErrKvNotFound
	}

	if err != nil {
		return nil, fmt.Errorf("kv get: %v", err)
	}

	return
}

func (store *RocksDBStore) KvDelete(ctx context.Context, key []byte) (err error) {

	err = store.db.Delete(store.wo, key)

	if err != nil {
		return fmt.Errorf("kv delete: %v", err)
	}

	return nil
}
