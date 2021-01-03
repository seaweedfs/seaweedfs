// +build rocksdb

package rocksdb

import (
	"context"
	"fmt"
	"github.com/tecbot/gorocksdb"

	"github.com/chrislusf/seaweedfs/weed/filer"
)

func (store *RocksDBStore) KvPut(ctx context.Context, key []byte, value []byte) (err error) {

	wo := gorocksdb.NewDefaultWriteOptions()
	err = store.db.Put(wo, key, value)

	if err != nil {
		return fmt.Errorf("kv put: %v", err)
	}

	return nil
}

func (store *RocksDBStore) KvGet(ctx context.Context, key []byte) (value []byte, err error) {

	ro := gorocksdb.NewDefaultReadOptions()
	value, err = store.db.GetBytes(ro, key)

	if value == nil {
		return nil, filer.ErrKvNotFound
	}

	if err != nil {
		return nil, fmt.Errorf("kv get: %v", err)
	}

	return
}

func (store *RocksDBStore) KvDelete(ctx context.Context, key []byte) (err error) {

	wo := gorocksdb.NewDefaultWriteOptions()
	err = store.db.Delete(wo, key)

	if err != nil {
		return fmt.Errorf("kv delete: %v", err)
	}

	return nil
}
