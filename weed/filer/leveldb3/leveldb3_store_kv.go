package leveldb

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/syndtr/goleveldb/leveldb"
)

func (store *LevelDB3Store) KvPut(ctx context.Context, key []byte, value []byte) (err error) {

	err = store.dbs[DEFAULT].Put(key, value, nil)

	if err != nil {
		return fmt.Errorf("kv put: %v", err)
	}

	return nil
}

func (store *LevelDB3Store) KvGet(ctx context.Context, key []byte) (value []byte, err error) {

	value, err = store.dbs[DEFAULT].Get(key, nil)

	if err == leveldb.ErrNotFound {
		return nil, filer.ErrKvNotFound
	}

	if err != nil {
		return nil, fmt.Errorf("kv get: %v", err)
	}

	return
}

func (store *LevelDB3Store) KvDelete(ctx context.Context, key []byte) (err error) {

	err = store.dbs[DEFAULT].Delete(key, nil)

	if err != nil {
		return fmt.Errorf("kv delete: %v", err)
	}

	return nil
}
