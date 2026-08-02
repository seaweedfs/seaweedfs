package leveldb

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/syndtr/goleveldb/leveldb"
	leveldb_util "github.com/syndtr/goleveldb/leveldb/util"
)

func (store *LevelDBStore) KvPut(ctx context.Context, key []byte, value []byte) (err error) {

	err = store.db.Put(key, value, nil)

	if err != nil {
		return fmt.Errorf("kv put: %w", err)
	}

	return nil
}

func (store *LevelDBStore) KvGet(ctx context.Context, key []byte) (value []byte, err error) {

	value, err = store.db.Get(key, nil)

	if err == leveldb.ErrNotFound {
		return nil, filer.ErrKvNotFound
	}

	if err != nil {
		return nil, fmt.Errorf("kv get: %w", err)
	}

	return
}

// VisitKvPrefix calls visit for every key-value pair whose key starts with
// prefix, in key order. Returning an error from visit stops the scan.
func (store *LevelDBStore) VisitKvPrefix(ctx context.Context, prefix []byte, visit func(key []byte, value []byte) error) error {
	iter := store.db.NewIterator(leveldb_util.BytesPrefix(prefix), nil)
	defer iter.Release()
	for iter.Next() {
		key := append([]byte(nil), iter.Key()...)
		value := append([]byte(nil), iter.Value()...)
		if err := visit(key, value); err != nil {
			return err
		}
	}
	return iter.Error()
}

func (store *LevelDBStore) KvDelete(ctx context.Context, key []byte) (err error) {

	err = store.db.Delete(key, nil)

	if err != nil {
		return fmt.Errorf("kv delete: %w", err)
	}

	return nil
}
