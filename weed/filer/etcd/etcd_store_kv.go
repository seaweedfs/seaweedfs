package etcd

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/filer"
)

// KvPut has no request deadline, like InsertEntry: the filer stores a hard-linked
// file's chunk list here, so a timed-out Put that was saved would leave it
// pointing at chunks the upload handler deletes.
func (store *EtcdStore) KvPut(ctx context.Context, key []byte, value []byte) (err error) {

	_, err = store.client.Put(ctx, store.etcdKeyPrefix+string(key), string(value))

	if err != nil {
		return fmt.Errorf("kv put: %w", err)
	}

	return nil
}

func (store *EtcdStore) KvGet(ctx context.Context, key []byte) (value []byte, err error) {
	opCtx, cancel := store.withTimeout(ctx)
	defer cancel()

	resp, err := store.client.Get(opCtx, store.etcdKeyPrefix+string(key), atScanRevision(ctx)...)

	if err != nil {
		return nil, fmt.Errorf("kv get: %w", err)
	}

	if len(resp.Kvs) == 0 {
		return nil, filer.ErrKvNotFound
	}

	return resp.Kvs[0].Value, nil
}

func (store *EtcdStore) KvDelete(ctx context.Context, key []byte) (err error) {
	opCtx, cancel := store.withTimeout(ctx)
	defer cancel()

	_, err = store.client.Delete(opCtx, store.etcdKeyPrefix+string(key))

	if err != nil {
		return fmt.Errorf("kv delete: %w", err)
	}

	return nil
}
