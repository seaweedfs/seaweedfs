//go:build tikv
// +build tikv

package tikv

import (
	"context"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/tikv/client-go/v2/txnkv"
)

func (store *TikvStore) KvPut(ctx context.Context, key []byte, value []byte) error {
	tw, err := store.getTxn(ctx)
	if err != nil {
		return err
	}
	return tw.RunInTxn(func(txn *txnkv.KVTxn) error {
		return txn.Set(key, value)
	})
}

func (store *TikvStore) KvGet(ctx context.Context, key []byte) ([]byte, error) {
	tw, err := store.getTxn(ctx)
	if err != nil {
		return nil, err
	}
	var data []byte = nil
	err = tw.RunInTxn(func(txn *txnkv.KVTxn) error {
		val, err := txn.Get(context.TODO(), key)
		if err == nil {
			data = val
		}
		return err
	})
	if isNotExists(err) {
		return data, filer.ErrKvNotFound
	}
	return data, err
}

func (store *TikvStore) KvDelete(ctx context.Context, key []byte) error {
	tw, err := store.getTxn(ctx)
	if err != nil {
		return err
	}
	return tw.RunInTxn(func(txn *txnkv.KVTxn) error {
		return txn.Delete(key)
	})
}
