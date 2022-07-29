package hbase

import (
	"context"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/tsuna/gohbase/hrpc"
	"time"
)

const (
	COLUMN_NAME = "a"
)

func (store *HbaseStore) KvPut(ctx context.Context, key []byte, value []byte) (err error) {
	return store.doPut(ctx, store.cfKv, key, value, 0)
}

func (store *HbaseStore) KvGet(ctx context.Context, key []byte) (value []byte, err error) {
	return store.doGet(ctx, store.cfKv, key)
}

func (store *HbaseStore) KvDelete(ctx context.Context, key []byte) (err error) {
	return store.doDelete(ctx, store.cfKv, key)
}

func (store *HbaseStore) doPut(ctx context.Context, cf string, key, value []byte, ttlSecond int32) (err error) {
	if ttlSecond > 0 {
		return store.doPutWithOptions(ctx, cf, key, value, hrpc.Durability(hrpc.AsyncWal), hrpc.TTL(time.Duration(ttlSecond)*time.Second))
	}
	return store.doPutWithOptions(ctx, cf, key, value, hrpc.Durability(hrpc.AsyncWal))
}

func (store *HbaseStore) doPutWithOptions(ctx context.Context, cf string, key, value []byte, options ...func(hrpc.Call) error) (err error) {
	values := map[string]map[string][]byte{cf: map[string][]byte{}}
	values[cf][COLUMN_NAME] = value
	putRequest, err := hrpc.NewPut(ctx, store.table, key, values, options...)
	if err != nil {
		return err
	}
	_, err = store.Client.Put(putRequest)
	if err != nil {
		return err
	}
	return nil
}

func (store *HbaseStore) doGet(ctx context.Context, cf string, key []byte) (value []byte, err error) {
	family := map[string][]string{cf: {COLUMN_NAME}}
	getRequest, err := hrpc.NewGet(context.Background(), store.table, key, hrpc.Families(family))
	if err != nil {
		return nil, err
	}
	getResp, err := store.Client.Get(getRequest)
	if err != nil {
		return nil, err
	}
	if len(getResp.Cells) == 0 {
		return nil, filer.ErrKvNotFound
	}

	return getResp.Cells[0].Value, nil
}

func (store *HbaseStore) doDelete(ctx context.Context, cf string, key []byte) (err error) {
	values := map[string]map[string][]byte{cf: map[string][]byte{}}
	values[cf][COLUMN_NAME] = nil
	deleteRequest, err := hrpc.NewDel(ctx, store.table, key, values, hrpc.Durability(hrpc.AsyncWal))
	if err != nil {
		return err
	}
	_, err = store.Client.Delete(deleteRequest)
	if err != nil {
		return err
	}
	return nil
}
