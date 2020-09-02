package mongodb

import (
	"context"
	"github.com/chrislusf/seaweedfs/weed/filer"
)

func (store *MongodbStore) KvPut(ctx context.Context, key []byte, value []byte) (err error) {
	return filer.ErrKvNotImplemented
}

func (store *MongodbStore) KvGet(ctx context.Context, key []byte) (value []byte, err error) {
	return nil, filer.ErrKvNotImplemented
}

func (store *MongodbStore) KvDelete(ctx context.Context, key []byte) (err error) {
	return filer.ErrKvNotImplemented
}
