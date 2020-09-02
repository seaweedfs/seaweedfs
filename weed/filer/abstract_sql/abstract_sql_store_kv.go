package abstract_sql

import (
	"context"
	"github.com/chrislusf/seaweedfs/weed/filer"
)

func (store *AbstractSqlStore) KvPut(ctx context.Context, key []byte, value []byte) (err error) {
	return filer.ErrKvNotImplemented
}

func (store *AbstractSqlStore) KvGet(ctx context.Context, key []byte) (value []byte, err error) {
	return nil, filer.ErrKvNotImplemented
}

func (store *AbstractSqlStore) KvDelete(ctx context.Context, key []byte) (err error) {
	return filer.ErrKvNotImplemented
}
