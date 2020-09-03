package cassandra

import (
	"context"
	"github.com/chrislusf/seaweedfs/weed/filer"
)

func (store *CassandraStore) KvPut(ctx context.Context, key []byte, value []byte) (err error) {
	return filer.ErrKvNotImplemented
}

func (store *CassandraStore) KvGet(ctx context.Context, key []byte) (value []byte, err error) {
	return nil, filer.ErrKvNotImplemented
}

func (store *CassandraStore) KvDelete(ctx context.Context, key []byte) (err error) {
	return filer.ErrKvNotImplemented
}
