package arangodb

import (
	"context"
	"fmt"

	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
)

func (store *ArangodbStore) KvPut(ctx context.Context, key []byte, value []byte) (err error) {
	dir, name := genDirAndName(key)
	model := &Model{
		Key:       hashString(string(key)),
		Directory: dir,
		Name:      name,
		Meta:      bytesToArray(value),
	}

	exists, err := store.collection.DocumentExists(ctx, model.Key)
	if err != nil {
		return fmt.Errorf("kv put: %v", err)
	}
	if exists {
		_, err = store.collection.UpdateDocument(ctx, model.Key, model)
	} else {
		_, err = store.collection.CreateDocument(ctx, model)
	}
	if err != nil {
		return fmt.Errorf("kv put: %v", err)
	}

	return nil
}

func (store *ArangodbStore) KvGet(ctx context.Context, key []byte) (value []byte, err error) {
	var model Model
	_, err = store.collection.ReadDocument(ctx, hashString(string(key)), &model)
	if err != nil {
		glog.Errorf("kv get: %v", err)
		return nil, filer.ErrKvNotFound
	}
	return arrayToBytes(model.Meta), nil
}

func (store *ArangodbStore) KvDelete(ctx context.Context, key []byte) (err error) {
	_, err = store.collection.RemoveDocument(ctx, hashString(string(key)))
	if err != nil {
		glog.Errorf("kv del: %v", err)
		return filer.ErrKvNotFound
	}
	return nil
}

func genDirAndName(key []byte) (dir string, name string) {
	for len(key) < 8 {
		key = append(key, 0)
	}
	dir = string(key[:8])
	name = string(key[8:])
	return
}
