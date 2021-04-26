package mongodb

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func (store *MongodbStore) KvPut(ctx context.Context, key []byte, value []byte) (err error) {

	dir, name := genDirAndName(key)

	c := store.connect.Database(store.database).Collection(store.collectionName)

	_, err = c.InsertOne(ctx, Model{
		Directory: dir,
		Name:      name,
		Meta:      value,
	})

	if err != nil {
		return fmt.Errorf("kv put: %v", err)
	}

	return nil
}

func (store *MongodbStore) KvGet(ctx context.Context, key []byte) (value []byte, err error) {
	dir, name := genDirAndName(key)

	var data Model

	var where = bson.M{"directory": dir, "name": name}
	err = store.connect.Database(store.database).Collection(store.collectionName).FindOne(ctx, where).Decode(&data)
	if err != mongo.ErrNoDocuments && err != nil {
		glog.Errorf("kv get: %v", err)
		return nil, filer.ErrKvNotFound
	}

	if len(data.Meta) == 0 {
		return nil, filer.ErrKvNotFound
	}

	return data.Meta, nil
}

func (store *MongodbStore) KvDelete(ctx context.Context, key []byte) (err error) {

	dir, name := genDirAndName(key)

	where := bson.M{"directory": dir, "name": name}
	_, err = store.connect.Database(store.database).Collection(store.collectionName).DeleteOne(ctx, where)
	if err != nil {
		return fmt.Errorf("kv delete: %v", err)
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
