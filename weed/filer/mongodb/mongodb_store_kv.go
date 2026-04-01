package mongodb

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func (store *MongodbStore) KvPut(ctx context.Context, key []byte, value []byte) (err error) {

	dir, name := genDirAndName(key)

	c := store.connect.Database(store.database).Collection(store.collectionName)

	opts := options.Update().SetUpsert(true)
	filter := bson.D{
		{Key: "directory", Value: dir},
		{Key: "name", Value: name},
	}
	update := bson.D{
		{Key: "$set", Value: bson.D{
			{Key: "meta", Value: value},
		}},
	}

	_, err = c.UpdateOne(ctx, filter, update, opts)

	if err != nil {
		return fmt.Errorf("kv put: %w", err)
	}

	return nil
}

func (store *MongodbStore) KvGet(ctx context.Context, key []byte) (value []byte, err error) {
	dir, name := genDirAndName(key)

	var data Model

	var where = bson.M{"directory": dir, "name": name}
	err = store.connect.Database(store.database).Collection(store.collectionName).FindOne(ctx, where).Decode(&data)
	if err != mongo.ErrNoDocuments && err != nil {
		glog.ErrorfCtx(ctx, "kv get: %v", err)
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
		return fmt.Errorf("kv delete: %w", err)
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
