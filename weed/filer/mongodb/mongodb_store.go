package mongodb

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/bsonx"
	"time"
)

func init() {
	filer.Stores = append(filer.Stores, &MongodbStore{})
}

type MongodbStore struct {
	connect        *mongo.Client
	database       string
	collectionName string
}

type Model struct {
	Directory string `bson:"directory"`
	Name      string `bson:"name"`
	Meta      []byte `bson:"meta"`
}

func (store *MongodbStore) GetName() string {
	return "mongodb"
}

func (store *MongodbStore) Initialize(configuration util.Configuration, prefix string) (err error) {
	store.database = configuration.GetString(prefix + "database")
	store.collectionName = "filemeta"
	poolSize := configuration.GetInt(prefix + "option_pool_size")
	return store.connection(configuration.GetString(prefix+"uri"), uint64(poolSize))
}

func (store *MongodbStore) connection(uri string, poolSize uint64) (err error) {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	opts := options.Client().ApplyURI(uri)

	if poolSize > 0 {
		opts.SetMaxPoolSize(poolSize)
	}

	client, err := mongo.Connect(ctx, opts)
	if err != nil {
		return err
	}

	c := client.Database(store.database).Collection(store.collectionName)
	err = store.indexUnique(c)
	store.connect = client
	return err
}

func (store *MongodbStore) createIndex(c *mongo.Collection, index mongo.IndexModel, opts *options.CreateIndexesOptions) error {
	_, err := c.Indexes().CreateOne(context.Background(), index, opts)
	return err
}

func (store *MongodbStore) indexUnique(c *mongo.Collection) error {
	opts := options.CreateIndexes().SetMaxTime(10 * time.Second)

	unique := new(bool)
	*unique = true

	index := mongo.IndexModel{
		Keys: bsonx.Doc{{Key: "directory", Value: bsonx.Int32(1)}, {Key: "name", Value: bsonx.Int32(1)}},
		Options: &options.IndexOptions{
			Unique: unique,
		},
	}

	return store.createIndex(c, index, opts)
}

func (store *MongodbStore) BeginTransaction(ctx context.Context) (context.Context, error) {
	return ctx, nil
}

func (store *MongodbStore) CommitTransaction(ctx context.Context) error {
	return nil
}

func (store *MongodbStore) RollbackTransaction(ctx context.Context) error {
	return nil
}

func (store *MongodbStore) InsertEntry(ctx context.Context, entry *filer.Entry) (err error) {

	return store.UpdateEntry(ctx, entry)

}

func (store *MongodbStore) UpdateEntry(ctx context.Context, entry *filer.Entry) (err error) {

	dir, name := entry.FullPath.DirAndName()
	meta, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		return fmt.Errorf("encode %s: %s", entry.FullPath, err)
	}

	if len(entry.Chunks) > 50 {
		meta = util.MaybeGzipData(meta)
	}

	c := store.connect.Database(store.database).Collection(store.collectionName)

	opts := options.Update().SetUpsert(true)
	filter := bson.D{{"directory", dir}, {"name", name}}
	update := bson.D{{"$set", bson.D{{"meta", meta}}}}

	_, err = c.UpdateOne(ctx, filter, update, opts)

	if err != nil {
		return fmt.Errorf("UpdateEntry %s: %v", entry.FullPath, err)
	}

	return nil
}

func (store *MongodbStore) FindEntry(ctx context.Context, fullpath util.FullPath) (entry *filer.Entry, err error) {

	dir, name := fullpath.DirAndName()
	var data Model

	var where = bson.M{"directory": dir, "name": name}
	err = store.connect.Database(store.database).Collection(store.collectionName).FindOne(ctx, where).Decode(&data)
	if err != mongo.ErrNoDocuments && err != nil {
		glog.Errorf("find %s: %v", fullpath, err)
		return nil, filer_pb.ErrNotFound
	}

	if len(data.Meta) == 0 {
		return nil, filer_pb.ErrNotFound
	}

	entry = &filer.Entry{
		FullPath: fullpath,
	}

	err = entry.DecodeAttributesAndChunks(util.MaybeDecompressData(data.Meta))
	if err != nil {
		return entry, fmt.Errorf("decode %s : %v", entry.FullPath, err)
	}

	return entry, nil
}

func (store *MongodbStore) DeleteEntry(ctx context.Context, fullpath util.FullPath) error {

	dir, name := fullpath.DirAndName()

	where := bson.M{"directory": dir, "name": name}
	_, err := store.connect.Database(store.database).Collection(store.collectionName).DeleteOne(ctx, where)
	if err != nil {
		return fmt.Errorf("delete %s : %v", fullpath, err)
	}

	return nil
}

func (store *MongodbStore) DeleteFolderChildren(ctx context.Context, fullpath util.FullPath) error {

	where := bson.M{"directory": fullpath}
	_, err := store.connect.Database(store.database).Collection(store.collectionName).DeleteMany(ctx, where)
	if err != nil {
		return fmt.Errorf("delete %s : %v", fullpath, err)
	}

	return nil
}

func (store *MongodbStore) ListDirectoryPrefixedEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, prefix string, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {
	return lastFileName, filer.ErrUnsupportedListDirectoryPrefixed
}

func (store *MongodbStore) ListDirectoryEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {

	var where = bson.M{"directory": string(dirPath), "name": bson.M{"$gt": startFileName}}
	if includeStartFile {
		where["name"] = bson.M{
			"$gte": startFileName,
		}
	}
	optLimit := int64(limit)
	opts := &options.FindOptions{Limit: &optLimit, Sort: bson.M{"name": 1}}
	cur, err := store.connect.Database(store.database).Collection(store.collectionName).Find(ctx, where, opts)
	if err != nil {
		return lastFileName, fmt.Errorf("failed to list directory entries: find error: %w", err)
	}

	for cur.Next(ctx) {
		var data Model
		err = cur.Decode(&data)
		if err != nil {
			break
		}

		entry := &filer.Entry{
			FullPath: util.NewFullPath(string(dirPath), data.Name),
		}
		lastFileName = data.Name
		if decodeErr := entry.DecodeAttributesAndChunks(util.MaybeDecompressData(data.Meta)); decodeErr != nil {
			err = decodeErr
			glog.V(0).Infof("list %s : %v", entry.FullPath, err)
			break
		}

		if !eachEntryFunc(entry) {
			break
		}

	}

	if err := cur.Close(ctx); err != nil {
		glog.V(0).Infof("list iterator close: %v", err)
	}

	return lastFileName, err
}

func (store *MongodbStore) Shutdown() {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	store.connect.Disconnect(ctx)
}
