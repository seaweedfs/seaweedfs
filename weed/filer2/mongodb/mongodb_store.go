package mongodb

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

func init() {
	filer2.Stores = append(filer2.Stores, &MongodbStore{})
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
	return store.connection(configuration.GetString(prefix + "uri"))
}

func (store *MongodbStore) connection(uri string) (err error) {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	store.connect = client
	return err
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

func (store *MongodbStore) InsertEntry(ctx context.Context, entry *filer2.Entry) (err error) {

	dir, name := entry.FullPath.DirAndName()
	meta, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		return fmt.Errorf("encode %s: %s", entry.FullPath, err)
	}

	c := store.connect.Database(store.database).Collection(store.collectionName)

	_, err = c.InsertOne(ctx, Model{
		Directory: dir,
		Name:      name,
		Meta:      meta,
	})

	return nil
}

func (store *MongodbStore) UpdateEntry(ctx context.Context, entry *filer2.Entry) (err error) {
	return store.UpdateEntry(ctx, entry)
}

func (store *MongodbStore) FindEntry(ctx context.Context, fullpath util.FullPath) (entry *filer2.Entry, err error) {

	dir, name := fullpath.DirAndName()
	var data Model

	var where = bson.M{"directory": dir, "name": name}
	err = store.connect.Database(store.database).Collection(store.collectionName).FindOne(ctx, where).Decode(&data)
	if err != mongo.ErrNoDocuments && err != nil {
		return nil, filer_pb.ErrNotFound
	}

	if len(data.Meta) == 0 {
		return nil, filer_pb.ErrNotFound
	}

	entry = &filer2.Entry{
		FullPath: fullpath,
	}

	err = entry.DecodeAttributesAndChunks(data.Meta)
	if err != nil {
		return entry, fmt.Errorf("decode %s : %v", entry.FullPath, err)
	}

	return entry, nil
}

func (store *MongodbStore) DeleteEntry(ctx context.Context, fullpath util.FullPath) error {

	where := bson.M{"directory": fullpath}
	_, err := store.connect.Database(store.database).Collection(store.collectionName).DeleteOne(ctx, where)
	if err != nil {
		return fmt.Errorf("delete %s : %v", fullpath, err)
	}

	return nil
}

func (store *MongodbStore) DeleteFolderChildren(ctx context.Context, fullpath util.FullPath) error {

	return nil
}

func (store *MongodbStore) ListDirectoryEntries(ctx context.Context, fullpath util.FullPath, startFileName string, inclusive bool, limit int) (entries []*filer2.Entry, err error) {

	var where = bson.M{"directory": string(fullpath), "name": bson.M{"$gt": startFileName,}}
	if inclusive {
		where["name"] = bson.M{
			"$gte": startFileName,
		}
	}
	optLimit := int64(limit)
	cur, err := store.connect.Database(store.database).Collection(store.collectionName).Find(ctx, where, &options.FindOptions{Limit: &optLimit})
	for cur.Next(ctx) {
		var data Model
		err := cur.Decode(&data)
		if err != nil && err != mongo.ErrNoDocuments {
			return nil, err
		}

		entry := &filer2.Entry{
			FullPath: util.NewFullPath(string(fullpath), data.Name),
		}
		if decodeErr := entry.DecodeAttributesAndChunks(data.Meta); decodeErr != nil {
			err = decodeErr
			glog.V(0).Infof("list %s : %v", entry.FullPath, err)
			break
		}

		entries = append(entries, entry)
	}

	if err := cur.Close(ctx); err != nil {
		glog.V(0).Infof("list iterator close: %v", err)
	}

	return entries, err
}

func (store *MongodbStore) Shutdown() {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	store.connect.Disconnect(ctx)
}
