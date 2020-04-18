package mongodb

import (
	"context"
	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/util"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

func init() {
	filer2.Stores = append(filer2.Stores, &MongodbStore{})
}

type MongodbStore struct {
	connect *mongo.Client
}

func (store *MongodbStore) GetName() string {
	return "mongodb"
}

func (store *MongodbStore) Initialize(configuration util.Configuration, prefix string) (err error) {
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
	return nil
}

func (store *MongodbStore) UpdateEntry(ctx context.Context, entry *filer2.Entry) (err error) {
	return nil
}

func (store *MongodbStore) FindEntry(ctx context.Context, fullpath util.FullPath) (entry *filer2.Entry, err error) {
	return nil, nil
}

func (store *MongodbStore) DeleteEntry(ctx context.Context, fullpath util.FullPath) error {

	return nil
}

func (store *MongodbStore) DeleteFolderChildren(ctx context.Context, fullpath util.FullPath) error {

	return nil
}

func (store *MongodbStore) ListDirectoryEntries(ctx context.Context, fullpath util.FullPath, startFileName string, inclusive bool, limit int) (entries []*filer2.Entry, err error) {

	return nil, nil
}

func (store *MongodbStore) Shutdown() {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	store.connect.Disconnect(ctx)
}
