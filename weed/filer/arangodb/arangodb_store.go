package arangodb

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	"github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/http"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func init() {
	filer.Stores = append(filer.Stores, &ArangodbStore{})
}

type ArangodbStore struct {
	connect    driver.Connection
	client     driver.Client
	database   driver.Database
	collection driver.Collection

	databaseName string
}

type Model struct {
	Key       string `json:"_key"`
	Directory string `json:"directory,omitempty"`
	Name      string `json:"name,omitempty"`
	Bucket    string `json:"bucket,omitempty"`

	//arangodb does not support binary blobs
	//we encode byte slice into uint64 slice
	//see helpers.go
	Meta []uint64 `json:"meta"`
}

func (store *ArangodbStore) GetName() string {
	return "arangodb"
}

func (store *ArangodbStore) Initialize(configuration util.Configuration, prefix string) (err error) {
	store.databaseName = configuration.GetString(prefix + "db_name")
	return store.connection(configuration.GetStringSlice(prefix+"servers"),
		configuration.GetString(prefix+"user"),
		configuration.GetString(prefix+"pass"),
		configuration.GetBool(prefix+"insecure_skip_verify"),
	)
}

func (store *ArangodbStore) connection(uris []string, user string, pass string, insecure bool) (err error) {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)

	store.connect, err = http.NewConnection(http.ConnectionConfig{
		Endpoints: uris,
		TLSConfig: &tls.Config{
			InsecureSkipVerify: insecure,
		},
	})
	if err != nil {
		return err
	}
	store.client, err = driver.NewClient(driver.ClientConfig{
		Connection:     store.connect,
		Authentication: driver.BasicAuthentication(user, pass),
	})
	if err != nil {
		return err
	}
	ok, err := store.client.DatabaseExists(ctx, store.databaseName)
	if err != nil {
		return err
	}
	if ok {
		store.database, err = store.client.Database(ctx, store.databaseName)
	} else {
		store.database, err = store.client.CreateDatabase(ctx, store.databaseName, &driver.CreateDatabaseOptions{})
	}
	if err != nil {
		return err
	}

	coll_name := "files"
	ok, err = store.database.CollectionExists(ctx, coll_name)
	if err != nil {
		return err
	}
	if ok {
		store.collection, err = store.database.Collection(ctx, coll_name)
	} else {
		store.collection, err = store.database.CreateCollection(ctx, coll_name, &driver.CreateCollectionOptions{})
	}
	if err != nil {
		return err
	}

	// ensure indices

	if _, _, err = store.collection.EnsurePersistentIndex(ctx, []string{"directory", "name"},
		&driver.EnsurePersistentIndexOptions{
			Name:   "directory_name_multi",
			Unique: true,
		}); err != nil {
		return err
	}
	if _, _, err = store.collection.EnsurePersistentIndex(ctx, []string{"directory"},
		&driver.EnsurePersistentIndexOptions{Name: "IDX_directory"}); err != nil {
		return err
	}

	if _, _, err = store.collection.EnsurePersistentIndex(ctx, []string{"name"}, &driver.EnsurePersistentIndexOptions{
		Name: "IDX_name",
	}); err != nil {
		return err
	}
	if _, _, err = store.collection.EnsurePersistentIndex(ctx, []string{"bucket"}, &driver.EnsurePersistentIndexOptions{
		Name:   "IDX_bucket",
		Sparse: true, //sparse index, to locate files of bucket
	}); err != nil {
		return err
	}
	return err
}

type key int

const (
	transactionKey key = 0
)

func (store *ArangodbStore) BeginTransaction(ctx context.Context) (context.Context, error) {
	txn, err := store.database.BeginTransaction(ctx, driver.TransactionCollections{
		Exclusive: []string{"files"},
	}, &driver.BeginTransactionOptions{})
	if err != nil {
		return nil, err
	}

	return context.WithValue(ctx, transactionKey, txn), nil
}

func (store *ArangodbStore) CommitTransaction(ctx context.Context) error {
	val := ctx.Value(transactionKey)
	cast, ok := val.(driver.TransactionID)
	if !ok {
		return fmt.Errorf("txn cast fail %s:", val)
	}
	err := store.database.CommitTransaction(ctx, cast, &driver.CommitTransactionOptions{})
	if err != nil {
		return err
	}
	return nil
}

func (store *ArangodbStore) RollbackTransaction(ctx context.Context) error {
	val := ctx.Value(transactionKey)
	cast, ok := val.(driver.TransactionID)
	if !ok {
		return fmt.Errorf("txn cast fail %s:", val)
	}
	err := store.database.AbortTransaction(ctx, cast, &driver.AbortTransactionOptions{})
	if err != nil {
		return err
	}
	return nil
}

func (store *ArangodbStore) InsertEntry(ctx context.Context, entry *filer.Entry) (err error) {
	dir, name := entry.FullPath.DirAndName()
	meta, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		return fmt.Errorf("encode %s: %s", entry.FullPath, err)
	}

	if len(entry.Chunks) > 50 {
		meta = util.MaybeGzipData(meta)
	}
	bucket, _ := extractBucket(entry.FullPath)
	model := &Model{
		Key:       hashString(string(entry.FullPath)),
		Directory: dir,
		Name:      name,
		Meta:      bytesToArray(meta),
		Bucket:    bucket,
	}
	_, err = store.collection.CreateDocument(ctx, model)
	if driver.IsConflict(err) {
		return store.UpdateEntry(ctx, entry)
	}
	if err != nil {
		return fmt.Errorf("InsertEntry %s: %v", entry.FullPath, err)
	}

	return nil

}

func extractBucket(fullpath util.FullPath) (string, string) {
	if !strings.HasPrefix(string(fullpath), "/buckets/") {
		return "", string(fullpath)
	}
	bucketAndObjectKey := string(fullpath)[len("/buckets/"):]
	t := strings.Index(bucketAndObjectKey, "/")
	bucket := bucketAndObjectKey
	shortPath := "/"
	if t > 0 {
		bucket = bucketAndObjectKey[:t]
		shortPath = string(util.FullPath(bucketAndObjectKey[t:]))
	}
	return bucket, shortPath
}

func (store *ArangodbStore) UpdateEntry(ctx context.Context, entry *filer.Entry) (err error) {
	dir, name := entry.FullPath.DirAndName()
	meta, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		return fmt.Errorf("encode %s: %s", entry.FullPath, err)
	}

	if len(entry.Chunks) > 50 {
		meta = util.MaybeGzipData(meta)
	}
	model := &Model{
		Key:       hashString(string(entry.FullPath)),
		Directory: dir,
		Name:      name,
		Meta:      bytesToArray(meta),
	}

	_, err = store.collection.UpdateDocument(ctx, model.Key, model)

	if err != nil {
		return fmt.Errorf("UpdateEntry %s: %v", entry.FullPath, err)
	}

	return nil
}

func (store *ArangodbStore) FindEntry(ctx context.Context, fullpath util.FullPath) (entry *filer.Entry, err error) {
	var data Model
	_, err = store.collection.ReadDocument(ctx, hashString(string(fullpath)), &data)
	if driver.IsNotFound(err) {
		return nil, filer_pb.ErrNotFound
	}
	if err != nil {
		glog.Errorf("find %s: %v", fullpath, err)
		return nil, filer_pb.ErrNotFound
	}
	if len(data.Meta) == 0 {
		return nil, filer_pb.ErrNotFound
	}
	entry = &filer.Entry{
		FullPath: fullpath,
	}
	err = entry.DecodeAttributesAndChunks(util.MaybeDecompressData(arrayToBytes(data.Meta)))
	if err != nil {
		return entry, fmt.Errorf("decode %s : %v", entry.FullPath, err)
	}

	return entry, nil
}

func (store *ArangodbStore) DeleteEntry(ctx context.Context, fullpath util.FullPath) error {
	_, err := store.collection.RemoveDocument(ctx, hashString(string(fullpath)))
	if err != nil && !driver.IsNotFound(err) {
		glog.Errorf("find %s: %v", fullpath, err)
		return fmt.Errorf("delete %s : %v", fullpath, err)
	}
	return nil
}

func (store *ArangodbStore) DeleteFolderChildren(ctx context.Context, fullpath util.FullPath) error {
	query := ""
	query = fmt.Sprintf(`for d in files filter starts_with(d.directory, "%s") remove d._key in files`,
		strings.Join(strings.Split(string(fullpath), "/"), ","),
		string(fullpath),
	)
	cur, err := store.database.Query(ctx, query, nil)
	if err != nil {
		return fmt.Errorf("delete %s : %v", fullpath, err)
	}
	defer cur.Close()
	return nil
}

func (store *ArangodbStore) ListDirectoryPrefixedEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, prefix string, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {
	// if no prefix, then dont use index
	if prefix == "" {
		return store.ListDirectoryEntries(ctx, dirPath, startFileName, includeStartFile, limit, eachEntryFunc)
	}
	eq := ""
	if includeStartFile {
		eq = "filter d.name >= \"" + startFileName + "\""
	} else {
		eq = "filter d.name > \"" + startFileName + "\""
	}
	query := fmt.Sprintf(`
for d in files
filter d.directory == @dir
filter starts_with(d.name, @prefix)
%s
sort d.name asc
limit %d
return d`, eq, limit)
	cur, err := store.database.Query(ctx, query, map[string]interface{}{"dir": dirPath, "prefix": prefix})
	if err != nil {
		return lastFileName, fmt.Errorf("failed to list directory entries: find error: %w", err)
	}
	defer cur.Close()
	for cur.HasMore() {
		var data Model
		_, err = cur.ReadDocument(ctx, &data)
		if err != nil {
			break
		}
		entry := &filer.Entry{
			FullPath: util.NewFullPath(data.Directory, data.Name),
		}
		lastFileName = data.Name
		converted := arrayToBytes(data.Meta)
		if decodeErr := entry.DecodeAttributesAndChunks(util.MaybeDecompressData(converted)); decodeErr != nil {
			err = decodeErr
			glog.V(0).Infof("list %s : %v", entry.FullPath, err)
			break
		}

		if !eachEntryFunc(entry) {
			break
		}

	}
	return lastFileName, err
}

func (store *ArangodbStore) ListDirectoryEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {
	eq := ""
	if includeStartFile {
		eq = "filter d.name >= \"" + startFileName + "\""
	} else {
		eq = "filter d.name > \"" + startFileName + "\""
	}
	query := fmt.Sprintf(`
for d in files
filter d.directory == "%s"
%s
sort d.name asc
limit %d
return d`, string(dirPath), eq, limit)
	cur, err := store.database.Query(ctx, query, nil)
	if err != nil {
		return lastFileName, fmt.Errorf("failed to list directory entries: find error: %w", err)
	}
	defer cur.Close()
	for cur.HasMore() {
		var data Model
		_, err = cur.ReadDocument(ctx, &data)
		if err != nil {
			break
		}
		entry := &filer.Entry{
			FullPath: util.NewFullPath(string(dirPath), data.Name),
		}
		lastFileName = data.Name
		converted := arrayToBytes(data.Meta)
		if decodeErr := entry.DecodeAttributesAndChunks(util.MaybeDecompressData(converted)); decodeErr != nil {
			err = decodeErr
			glog.V(0).Infof("list %s : %v", entry.FullPath, err)
			break
		}

		if !eachEntryFunc(entry) {
			break
		}

	}
	return lastFileName, err
}

func (store *ArangodbStore) Shutdown() {
}
