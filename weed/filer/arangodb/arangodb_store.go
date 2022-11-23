package arangodb

import (
	"context"
	"crypto/tls"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/http"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	filer.Stores = append(filer.Stores, &ArangodbStore{})
}

var (
	BUCKET_PREFIX      = "/buckets"
	DEFAULT_COLLECTION = "seaweed_no_bucket"
	KVMETA_COLLECTION  = "seaweed_kvmeta"
)

type ArangodbStore struct {
	connect      driver.Connection
	client       driver.Client
	database     driver.Database
	kvCollection driver.Collection

	buckets map[string]driver.Collection
	mu      sync.RWMutex

	databaseName string
}

type Model struct {
	Key       string `json:"_key"`
	Directory string `json:"directory,omitempty"`
	Name      string `json:"name,omitempty"`
	Ttl       string `json:"ttl,omitempty"`

	//arangodb does not support binary blobs
	//we encode byte slice into uint64 slice
	//see helpers.go
	Meta []uint64 `json:"meta"`
}

func (store *ArangodbStore) GetName() string {
	return "arangodb"
}

func (store *ArangodbStore) Initialize(configuration util.Configuration, prefix string) (err error) {
	store.buckets = make(map[string]driver.Collection, 3)
	store.databaseName = configuration.GetString(prefix + "db_name")
	return store.connection(configuration.GetStringSlice(prefix+"servers"),
		configuration.GetString(prefix+"username"),
		configuration.GetString(prefix+"password"),
		configuration.GetBool(prefix+"insecure_skip_verify"),
	)
}

func (store *ArangodbStore) connection(uris []string, user string, pass string, insecure bool) (err error) {
	ctx, cn := context.WithTimeout(context.Background(), 10*time.Second)
	defer cn()
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
	if store.kvCollection, err = store.ensureCollection(ctx, KVMETA_COLLECTION); err != nil {
		return err
	}
	return err
}

type key int

const (
	transactionKey key = 0
)

func (store *ArangodbStore) BeginTransaction(ctx context.Context) (context.Context, error) {
	keys := make([]string, 0, len(store.buckets)+1)
	for k := range store.buckets {
		keys = append(keys, k)
	}
	keys = append(keys, store.kvCollection.Name())
	txn, err := store.database.BeginTransaction(ctx, driver.TransactionCollections{
		Exclusive: keys,
	}, &driver.BeginTransactionOptions{})
	if err != nil {
		return nil, err
	}

	return context.WithValue(driver.WithTransactionID(ctx, txn), transactionKey, txn), nil
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

	if len(entry.GetChunks()) > filer.CountEntryChunksForGzip {
		meta = util.MaybeGzipData(meta)
	}
	model := &Model{
		Key:       hashString(string(entry.FullPath)),
		Directory: dir,
		Name:      name,
		Meta:      bytesToArray(meta),
	}
	if entry.TtlSec > 0 {
		model.Ttl = time.Now().Add(time.Second * time.Duration(entry.TtlSec)).Format(time.RFC3339)
	} else {
		model.Ttl = ""
	}

	targetCollection, err := store.extractBucketCollection(ctx, entry.FullPath)
	if err != nil {
		return err
	}
	_, err = targetCollection.CreateDocument(ctx, model)
	if driver.IsConflict(err) {
		return store.UpdateEntry(ctx, entry)
	}

	if err != nil {
		return fmt.Errorf("InsertEntry %s: %v", entry.FullPath, err)
	}

	return nil

}

func (store *ArangodbStore) UpdateEntry(ctx context.Context, entry *filer.Entry) (err error) {
	dir, name := entry.FullPath.DirAndName()
	meta, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		return fmt.Errorf("encode %s: %s", entry.FullPath, err)
	}

	if len(entry.GetChunks()) > filer.CountEntryChunksForGzip {
		meta = util.MaybeGzipData(meta)
	}
	model := &Model{
		Key:       hashString(string(entry.FullPath)),
		Directory: dir,
		Name:      name,
		Meta:      bytesToArray(meta),
	}
	if entry.TtlSec > 0 {
		model.Ttl = time.Now().Add(time.Duration(entry.TtlSec) * time.Second).Format(time.RFC3339)
	} else {
		model.Ttl = "none"
	}
	targetCollection, err := store.extractBucketCollection(ctx, entry.FullPath)
	if err != nil {
		return err
	}
	_, err = targetCollection.UpdateDocument(ctx, model.Key, model)
	if err != nil {
		return fmt.Errorf("UpdateEntry %s: %v", entry.FullPath, err)
	}

	return nil
}

func (store *ArangodbStore) FindEntry(ctx context.Context, fullpath util.FullPath) (entry *filer.Entry, err error) {
	var data Model
	targetCollection, err := store.extractBucketCollection(ctx, fullpath)
	if err != nil {
		return nil, err
	}
	_, err = targetCollection.ReadDocument(ctx, hashString(string(fullpath)), &data)
	if err != nil {
		if driver.IsNotFound(err) {
			return nil, filer_pb.ErrNotFound
		}
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

func (store *ArangodbStore) DeleteEntry(ctx context.Context, fullpath util.FullPath) (err error) {
	targetCollection, err := store.extractBucketCollection(ctx, fullpath)
	if err != nil {
		return err
	}
	_, err = targetCollection.RemoveDocument(ctx, hashString(string(fullpath)))
	if err != nil && !driver.IsNotFound(err) {
		glog.Errorf("find %s: %v", fullpath, err)
		return fmt.Errorf("delete %s : %v", fullpath, err)
	}
	return nil
}

// this runs in log time
func (store *ArangodbStore) DeleteFolderChildren(ctx context.Context, fullpath util.FullPath) (err error) {
	var query string
	targetCollection, err := store.extractBucketCollection(ctx, fullpath)
	if err != nil {
		return err
	}
	query = query + fmt.Sprintf(`
	for d in %s
	filter starts_with(d.directory, "%s/")  || d.directory == "%s"
	remove d._key in %s`,
		"`"+targetCollection.Name()+"`",
		strings.Join(strings.Split(string(fullpath), "/"), ","),
		string(fullpath),
		"`"+targetCollection.Name()+"`",
	)
	cur, err := store.database.Query(ctx, query, nil)
	if err != nil {
		return fmt.Errorf("delete %s : %v", fullpath, err)
	}
	defer cur.Close()
	return nil
}

func (store *ArangodbStore) ListDirectoryEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {
	return store.ListDirectoryPrefixedEntries(ctx, dirPath, startFileName, includeStartFile, limit, "", eachEntryFunc)
}

func (store *ArangodbStore) ListDirectoryPrefixedEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, prefix string, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {
	targetCollection, err := store.extractBucketCollection(ctx, dirPath+"/")
	if err != nil {
		return lastFileName, err
	}
	query := "for d in " + "`" + targetCollection.Name() + "`"
	if includeStartFile {
		query = query + " filter d.name >= \"" + startFileName + "\" "
	} else {
		query = query + " filter d.name > \"" + startFileName + "\" "
	}
	if prefix != "" {
		query = query + fmt.Sprintf(`&& starts_with(d.name, "%s")`, prefix)
	}
	query = query + `
filter d.directory == @dir
sort d.name asc
`
	if limit > 0 {
		query = query + "limit " + strconv.Itoa(int(limit))
	}
	query = query + "\n return d"
	cur, err := store.database.Query(ctx, query, map[string]interface{}{"dir": dirPath})
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

func (store *ArangodbStore) Shutdown() {
}
