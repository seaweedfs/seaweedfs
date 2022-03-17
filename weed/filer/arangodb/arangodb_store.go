package arangodb

import (
	"context"
	"crypto/md5"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
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
}

type Model struct {
	Key       string   `json:"_key"`
	Directory string   `json:"directory"`
	Name      string   `json:"name"`
	Meta      []uint64 `json:"meta"`
}

func (store *ArangodbStore) GetName() string {
	return "arangodb"
}

func (store *ArangodbStore) Initialize(configuration util.Configuration, prefix string) (err error) {
	return store.connection(configuration.GetStringSlice(prefix+"arango_host"),
		configuration.GetString(prefix+"arango_user"),
		configuration.GetString(prefix+"arango_pass"),
	)
}

func (store *ArangodbStore) connection(uris []string, user string, pass string) (err error) {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)

	store.connect, err = http.NewConnection(http.ConnectionConfig{
		Endpoints: uris,
		TLSConfig: &tls.Config{
			InsecureSkipVerify: true,
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
	db_name := "seaweed-filer"
	ok, err := store.client.DatabaseExists(ctx, db_name)
	if err != nil {
		return err
	}
	if ok {
		store.database, err = store.client.Database(ctx, db_name)
	} else {
		store.database, err = store.client.CreateDatabase(ctx, db_name, &driver.CreateDatabaseOptions{})
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

	if _, _, err = store.collection.EnsurePersistentIndex(ctx, []string{"directory", "name"}, &driver.EnsurePersistentIndexOptions{
		Name:   "directory_name_multi",
		Unique: true,
	}); err != nil {
		return err
	}
	if _, _, err = store.collection.EnsurePersistentIndex(ctx, []string{"directory"},
		&driver.EnsurePersistentIndexOptions{Name: "IDX_directory"}); err != nil {
		return err
	}
	//  fulltext index not required since no prefix search
	//  might change
	//	if _, _, err = store.collection.EnsureFullTextIndex(ctx, []string{"directory"},
	//		&driver.EnsureFullTextIndexOptions{Name: "IDX_FULLTEXT_directory", MinLength: 1}); err != nil {
	//		return err
	//	}

	if _, _, err = store.collection.EnsurePersistentIndex(ctx, []string{"name"}, &driver.EnsurePersistentIndexOptions{
		Name: "IDX_name",
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
	model := &Model{
		Key:       hashString(string(entry.FullPath)),
		Directory: dir,
		Name:      name,
		Meta:      bytesToArray(meta),
	}
	_, err = store.collection.CreateDocument(ctx, model)

	if err != nil {
		return fmt.Errorf("UpdateEntry %s: %v", entry.FullPath, err)
	}

	return nil

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
	if err != nil {
		glog.Errorf("find %s: %v", fullpath, err)
		return fmt.Errorf("delete %s : %v", fullpath, err)
	}

	return nil
}

func (store *ArangodbStore) DeleteFolderChildren(ctx context.Context, fullpath util.FullPath) error {
	dir, _ := fullpath.DirAndName()
	cur, err := store.database.Query(ctx, `
for d in files
filter d.directory == @dir
remove d in files`, map[string]interface{}{"dir": dir})
	if err != nil {
		return fmt.Errorf("delete %s : %v", fullpath, err)
	}
	defer cur.Close()
	return nil
}

func (store *ArangodbStore) ListDirectoryPrefixedEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, prefix string, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {
	return lastFileName, filer.ErrUnsupportedListDirectoryPrefixed
}

//TODO: i must be misunderstanding what this function is supposed to do
//so figure it out is the todo, i guess lol - aaaaa
//func (store *ArangodbStore) ListDirectoryPrefixedEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, prefix string, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {
//	eq := ""
//	if includeStartFile {
//		eq = "filter d.name >= \"" + startFileName + "\""
//	} else {
//		eq = "filter d.name > \"" + startFileName + "\""
//	}
//	query := fmt.Sprintf(`
//for d in fulltext(files,"directory","prefix:%s")
//sort d.name desc
//%s
//limit %d
//return d`, string(dirPath), eq, limit)
//	cur, err := store.database.Query(ctx, query, nil)
//	if err != nil {
//		return lastFileName, fmt.Errorf("failed to list directory entries: find error: %w", err)
//	}
//	defer cur.Close()
//	for cur.HasMore() {
//		var data Model
//		_, err = cur.ReadDocument(ctx, &data)
//		if err != nil {
//			break
//		}
//		entry := &filer.Entry{
//			FullPath: util.NewFullPath(data.Directory, data.Name),
//		}
//		lastFileName = data.Name
//		converted := arrayToBytes(data.Meta)
//		if decodeErr := entry.DecodeAttributesAndChunks(util.MaybeDecompressData(converted)); decodeErr != nil {
//			err = decodeErr
//			glog.V(0).Infof("list %s : %v", entry.FullPath, err)
//			break
//		}
//
//		if !eachEntryFunc(entry) {
//			break
//		}
//
//	}
//	return lastFileName, err
//}

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
sort d.name desc
%s
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

//convert a string into arango-key safe hex bytes hash
func hashString(dir string) string {
	h := md5.New()
	io.WriteString(h, dir)
	b := h.Sum(nil)
	return hex.EncodeToString(b)
}

func bytesToArray(bs []byte) []uint64 {
	out := make([]uint64, 0, 2+len(bs)/8)
	out = append(out, uint64(len(bs)))
	for len(bs)%8 != 0 {
		bs = append(bs, 0)
	}
	for i := 0; i < len(bs); i = i + 8 {
		out = append(out, binary.BigEndian.Uint64(bs[i:]))
	}
	return out
}

func arrayToBytes(xs []uint64) []byte {
	if len(xs) < 2 {
		return []byte{}
	}
	first := xs[0]
	out := make([]byte, len(xs)*8)
	for i := 1; i < len(xs); i = i + 1 {
		binary.BigEndian.PutUint64(out[((i-1)*8):], xs[i])
	}
	return out[:first]
}
