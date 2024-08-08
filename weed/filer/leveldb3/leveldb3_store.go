package leveldb

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	leveldb_errors "github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	leveldb_util "github.com/syndtr/goleveldb/leveldb/util"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	weed_util "github.com/seaweedfs/seaweedfs/weed/util"
)

const (
	DEFAULT = "_main"
)

func init() {
	filer.Stores = append(filer.Stores, &LevelDB3Store{})
}

type LevelDB3Store struct {
	dir      string
	dbs      map[string]*leveldb.DB
	dbsLock  sync.RWMutex
	ReadOnly bool
}

func (store *LevelDB3Store) GetName() string {
	return "leveldb3"
}

func (store *LevelDB3Store) Initialize(configuration weed_util.Configuration, prefix string) (err error) {
	dir := configuration.GetString(prefix + "dir")
	return store.initialize(dir)
}

func (store *LevelDB3Store) initialize(dir string) (err error) {
	glog.Infof("filer store leveldb3 dir: %s", dir)
	os.MkdirAll(dir, 0755)
	if err := weed_util.TestFolderWritable(dir); err != nil {
		return fmt.Errorf("Check Level Folder %s Writable: %s", dir, err)
	}
	store.dir = dir

	db, loadDbErr := store.loadDB(DEFAULT)
	if loadDbErr != nil {
		return loadDbErr
	}
	store.dbs = make(map[string]*leveldb.DB)
	store.dbs[DEFAULT] = db

	return
}

func (store *LevelDB3Store) loadDB(name string) (*leveldb.DB, error) {
	bloom := filter.NewBloomFilter(8) // false positive rate 0.02
	opts := &opt.Options{
		BlockCacheCapacity: 32 * 1024 * 1024, // default value is 8MiB
		WriteBuffer:        16 * 1024 * 1024, // default value is 4MiB
		Filter:             bloom,
		ReadOnly:           store.ReadOnly,
	}
	if name != DEFAULT {
		opts = &opt.Options{
			BlockCacheCapacity: 16 * 1024 * 1024, // default value is 8MiB
			WriteBuffer:        8 * 1024 * 1024,  // default value is 4MiB
			Filter:             bloom,
			ReadOnly:           store.ReadOnly,
		}
	}

	dbFolder := fmt.Sprintf("%s/%s", store.dir, name)
	os.MkdirAll(dbFolder, 0755)
	db, dbErr := leveldb.OpenFile(dbFolder, opts)
	if leveldb_errors.IsCorrupted(dbErr) {
		db, dbErr = leveldb.RecoverFile(dbFolder, opts)
	}
	if dbErr != nil {
		glog.Errorf("filer store open dir %s: %v", dbFolder, dbErr)
		return nil, dbErr
	}
	return db, nil
}

func (store *LevelDB3Store) findDB(fullpath weed_util.FullPath, isForChildren bool) (*leveldb.DB, string, weed_util.FullPath, error) {

	store.dbsLock.RLock()

	defaultDB := store.dbs[DEFAULT]
	if !strings.HasPrefix(string(fullpath), "/buckets/") {
		store.dbsLock.RUnlock()
		return defaultDB, DEFAULT, fullpath, nil
	}

	// detect bucket
	bucketAndObjectKey := string(fullpath)[len("/buckets/"):]
	t := strings.Index(bucketAndObjectKey, "/")
	if t < 0 && !isForChildren {
		store.dbsLock.RUnlock()
		return defaultDB, DEFAULT, fullpath, nil
	}
	bucket := bucketAndObjectKey
	shortPath := weed_util.FullPath("/")
	if t > 0 {
		bucket = bucketAndObjectKey[:t]
		shortPath = weed_util.FullPath(bucketAndObjectKey[t:])
	}

	if db, found := store.dbs[bucket]; found {
		store.dbsLock.RUnlock()
		return db, bucket, shortPath, nil
	}

	store.dbsLock.RUnlock()

	db, err := store.createDB(bucket)

	return db, bucket, shortPath, err
}

func (store *LevelDB3Store) createDB(bucket string) (*leveldb.DB, error) {

	store.dbsLock.Lock()
	defer store.dbsLock.Unlock()

	// double check after getting the write lock
	if db, found := store.dbs[bucket]; found {
		return db, nil
	}

	// create db
	db, err := store.loadDB(bucket)
	if err != nil {
		return nil, err
	}

	store.dbs[bucket] = db

	return db, nil
}

func (store *LevelDB3Store) closeDB(bucket string) {

	store.dbsLock.Lock()
	defer store.dbsLock.Unlock()

	if db, found := store.dbs[bucket]; found {
		db.Close()
		delete(store.dbs, bucket)
	}

}

func (store *LevelDB3Store) BeginTransaction(ctx context.Context) (context.Context, error) {
	return ctx, nil
}
func (store *LevelDB3Store) CommitTransaction(ctx context.Context) error {
	return nil
}
func (store *LevelDB3Store) RollbackTransaction(ctx context.Context) error {
	return nil
}

func (store *LevelDB3Store) InsertEntry(ctx context.Context, entry *filer.Entry) (err error) {

	db, _, shortPath, err := store.findDB(entry.FullPath, false)
	if err != nil {
		return fmt.Errorf("findDB %s : %v", entry.FullPath, err)
	}

	dir, name := shortPath.DirAndName()
	key := genKey(dir, name)

	value, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		return fmt.Errorf("encoding %s %+v: %v", entry.FullPath, entry.Attr, err)
	}

	if len(entry.GetChunks()) > filer.CountEntryChunksForGzip {
		value = weed_util.MaybeGzipData(value)
	}

	err = db.Put(key, value, nil)

	if err != nil {
		return fmt.Errorf("persisting %s : %v", entry.FullPath, err)
	}

	// println("saved", entry.FullPath, "chunks", len(entry.GetChunks()))

	return nil
}

func (store *LevelDB3Store) UpdateEntry(ctx context.Context, entry *filer.Entry) (err error) {

	return store.InsertEntry(ctx, entry)
}

func (store *LevelDB3Store) FindEntry(ctx context.Context, fullpath weed_util.FullPath) (entry *filer.Entry, err error) {

	db, _, shortPath, err := store.findDB(fullpath, false)
	if err != nil {
		return nil, fmt.Errorf("findDB %s : %v", fullpath, err)
	}

	dir, name := shortPath.DirAndName()
	key := genKey(dir, name)

	data, err := db.Get(key, nil)

	if err == leveldb.ErrNotFound {
		return nil, filer_pb.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get %s : %v", fullpath, err)
	}

	entry = &filer.Entry{
		FullPath: fullpath,
	}
	err = entry.DecodeAttributesAndChunks(weed_util.MaybeDecompressData(data))
	if err != nil {
		return entry, fmt.Errorf("decode %s : %v", entry.FullPath, err)
	}

	// println("read", entry.FullPath, "chunks", len(entry.GetChunks()), "data", len(data), string(data))

	return entry, nil
}

func (store *LevelDB3Store) DeleteEntry(ctx context.Context, fullpath weed_util.FullPath) (err error) {

	db, _, shortPath, err := store.findDB(fullpath, false)
	if err != nil {
		return fmt.Errorf("findDB %s : %v", fullpath, err)
	}

	dir, name := shortPath.DirAndName()
	key := genKey(dir, name)

	err = db.Delete(key, nil)
	if err != nil {
		return fmt.Errorf("delete %s : %v", fullpath, err)
	}

	return nil
}

func (store *LevelDB3Store) DeleteFolderChildren(ctx context.Context, fullpath weed_util.FullPath) (err error) {

	db, bucket, shortPath, err := store.findDB(fullpath, true)
	if err != nil {
		return fmt.Errorf("findDB %s : %v", fullpath, err)
	}

	if bucket != DEFAULT && shortPath == "/" {
		store.closeDB(bucket)
		if bucket != "" { // just to make sure
			os.RemoveAll(store.dir + "/" + bucket)
		}
		return nil
	}

	directoryPrefix := genDirectoryKeyPrefix(shortPath, "")

	batch := new(leveldb.Batch)

	iter := db.NewIterator(&leveldb_util.Range{Start: directoryPrefix}, nil)
	for iter.Next() {
		key := iter.Key()
		if !bytes.HasPrefix(key, directoryPrefix) {
			break
		}
		fileName := getNameFromKey(key)
		if fileName == "" {
			continue
		}
		batch.Delete(append(directoryPrefix, []byte(fileName)...))
	}
	iter.Release()

	err = db.Write(batch, nil)

	if err != nil {
		return fmt.Errorf("delete %s : %v", fullpath, err)
	}

	return nil
}

func (store *LevelDB3Store) ListDirectoryEntries(ctx context.Context, dirPath weed_util.FullPath, startFileName string, includeStartFile bool, limit int64, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {
	return store.ListDirectoryPrefixedEntries(ctx, dirPath, startFileName, includeStartFile, limit, "", eachEntryFunc)
}

func (store *LevelDB3Store) ListDirectoryPrefixedEntries(ctx context.Context, dirPath weed_util.FullPath, startFileName string, includeStartFile bool, limit int64, prefix string, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {

	db, _, shortPath, err := store.findDB(dirPath, true)
	if err != nil {
		return lastFileName, fmt.Errorf("findDB %s : %v", dirPath, err)
	}

	directoryPrefix := genDirectoryKeyPrefix(shortPath, prefix)
	lastFileStart := directoryPrefix
	if startFileName != "" {
		lastFileStart = genDirectoryKeyPrefix(shortPath, startFileName)
	}

	iter := db.NewIterator(&leveldb_util.Range{Start: lastFileStart}, nil)
	for iter.Next() {
		key := iter.Key()
		if !bytes.HasPrefix(key, directoryPrefix) {
			break
		}
		fileName := getNameFromKey(key)
		if fileName == "" {
			continue
		}
		if fileName == startFileName && !includeStartFile {
			continue
		}
		limit--
		if limit < 0 {
			break
		}
		lastFileName = fileName
		entry := &filer.Entry{
			FullPath: weed_util.NewFullPath(string(dirPath), fileName),
		}

		// println("list", entry.FullPath, "chunks", len(entry.GetChunks()))
		if decodeErr := entry.DecodeAttributesAndChunks(weed_util.MaybeDecompressData(iter.Value())); decodeErr != nil {
			err = decodeErr
			glog.V(0).Infof("list %s : %v", entry.FullPath, err)
			break
		}
		if !eachEntryFunc(entry) {
			break
		}
	}
	iter.Release()

	return lastFileName, err
}

func genKey(dirPath, fileName string) (key []byte) {
	key = hashToBytes(dirPath)
	key = append(key, []byte(fileName)...)
	return key
}

func genDirectoryKeyPrefix(fullpath weed_util.FullPath, startFileName string) (keyPrefix []byte) {
	keyPrefix = hashToBytes(string(fullpath))
	if len(startFileName) > 0 {
		keyPrefix = append(keyPrefix, []byte(startFileName)...)
	}
	return keyPrefix
}

func getNameFromKey(key []byte) string {

	return string(key[md5.Size:])

}

// hash directory
func hashToBytes(dir string) []byte {
	h := md5.New()
	io.WriteString(h, dir)
	b := h.Sum(nil)
	return b
}

func (store *LevelDB3Store) Shutdown() {
	for _, db := range store.dbs {
		db.Close()
	}
}
