package leveldb

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	leveldb_errors "github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
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
	emptyDB  *leveldb.DB

	blockCacheSizeMB      int
	writeBufferSizeMB     int
	blockSizeKB           int
	compactionTableSizeMB int
}

func (store *LevelDB3Store) GetName() string {
	return "leveldb3"
}

func (store *LevelDB3Store) Initialize(configuration weed_util.Configuration, prefix string) (err error) {
	dir := configuration.GetString(prefix + "dir")
	store.blockCacheSizeMB = configuration.GetInt(prefix + "block_cache_size_mb")
	store.writeBufferSizeMB = configuration.GetInt(prefix + "write_buffer_size_mb")
	store.blockSizeKB = configuration.GetInt(prefix + "block_size_kb")
	store.compactionTableSizeMB = configuration.GetInt(prefix + "compaction_table_size_mb")
	if err := store.validateSizeOptions(); err != nil {
		return err
	}
	return store.initialize(dir)
}

// validateSizeOptions rejects sizes too large to convert to bytes without
// overflowing int on this platform.
func (store *LevelDB3Store) validateSizeOptions() error {
	for name, kv := range map[string][2]int{
		"block_cache_size_mb":      {store.blockCacheSizeMB, 1 << 20},
		"write_buffer_size_mb":     {store.writeBufferSizeMB, 1 << 20},
		"block_size_kb":            {store.blockSizeKB, 1 << 10},
		"compaction_table_size_mb": {store.compactionTableSizeMB, 1 << 20},
	} {
		if kv[0] > math.MaxInt/kv[1] {
			return fmt.Errorf("leveldb3 option %s=%d overflows int", name, kv[0])
		}
	}
	return nil
}

func (store *LevelDB3Store) buildOptions(name string) *opt.Options {
	opts := &opt.Options{
		BlockCacheCapacity: 32 * 1024 * 1024, // default value is 8MiB
		WriteBuffer:        16 * 1024 * 1024, // default value is 4MiB
		Filter:             filter.NewBloomFilter(8),
		ReadOnly:           store.ReadOnly,
	}
	if name != DEFAULT {
		opts.BlockCacheCapacity = 16 * 1024 * 1024 // default value is 8MiB
		opts.WriteBuffer = 8 * 1024 * 1024         // default value is 4MiB
	}
	if store.blockCacheSizeMB > 0 {
		opts.BlockCacheCapacity = store.blockCacheSizeMB * 1024 * 1024
	}
	if store.writeBufferSizeMB > 0 {
		opts.WriteBuffer = store.writeBufferSizeMB * 1024 * 1024
	}
	if store.blockSizeKB > 0 {
		opts.BlockSize = store.blockSizeKB * 1024
	}
	if store.compactionTableSizeMB > 0 {
		opts.CompactionTableSize = store.compactionTableSizeMB * 1024 * 1024
	}
	return opts
}

func (store *LevelDB3Store) initialize(dir string) (err error) {
	glog.Infof("filer store leveldb3 dir: %s", dir)
	os.MkdirAll(dir, 0755)
	if weed_util.IsReadOnlyMount(dir) {
		glog.V(0).Infof("filer store %s is on a read-only filesystem, opening it read-only", dir)
		store.ReadOnly = true
	}
	if !store.ReadOnly {
		if err := weed_util.TestFolderWritable(dir); err != nil {
			return fmt.Errorf("Check Level Folder %s Writable: %s", dir, err)
		}
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
	opts := store.buildOptions(name)

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

	if !isBucketFolder(bucket) {
		store.dbsLock.RUnlock()
		return defaultDB, DEFAULT, fullpath, nil
	}

	if db, found := store.dbs[bucket]; found {
		store.dbsLock.RUnlock()
		return db, bucket, shortPath, nil
	}

	store.dbsLock.RUnlock()

	// A bucket with no db directory has no entries. Opening it read-only
	// cannot create the directory, so serve its reads from an empty
	// in-memory db; the store's write methods refuse mutations.
	if store.ReadOnly && !weed_util.FolderExists(fmt.Sprintf("%s/%s", store.dir, bucket)) {
		db, err := store.emptyBucketDB()
		return db, bucket, shortPath, err
	}

	db, err := store.createDB(bucket)

	return db, bucket, shortPath, err
}

func (store *LevelDB3Store) emptyBucketDB() (*leveldb.DB, error) {
	store.dbsLock.Lock()
	defer store.dbsLock.Unlock()
	if store.emptyDB == nil {
		db, err := leveldb.Open(storage.NewMemStorage(), nil)
		if err != nil {
			return nil, err
		}
		store.emptyDB = db
	}
	return store.emptyDB, nil
}

// Dot-prefixed entries directly under /buckets (e.g. .system) are internal
// folders, not S3 buckets; they stay in the default DB by full path. A bucket
// also names a folder under store.dir, so it must stay a local name.
func isBucketFolder(bucket string) bool {
	return !strings.HasPrefix(bucket, ".") && filepath.IsLocal(bucket)
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

var syncWriteOptions = &opt.WriteOptions{Sync: true}

func writeOptions(ctx context.Context) *opt.WriteOptions {
	if filer.SyncWrite(ctx) {
		return syncWriteOptions
	}
	return nil
}

func (store *LevelDB3Store) InsertEntry(ctx context.Context, entry *filer.Entry) (err error) {

	if store.ReadOnly {
		return fmt.Errorf("filer store leveldb3 is read-only")
	}
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

	err = db.Put(key, value, writeOptions(ctx))

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

	if store.ReadOnly {
		return fmt.Errorf("filer store leveldb3 is read-only")
	}
	db, _, shortPath, err := store.findDB(fullpath, false)
	if err != nil {
		return fmt.Errorf("findDB %s : %v", fullpath, err)
	}

	dir, name := shortPath.DirAndName()
	key := genKey(dir, name)

	err = db.Delete(key, writeOptions(ctx))
	if err != nil {
		return fmt.Errorf("delete %s : %v", fullpath, err)
	}

	return nil
}

func (store *LevelDB3Store) DeleteFolderChildren(ctx context.Context, fullpath weed_util.FullPath) (err error) {

	if store.ReadOnly {
		return fmt.Errorf("filer store leveldb3 is read-only")
	}
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

	err = db.Write(batch, writeOptions(ctx))

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
	if startFileName > prefix {
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
			glog.V(0).InfofCtx(ctx, "list %s : %v", entry.FullPath, err)
			break
		}

		resEachEntryFunc, resEachEntryFuncErr := eachEntryFunc(entry)
		if resEachEntryFuncErr != nil {
			err = fmt.Errorf("failed to process eachEntryFunc: %w", resEachEntryFuncErr)
			break
		}

		if !resEachEntryFunc {
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
	if store.emptyDB != nil {
		store.emptyDB.Close()
	}
}
