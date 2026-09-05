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

	// Dot-prefixed entries directly under /buckets (e.g. .system) are internal
	// folders, not S3 buckets; keep them in the default DB by full path.
	if strings.HasPrefix(bucket, ".") {
		store.dbsLock.RUnlock()
		return defaultDB, DEFAULT, fullpath, nil
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

	if collection := filer.EntryCollection(entry); collection != "" {
		// atomically write the entry together with its collection index key
		batch := new(leveldb.Batch)
		batch.Put(key, value)
		batch.Put(filer.ColIdxKey(collection, entry.FullPath), nil)
		err = db.Write(batch, nil)
	} else {
		err = db.Put(key, value, nil)
	}

	if err != nil {
		return fmt.Errorf("persisting %s : %v", entry.FullPath, err)
	}

	// println("saved", entry.FullPath, "chunks", len(entry.GetChunks()))

	return nil
}

func (store *LevelDB3Store) UpdateEntry(ctx context.Context, entry *filer.Entry) (err error) {

	db, _, shortPath, findErr := store.findDB(entry.FullPath, false)
	if findErr == nil {
		sDir, sName := shortPath.DirAndName()
		key := genKey(sDir, sName)
		// If the entry previously belonged to a different collection, remove
		// that stale index key so a later cleanup of the old collection cannot
		// delete this entry through a leftover index.
		if oldData, getErr := db.Get(key, nil); getErr == nil {
			if oldCollection, _ := filer.EntryCollectionFromBlob(entry.FullPath, oldData); oldCollection != "" && oldCollection != filer.EntryCollection(entry) {
				db.Delete(filer.ColIdxKey(oldCollection, entry.FullPath), nil)
			}
		}
	}

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

	// remove the collection index key together with the entry, if any
	if oldData, getErr := db.Get(key, nil); getErr == nil {
		if collection, _ := filer.EntryCollectionFromBlob(fullpath, oldData); collection != "" {
			batch := new(leveldb.Batch)
			batch.Delete(key)
			batch.Delete(filer.ColIdxKey(collection, fullpath))
			if err = db.Write(batch, nil); err != nil {
				return fmt.Errorf("delete %s : %v", fullpath, err)
			}
			return nil
		}
	}

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
}

// ============================================================
// Collection → Filer Path 反向索引
// key 格式与共享函数见 weed/filer/collection_index.go
// ============================================================

var _ filer.CollectionIndexedStore = (*LevelDB3Store)(nil)

// DeleteCollectionEntries deletes all entries recorded under the given
// collection via the collection index, returning the number of deleted
// files and the set of parent directories that may now be empty.
// Stale index keys (entry already gone) are removed without being counted.
func (store *LevelDB3Store) DeleteCollectionEntries(ctx context.Context, collection string, eachEntryFn func(*filer.Entry)) (deletedFiles int, parentDirs []weed_util.FullPath, err error) {
	if collection == "" {
		return 0, nil, fmt.Errorf("collection is required")
	}

	prefix := filer.ColIdxPrefix(collection)
	dirSet := make(map[weed_util.FullPath]bool)

	// Open every bucket db so the scan covers all collection indexes; a bucket
	// db that was never touched is not in the map yet.
	if err = store.openAllBucketDBs(); err != nil {
		return 0, nil, err
	}

	store.dbsLock.RLock()
	dbs := make([]*leveldb.DB, 0, len(store.dbs))
	for _, db := range store.dbs {
		dbs = append(dbs, db)
	}
	store.dbsLock.RUnlock()

	for _, db := range dbs {
		batch := new(leveldb.Batch)
		batchCount := 0
		var notifyEntries []*filer.Entry

		iter := db.NewIterator(leveldb_util.BytesPrefix(prefix), nil)
		for iter.Next() {
			if ctxErr := ctx.Err(); ctxErr != nil {
				iter.Release()
				return deletedFiles, dirsOf(dirSet), ctxErr
			}

			idxKey := append([]byte(nil), iter.Key()...)
			fullPath := weed_util.FullPath(idxKey[len(prefix):])

			// Resolve the entry's db and bucket-relative key without creating a
			// bucket db (all of them are already open from openAllBucketDBs).
			entryDb, shortPath, ok := store.findOpenedDB(fullPath)
			if !ok {
				// The bucket db vanished concurrently; drop the stale index key.
				batch.Delete(idxKey)
				batchCount++
				if batchCount >= filer.ColIdxDeleteBatchSize {
					if err = db.Write(batch, nil); err != nil {
						iter.Release()
						return deletedFiles, dirsOf(dirSet), fmt.Errorf("delete collection %s entries: %v", collection, err)
					}
					batch.Reset()
					batchCount = 0
				}
				continue
			}

			sDir, sName := shortPath.DirAndName()
			entryKey := genKey(sDir, sName)

			if has, _ := entryDb.Has(entryKey, nil); has {
				// Decode the entry so the caller can propagate the deletion
				// (NotifyUpdateEvent) once it is durable. If decoding fails the
				// entry is still removed, so publish the path anyway with a
				// minimal entry so peers/subscribers drop it too.
				var notifyEntry *filer.Entry
				if eachEntryFn != nil {
					if e, findErr := store.FindEntry(ctx, fullPath); findErr == nil && e != nil {
						notifyEntry = e
					} else {
						notifyEntry = &filer.Entry{FullPath: fullPath}
					}
				}

				if entryDb == db {
					batch.Delete(entryKey)
					batchCount++
					if notifyEntry != nil {
						notifyEntries = append(notifyEntries, notifyEntry)
					}
				} else {
					if err = entryDb.Delete(entryKey, nil); err != nil {
						iter.Release()
						return deletedFiles, dirsOf(dirSet), fmt.Errorf("delete collection %s entry %s: %v", collection, fullPath, err)
					}
					if notifyEntry != nil {
						eachEntryFn(notifyEntry)
					}
				}
				deletedFiles++
				dir, _ := fullPath.DirAndName()
				dirSet[weed_util.FullPath(dir)] = true
			}
			batch.Delete(idxKey)
			batchCount++

			if batchCount >= filer.ColIdxDeleteBatchSize {
				if err = db.Write(batch, nil); err != nil {
					iter.Release()
					return deletedFiles, dirsOf(dirSet), fmt.Errorf("delete collection %s entries: %v", collection, err)
				}
				// Deletion is durable; publish the events now.
				for _, e := range notifyEntries {
					eachEntryFn(e)
				}
				notifyEntries = notifyEntries[:0]
				batch.Reset()
				batchCount = 0
			}
		}
		iter.Release()

		if batchCount > 0 {
			if err = db.Write(batch, nil); err != nil {
				return deletedFiles, dirsOf(dirSet), fmt.Errorf("delete collection %s entries: %v", collection, err)
			}
		}
		for _, e := range notifyEntries {
			eachEntryFn(e)
		}
	}

	for dir := range dirSet {
		parentDirs = append(parentDirs, dir)
	}
	return deletedFiles, parentDirs, nil
}

func dirsOf(dirSet map[weed_util.FullPath]bool) (dirs []weed_util.FullPath) {
	for dir := range dirSet {
		dirs = append(dirs, dir)
	}
	return dirs
}

// openAllBucketDBs opens every bucket db present on disk so a subsequent scan
// over store.dbs covers all collection indexes. Bucket dbs are otherwise opened
// lazily on first access.
func (store *LevelDB3Store) openAllBucketDBs() error {
	entries, err := os.ReadDir(store.dir)
	if err != nil {
		return fmt.Errorf("list filer store dir %s: %v", store.dir, err)
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		if name == DEFAULT || strings.HasPrefix(name, ".") {
			// DEFAULT is opened at init; dot-prefixed buckets live in DEFAULT.
			continue
		}
		if _, err := store.createDB(name); err != nil {
			return fmt.Errorf("open bucket db %s: %v", name, err)
		}
	}
	return nil
}

// findOpenedDB resolves the db and bucket-relative path for a full path without
// creating a bucket db. ok is false when the bucket db is not open. It is used
// by the collection-index scan, which opens every bucket db first.
func (store *LevelDB3Store) findOpenedDB(fullpath weed_util.FullPath) (db *leveldb.DB, shortPath weed_util.FullPath, ok bool) {
	store.dbsLock.RLock()
	defer store.dbsLock.RUnlock()

	defaultDB := store.dbs[DEFAULT]
	if !strings.HasPrefix(string(fullpath), "/buckets/") {
		return defaultDB, fullpath, true
	}
	bucketAndObjectKey := string(fullpath)[len("/buckets/"):]
	t := strings.Index(bucketAndObjectKey, "/")
	if t < 0 {
		return defaultDB, fullpath, true
	}
	bucket := bucketAndObjectKey
	shortPath = weed_util.FullPath("/")
	if t > 0 {
		bucket = bucketAndObjectKey[:t]
		shortPath = weed_util.FullPath(bucketAndObjectKey[t:])
	}
	if strings.HasPrefix(bucket, ".") {
		return defaultDB, fullpath, true
	}
	db, found := store.dbs[bucket]
	if !found {
		return nil, "", false
	}
	return db, shortPath, true
}
