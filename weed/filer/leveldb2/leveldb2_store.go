package leveldb

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"os"

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

func init() {
	filer.Stores = append(filer.Stores, &LevelDB2Store{})
}

type LevelDB2Store struct {
	dbs      []*leveldb.DB
	dbCount  int
	ReadOnly bool
}

func (store *LevelDB2Store) GetName() string {
	return "leveldb2"
}

func (store *LevelDB2Store) Initialize(configuration weed_util.Configuration, prefix string) (err error) {
	dir := configuration.GetString(prefix + "dir")
	return store.initialize(dir, 8)
}

func (store *LevelDB2Store) initialize(dir string, dbCount int) (err error) {
	glog.Infof("filer store leveldb2 dir: %s", dir)
	os.MkdirAll(dir, 0755)
	if err := weed_util.TestFolderWritable(dir); err != nil {
		return fmt.Errorf("Check Level Folder %s Writable: %s", dir, err)
	}

	opts := &opt.Options{
		BlockCacheCapacity: 32 * 1024 * 1024,         // default value is 8MiB
		WriteBuffer:        16 * 1024 * 1024,         // default value is 4MiB
		Filter:             filter.NewBloomFilter(8), // false positive rate 0.02
		ReadOnly:           store.ReadOnly,
	}

	for d := 0; d < dbCount; d++ {
		dbFolder := fmt.Sprintf("%s/%02d", dir, d)
		os.MkdirAll(dbFolder, 0755)
		db, dbErr := leveldb.OpenFile(dbFolder, opts)
		if leveldb_errors.IsCorrupted(dbErr) {
			db, dbErr = leveldb.RecoverFile(dbFolder, opts)
		}
		if dbErr != nil {
			glog.Errorf("filer store open dir %s: %v", dbFolder, dbErr)
			return dbErr
		}
		store.dbs = append(store.dbs, db)
	}
	store.dbCount = dbCount

	return
}

func (store *LevelDB2Store) BeginTransaction(ctx context.Context) (context.Context, error) {
	return ctx, nil
}
func (store *LevelDB2Store) CommitTransaction(ctx context.Context) error {
	return nil
}
func (store *LevelDB2Store) RollbackTransaction(ctx context.Context) error {
	return nil
}

func (store *LevelDB2Store) InsertEntry(ctx context.Context, entry *filer.Entry) (err error) {
	dir, name := entry.DirAndName()
	key, partitionId := genKey(dir, name, store.dbCount)

	value, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		return fmt.Errorf("encoding %s %+v: %v", entry.FullPath, entry.Attr, err)
	}

	if len(entry.GetChunks()) > filer.CountEntryChunksForGzip {
		value = weed_util.MaybeGzipData(value)
	}

	err = store.dbs[partitionId].Put(key, value, nil)

	if err != nil {
		return fmt.Errorf("persisting %s : %v", entry.FullPath, err)
	}

	// println("saved", entry.FullPath, "chunks", len(entry.GetChunks()))

	return nil
}

func (store *LevelDB2Store) UpdateEntry(ctx context.Context, entry *filer.Entry) (err error) {

	return store.InsertEntry(ctx, entry)
}

func (store *LevelDB2Store) FindEntry(ctx context.Context, fullpath weed_util.FullPath) (entry *filer.Entry, err error) {
	dir, name := fullpath.DirAndName()
	key, partitionId := genKey(dir, name, store.dbCount)

	data, err := store.dbs[partitionId].Get(key, nil)

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

func (store *LevelDB2Store) DeleteEntry(ctx context.Context, fullpath weed_util.FullPath) (err error) {
	dir, name := fullpath.DirAndName()
	key, partitionId := genKey(dir, name, store.dbCount)

	err = store.dbs[partitionId].Delete(key, nil)
	if err != nil {
		return fmt.Errorf("delete %s : %v", fullpath, err)
	}

	return nil
}

func (store *LevelDB2Store) DeleteFolderChildren(ctx context.Context, fullpath weed_util.FullPath) (err error) {
	directoryPrefix, partitionId := genDirectoryKeyPrefix(fullpath, "", store.dbCount)

	batch := new(leveldb.Batch)

	iter := store.dbs[partitionId].NewIterator(&leveldb_util.Range{Start: directoryPrefix}, nil)
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

	err = store.dbs[partitionId].Write(batch, nil)

	if err != nil {
		return fmt.Errorf("delete %s : %v", fullpath, err)
	}

	return nil
}

func (store *LevelDB2Store) ListDirectoryEntries(ctx context.Context, dirPath weed_util.FullPath, startFileName string, includeStartFile bool, limit int64, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {
	return store.ListDirectoryPrefixedEntries(ctx, dirPath, startFileName, includeStartFile, limit, "", eachEntryFunc)
}

func (store *LevelDB2Store) ListDirectoryPrefixedEntries(ctx context.Context, dirPath weed_util.FullPath, startFileName string, includeStartFile bool, limit int64, prefix string, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {

	directoryPrefix, partitionId := genDirectoryKeyPrefix(dirPath, prefix, store.dbCount)
	lastFileStart := directoryPrefix
	if startFileName != "" {
		lastFileStart, _ = genDirectoryKeyPrefix(dirPath, startFileName, store.dbCount)
	}

	iter := store.dbs[partitionId].NewIterator(&leveldb_util.Range{Start: lastFileStart}, nil)
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

func genKey(dirPath, fileName string, dbCount int) (key []byte, partitionId int) {
	key, partitionId = hashToBytes(dirPath, dbCount)
	key = append(key, []byte(fileName)...)
	return key, partitionId
}

func genDirectoryKeyPrefix(fullpath weed_util.FullPath, startFileName string, dbCount int) (keyPrefix []byte, partitionId int) {
	keyPrefix, partitionId = hashToBytes(string(fullpath), dbCount)
	if len(startFileName) > 0 {
		keyPrefix = append(keyPrefix, []byte(startFileName)...)
	}
	return keyPrefix, partitionId
}

func getNameFromKey(key []byte) string {

	return string(key[md5.Size:])

}

// hash directory, and use last byte for partitioning
func hashToBytes(dir string, dbCount int) ([]byte, int) {
	h := md5.New()
	io.WriteString(h, dir)

	b := h.Sum(nil)

	x := b[len(b)-1]

	return b, int(x) % dbCount
}

func (store *LevelDB2Store) Shutdown() {
	for d := 0; d < store.dbCount; d++ {
		store.dbs[d].Close()
	}
}
