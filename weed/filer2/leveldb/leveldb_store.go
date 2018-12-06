package leveldb

import (
	"bytes"
	"fmt"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/glog"
	weed_util "github.com/chrislusf/seaweedfs/weed/util"
	"github.com/syndtr/goleveldb/leveldb"
	leveldb_util "github.com/syndtr/goleveldb/leveldb/util"
)

const (
	DIR_FILE_SEPARATOR = byte(0x00)
)

func init() {
	filer2.Stores = append(filer2.Stores, &LevelDBStore{})
}

type LevelDBStore struct {
	db *leveldb.DB
}

func (store *LevelDBStore) GetName() string {
	return "leveldb"
}

func (store *LevelDBStore) Initialize(configuration weed_util.Configuration) (err error) {
	dir := configuration.GetString("dir")
	return store.initialize(dir)
}

func (store *LevelDBStore) initialize(dir string) (err error) {
	glog.Infof("filer store dir: %s", dir)
	if err := weed_util.TestFolderWritable(dir); err != nil {
		return fmt.Errorf("Check Level Folder %s Writable: %s", dir, err)
	}

	if store.db, err = leveldb.OpenFile(dir, nil); err != nil {
		glog.Infof("filer store open dir %s: %v", dir, err)
		return
	}
	return
}

func (store *LevelDBStore) InsertEntry(entry *filer2.Entry) (err error) {
	key := genKey(entry.DirAndName())

	value, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		return fmt.Errorf("encoding %s %+v: %v", entry.FullPath, entry.Attr, err)
	}

	err = store.db.Put(key, value, nil)

	if err != nil {
		return fmt.Errorf("persisting %s : %v", entry.FullPath, err)
	}

	// println("saved", entry.FullPath, "chunks", len(entry.Chunks))

	return nil
}

func (store *LevelDBStore) UpdateEntry(entry *filer2.Entry) (err error) {

	return store.InsertEntry(entry)
}

func (store *LevelDBStore) FindEntry(fullpath filer2.FullPath) (entry *filer2.Entry, err error) {
	key := genKey(fullpath.DirAndName())

	data, err := store.db.Get(key, nil)

	if err == leveldb.ErrNotFound {
		return nil, filer2.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get %s : %v", entry.FullPath, err)
	}

	entry = &filer2.Entry{
		FullPath: fullpath,
	}
	err = entry.DecodeAttributesAndChunks(data)
	if err != nil {
		return entry, fmt.Errorf("decode %s : %v", entry.FullPath, err)
	}

	// println("read", entry.FullPath, "chunks", len(entry.Chunks), "data", len(data), string(data))

	return entry, nil
}

func (store *LevelDBStore) DeleteEntry(fullpath filer2.FullPath) (err error) {
	key := genKey(fullpath.DirAndName())

	err = store.db.Delete(key, nil)
	if err != nil {
		return fmt.Errorf("delete %s : %v", fullpath, err)
	}

	return nil
}

func (store *LevelDBStore) ListDirectoryEntries(fullpath filer2.FullPath, startFileName string, inclusive bool,
	limit int) (entries []*filer2.Entry, err error) {

	directoryPrefix := genDirectoryKeyPrefix(fullpath, "")

	iter := store.db.NewIterator(&leveldb_util.Range{Start: genDirectoryKeyPrefix(fullpath, startFileName)}, nil)
	for iter.Next() {
		key := iter.Key()
		if !bytes.HasPrefix(key, directoryPrefix) {
			break
		}
		fileName := getNameFromKey(key)
		if fileName == "" {
			continue
		}
		if fileName == startFileName && !inclusive {
			continue
		}
		limit--
		if limit < 0 {
			break
		}
		entry := &filer2.Entry{
			FullPath: filer2.NewFullPath(string(fullpath), fileName),
		}
		if decodeErr := entry.DecodeAttributesAndChunks(iter.Value()); decodeErr != nil {
			err = decodeErr
			glog.V(0).Infof("list %s : %v", entry.FullPath, err)
			break
		}
		entries = append(entries, entry)
	}
	iter.Release()

	return entries, err
}

func genKey(dirPath, fileName string) (key []byte) {
	key = []byte(dirPath)
	key = append(key, DIR_FILE_SEPARATOR)
	key = append(key, []byte(fileName)...)
	return key
}

func genDirectoryKeyPrefix(fullpath filer2.FullPath, startFileName string) (keyPrefix []byte) {
	keyPrefix = []byte(string(fullpath))
	keyPrefix = append(keyPrefix, DIR_FILE_SEPARATOR)
	if len(startFileName) > 0 {
		keyPrefix = append(keyPrefix, []byte(startFileName)...)
	}
	return keyPrefix
}

func getNameFromKey(key []byte) string {

	sepIndex := len(key) - 1
	for sepIndex >= 0 && key[sepIndex] != DIR_FILE_SEPARATOR {
		sepIndex--
	}

	return string(key[sepIndex+1:])

}
