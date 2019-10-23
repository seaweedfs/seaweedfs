package tikv

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"io"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/glog"
	weed_util "github.com/chrislusf/seaweedfs/weed/util"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
)

func init() {
	filer2.Stores = append(filer2.Stores, &TikvStore{})
}

type TikvStore struct {
	store  kv.Storage
}

func (store *TikvStore) GetName() string {
	return "tikv"
}

func (store *TikvStore) Initialize(configuration weed_util.Configuration) (err error) {
	pdAddr := configuration.GetString("pdAddress")
	return store.initialize(pdAddr)
}

func (store *TikvStore) initialize(pdAddr string) (err error) {
	glog.Infof("filer store tikv pd address: %s", pdAddr)

	driver := tikv.Driver{}

	store.store, err = driver.Open(fmt.Sprintf("tikv://%s", pdAddr))

	if err != nil {
		return fmt.Errorf("open tikv %s : %v", pdAddr, err)
	}

	return
}

func (store *TikvStore) BeginTransaction(ctx context.Context) (context.Context, error) {
	tx, err := store.store.Begin()
	if err != nil {
		return ctx, err
	}
	return context.WithValue(ctx, "tx", tx), nil
}
func (store *TikvStore) CommitTransaction(ctx context.Context) error {
	tx, ok := ctx.Value("tx").(kv.Transaction)
	if ok {
		return tx.Commit(ctx)
	}
	return nil
}
func (store *TikvStore) RollbackTransaction(ctx context.Context) error {
	tx, ok := ctx.Value("tx").(kv.Transaction)
	if ok {
		return tx.Rollback()
	}
	return nil
}

func (store *TikvStore) getTx(ctx context.Context) kv.Transaction {
	if tx, ok := ctx.Value("tx").(kv.Transaction); ok {
		return tx
	}
	return nil
}

func (store *TikvStore) InsertEntry(ctx context.Context, entry *filer2.Entry) (err error) {
	dir, name := entry.DirAndName()
	key := genKey(dir, name)

	value, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		return fmt.Errorf("encoding %s %+v: %v", entry.FullPath, entry.Attr, err)
	}

	err = store.getTx(ctx).Set(key, value)

	if err != nil {
		return fmt.Errorf("persisting %s : %v", entry.FullPath, err)
	}

	// println("saved", entry.FullPath, "chunks", len(entry.Chunks))

	return nil
}

func (store *TikvStore) UpdateEntry(ctx context.Context, entry *filer2.Entry) (err error) {

	return store.InsertEntry(ctx, entry)
}

func (store *TikvStore) FindEntry(ctx context.Context, fullpath filer2.FullPath) (entry *filer2.Entry, err error) {
	dir, name := fullpath.DirAndName()
	key := genKey(dir, name)

	data, err := store.getTx(ctx).Get(ctx, key)

	if err == kv.ErrNotExist {
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

func (store *TikvStore) DeleteEntry(ctx context.Context, fullpath filer2.FullPath) (err error) {
	dir, name := fullpath.DirAndName()
	key := genKey(dir, name)

	err = store.getTx(ctx).Delete(key)
	if err != nil {
		return fmt.Errorf("delete %s : %v", fullpath, err)
	}

	return nil
}

func (store *TikvStore) ListDirectoryEntries(ctx context.Context, fullpath filer2.FullPath, startFileName string, inclusive bool,
	limit int) (entries []*filer2.Entry, err error) {

	directoryPrefix := genDirectoryKeyPrefix(fullpath, "")
	lastFileStart := genDirectoryKeyPrefix(fullpath, startFileName)

	iter, err := store.getTx(ctx).Iter(lastFileStart, nil)
	if err != nil {
		return nil, fmt.Errorf("list %s: %v", fullpath, err)
	}
	defer iter.Close()
	for iter.Valid() {
		key := iter.Key()
		if !bytes.HasPrefix(key, directoryPrefix) {
			break
		}
		fileName := getNameFromKey(key)
		if fileName == "" {
			iter.Next()
			continue
		}
		if fileName == startFileName && !inclusive {
			iter.Next()
			continue
		}
		limit--
		if limit < 0 {
			break
		}
		entry := &filer2.Entry{
			FullPath: filer2.NewFullPath(string(fullpath), fileName),
		}

		// println("list", entry.FullPath, "chunks", len(entry.Chunks))

		if decodeErr := entry.DecodeAttributesAndChunks(iter.Value()); decodeErr != nil {
			err = decodeErr
			glog.V(0).Infof("list %s : %v", entry.FullPath, err)
			break
		}
		entries = append(entries, entry)
		iter.Next()
	}

	return entries, err
}

func genKey(dirPath, fileName string) (key []byte) {
	key = hashToBytes(dirPath)
	key = append(key, []byte(fileName)...)
	return key
}

func genDirectoryKeyPrefix(fullpath filer2.FullPath, startFileName string) (keyPrefix []byte) {
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
func hashToBytes(dir string) ([]byte) {
	h := md5.New()
	io.WriteString(h, dir)

	b := h.Sum(nil)

	return b
}
