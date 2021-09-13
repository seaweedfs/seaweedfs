package etcd

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"go.etcd.io/etcd/client/v3"

	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	weed_util "github.com/chrislusf/seaweedfs/weed/util"
)

const (
	DIR_FILE_SEPARATOR = byte(0x00)
)

func init() {
	filer.Stores = append(filer.Stores, &EtcdStore{})
}

type EtcdStore struct {
	client *clientv3.Client
}

func (store *EtcdStore) GetName() string {
	return "etcd"
}

func (store *EtcdStore) Initialize(configuration weed_util.Configuration, prefix string) (err error) {
	servers := configuration.GetString(prefix + "servers")
	if servers == "" {
		servers = "localhost:2379"
	}

	timeout := configuration.GetString(prefix + "timeout")
	if timeout == "" {
		timeout = "3s"
	}

	return store.initialize(servers, timeout)
}

func (store *EtcdStore) initialize(servers string, timeout string) (err error) {
	glog.Infof("filer store etcd: %s", servers)

	to, err := time.ParseDuration(timeout)
	if err != nil {
		return fmt.Errorf("parse timeout %s: %s", timeout, err)
	}

	store.client, err = clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(servers, ","),
		DialTimeout: to,
	})
	if err != nil {
		return fmt.Errorf("connect to etcd %s: %s", servers, err)
	}

	return
}

func (store *EtcdStore) BeginTransaction(ctx context.Context) (context.Context, error) {
	return ctx, nil
}
func (store *EtcdStore) CommitTransaction(ctx context.Context) error {
	return nil
}
func (store *EtcdStore) RollbackTransaction(ctx context.Context) error {
	return nil
}

func (store *EtcdStore) InsertEntry(ctx context.Context, entry *filer.Entry) (err error) {
	key := genKey(entry.DirAndName())

	meta, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		return fmt.Errorf("encoding %s %+v: %v", entry.FullPath, entry.Attr, err)
	}

	if len(entry.Chunks) > 50 {
		meta = weed_util.MaybeGzipData(meta)
	}

	if _, err := store.client.Put(ctx, string(key), string(meta)); err != nil {
		return fmt.Errorf("persisting %s : %v", entry.FullPath, err)
	}

	return nil
}

func (store *EtcdStore) UpdateEntry(ctx context.Context, entry *filer.Entry) (err error) {
	return store.InsertEntry(ctx, entry)
}

func (store *EtcdStore) FindEntry(ctx context.Context, fullpath weed_util.FullPath) (entry *filer.Entry, err error) {
	key := genKey(fullpath.DirAndName())

	resp, err := store.client.Get(ctx, string(key))
	if err != nil {
		return nil, fmt.Errorf("get %s : %v", fullpath, err)
	}

	if len(resp.Kvs) == 0 {
		return nil, filer_pb.ErrNotFound
	}

	entry = &filer.Entry{
		FullPath: fullpath,
	}
	err = entry.DecodeAttributesAndChunks(weed_util.MaybeDecompressData(resp.Kvs[0].Value))
	if err != nil {
		return entry, fmt.Errorf("decode %s : %v", entry.FullPath, err)
	}

	return entry, nil
}

func (store *EtcdStore) DeleteEntry(ctx context.Context, fullpath weed_util.FullPath) (err error) {
	key := genKey(fullpath.DirAndName())

	if _, err := store.client.Delete(ctx, string(key)); err != nil {
		return fmt.Errorf("delete %s : %v", fullpath, err)
	}

	return nil
}

func (store *EtcdStore) DeleteFolderChildren(ctx context.Context, fullpath weed_util.FullPath) (err error) {
	directoryPrefix := genDirectoryKeyPrefix(fullpath, "")

	if _, err := store.client.Delete(ctx, string(directoryPrefix), clientv3.WithPrefix()); err != nil {
		return fmt.Errorf("deleteFolderChildren %s : %v", fullpath, err)
	}

	return nil
}

func (store *EtcdStore) ListDirectoryPrefixedEntries(ctx context.Context, dirPath weed_util.FullPath, startFileName string, includeStartFile bool, limit int64, prefix string, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {
	return lastFileName, filer.ErrUnsupportedListDirectoryPrefixed
}

func (store *EtcdStore) ListDirectoryEntries(ctx context.Context, dirPath weed_util.FullPath, startFileName string, includeStartFile bool, limit int64, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {
	directoryPrefix := genDirectoryKeyPrefix(dirPath, "")
	lastFileStart := directoryPrefix
	if startFileName != "" {
		lastFileStart = genDirectoryKeyPrefix(dirPath, startFileName)
	}

	resp, err := store.client.Get(ctx, string(lastFileStart),
		clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))
	if err != nil {
		return lastFileName, fmt.Errorf("list %s : %v", dirPath, err)
	}

	for _, kv := range resp.Kvs {
		if !bytes.HasPrefix(kv.Key, directoryPrefix) {
			break
		}
		fileName := getNameFromKey(kv.Key)
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
		entry := &filer.Entry{
			FullPath: weed_util.NewFullPath(string(dirPath), fileName),
		}
		if decodeErr := entry.DecodeAttributesAndChunks(weed_util.MaybeDecompressData(kv.Value)); decodeErr != nil {
			err = decodeErr
			glog.V(0).Infof("list %s : %v", entry.FullPath, err)
			break
		}
		if !eachEntryFunc(entry) {
			break
		}
		lastFileName = fileName
	}

	return lastFileName, err
}

func genKey(dirPath, fileName string) (key []byte) {
	key = []byte(dirPath)
	key = append(key, DIR_FILE_SEPARATOR)
	key = append(key, []byte(fileName)...)
	return key
}

func genDirectoryKeyPrefix(fullpath weed_util.FullPath, startFileName string) (keyPrefix []byte) {
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

func (store *EtcdStore) Shutdown() {
	store.client.Close()
}
