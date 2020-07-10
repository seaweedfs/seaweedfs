package etcd

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.etcd.io/etcd/clientv3"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	weed_util "github.com/chrislusf/seaweedfs/weed/util"
)

const (
	DIR_FILE_SEPARATOR = byte(0x00)
)

func init() {
	filer2.Stores = append(filer2.Stores, &EtcdStore{})
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

func (store *EtcdStore) InsertEntry(ctx context.Context, entry *filer2.Entry) (err error) {
	key := genKey(entry.DirAndName())

	value, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		return fmt.Errorf("encoding %s %+v: %v", entry.FullPath, entry.Attr, err)
	}

	if _, err := store.client.Put(ctx, string(key), string(value)); err != nil {
		return fmt.Errorf("persisting %s : %v", entry.FullPath, err)
	}

	return nil
}

func (store *EtcdStore) UpdateEntry(ctx context.Context, entry *filer2.Entry) (err error) {
	return store.InsertEntry(ctx, entry)
}

func (store *EtcdStore) FindEntry(ctx context.Context, fullpath weed_util.FullPath) (entry *filer2.Entry, err error) {
	key := genKey(fullpath.DirAndName())

	resp, err := store.client.Get(ctx, string(key))
	if err != nil {
		return nil, fmt.Errorf("get %s : %v", entry.FullPath, err)
	}

	if len(resp.Kvs) == 0 {
		return nil, filer_pb.ErrNotFound
	}

	entry = &filer2.Entry{
		FullPath: fullpath,
	}
	err = entry.DecodeAttributesAndChunks(resp.Kvs[0].Value)
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

func (store *EtcdStore) ListDirectoryEntries(
	ctx context.Context, fullpath weed_util.FullPath, startFileName string, inclusive bool, limit int,
) (entries []*filer2.Entry, err error) {
	directoryPrefix := genDirectoryKeyPrefix(fullpath, "")

	resp, err := store.client.Get(ctx, string(directoryPrefix),
		clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))
	if err != nil {
		return nil, fmt.Errorf("list %s : %v", fullpath, err)
	}

	for _, kv := range resp.Kvs {
		fileName := getNameFromKey(kv.Key)
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
			FullPath: weed_util.NewFullPath(string(fullpath), fileName),
		}
		if decodeErr := entry.DecodeAttributesAndChunks(kv.Value); decodeErr != nil {
			err = decodeErr
			glog.V(0).Infof("list %s : %v", entry.FullPath, err)
			break
		}
		entries = append(entries, entry)
	}

	return entries, err
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
