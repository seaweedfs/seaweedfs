package consul

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	consul "github.com/hashicorp/consul/api"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	weed_util "github.com/seaweedfs/seaweedfs/weed/util"
)

const (
	DIR_FILE_SEPARATOR = "/"
)

func init() {
	filer.Stores = append(filer.Stores, &ConsulStore{})
}

type ConsulStore struct {
	client          *consul.Client
	consulKeyPrefix string
}

func (store *ConsulStore) GetName() string {
	return "consul"
}

func (store *ConsulStore) Initialize(configuration weed_util.Configuration, prefix string) (err error) {
	addr := configuration.GetString(prefix + "addr")
	if addr == "" {
		addr = "localhost:8500"
	}

	token := configuration.GetString(prefix + "token")
	store.consulKeyPrefix = configuration.GetString(prefix + "prefix")
	if store.consulKeyPrefix == "" {
		store.consulKeyPrefix = "seaweedfs"
	}

	timeout := configuration.GetString(prefix + "timeout")
	if timeout == "" {
		timeout = "3s"
	}

	caPath := configuration.GetString(prefix + "ca_path")
	return store.initialize(addr, token, caPath, timeout)
}

func (store *ConsulStore) initialize(addr, token, caPath, timeout string) (err error) {
	glog.Infof("filer store consul: %s", addr)

	to, err := time.ParseDuration(timeout)
	if err != nil {
		return fmt.Errorf("parse timeout %s: %s", timeout, err)
	}

	if !strings.Contains(addr, "://") {
		addr = "http://" + addr
	}
	u, err := url.Parse(addr)
	if err != nil {
		return fmt.Errorf("parse %s: %s", addr, err)
	}
	cfg := consul.DefaultConfig()
	cfg.Address = u.Host
	cfg.Scheme = u.Scheme
	cfg.Token = token

	defaults := consul.DefaultConfig()
	if caPath != "" {
		cfg.TLSConfig = defaults.TLSConfig
		cfg.TLSConfig.CAFile = caPath
	}

	cfg.HttpClient = http.DefaultClient
	cfg.HttpClient.Timeout = to

	store.client, err = consul.NewClient(cfg)
	return
}

func (store *ConsulStore) BeginTransaction(ctx context.Context) (context.Context, error) {
	return ctx, nil
}
func (store *ConsulStore) CommitTransaction(ctx context.Context) error {
	return nil
}
func (store *ConsulStore) RollbackTransaction(ctx context.Context) error {
	return nil
}

func (store *ConsulStore) InsertEntry(ctx context.Context, entry *filer.Entry) (err error) {
	key := store.key(string(entry.FullPath))

	meta, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		return fmt.Errorf("encoding %s %+v: %v", entry.FullPath, entry.Attr, err)
	}

	if len(entry.GetChunks()) > filer.CountEntryChunksForGzip {
		meta = weed_util.MaybeGzipData(meta)
	}

	data := &consul.KVPair{
		Key:   key,
		Value: meta,
	}

	if _, err := store.client.KV().Put(data, &consul.WriteOptions{}); err != nil {
		return fmt.Errorf("persisting %s : %v", entry.FullPath, err)
	}

	return nil
}

func (store *ConsulStore) UpdateEntry(ctx context.Context, entry *filer.Entry) (err error) {
	return store.InsertEntry(ctx, entry)
}

func (store *ConsulStore) FindEntry(ctx context.Context, fullpath weed_util.FullPath) (entry *filer.Entry, err error) {
	key := store.genKey(fullpath.DirAndName())

	resp, _, err := store.client.KV().List(key, &consul.QueryOptions{})
	if err != nil {
		return nil, fmt.Errorf("get %s : %v", fullpath, err)
	}

	if len(resp) == 0 {
		return nil, filer_pb.ErrNotFound
	}

	entry = &filer.Entry{
		FullPath: fullpath,
	}
	err = entry.DecodeAttributesAndChunks(weed_util.MaybeDecompressData(resp[0].Value))
	if err != nil {
		return entry, fmt.Errorf("decode %s : %v", entry.FullPath, err)
	}

	return entry, nil
}

func (store *ConsulStore) DeleteEntry(ctx context.Context, fullpath weed_util.FullPath) (err error) {
	key := store.genKey(fullpath.DirAndName())

	if _, err := store.client.KV().Delete(key, &consul.WriteOptions{}); err != nil {
		return fmt.Errorf("delete %s : %v", fullpath, err)
	}

	return nil
}

func (store *ConsulStore) DeleteFolderChildren(ctx context.Context, fullpath weed_util.FullPath) (err error) {
	directoryPrefix := store.genDirectoryKeyPrefix(fullpath, "")

	if _, err := store.client.KV().Delete(directoryPrefix, &consul.WriteOptions{}); err != nil {
		return fmt.Errorf("deleteFolderChildren %s : %v", fullpath, err)
	}

	return nil
}

func (store *ConsulStore) ListDirectoryPrefixedEntries(ctx context.Context, dirPath weed_util.FullPath, startFileName string, includeStartFile bool, limit int64, prefix string, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {
	directoryPrefix := store.genDirectoryKeyPrefix(dirPath, prefix)
	lastFileName = directoryPrefix

	resp, _, err := store.client.KV().List(directoryPrefix, &consul.QueryOptions{})
	if err != nil {
		return lastFileName, fmt.Errorf("list %s : %v", dirPath, err)
	}

	for _, kv := range resp {
		fileName := store.getNameFromKey(kv.Key)
		if fileName == "" {
			continue
		}
		if fileName < startFileName && !includeStartFile {
			continue
		}
		limit--
		if limit < 0 {
			lastFileName = fileName
			break
		}
		entry := &filer.Entry{
			FullPath: weed_util.NewFullPath(string(dirPath), fileName),
		}
		if decodeErr := entry.DecodeAttributesAndChunks(weed_util.MaybeDecompressData(kv.Value)); decodeErr != nil {
			err = decodeErr
			break
		}
		if !eachEntryFunc(entry) {
			break
		}
		lastFileName = fileName
	}

	return lastFileName, err
}

func (store *ConsulStore) ListDirectoryEntries(ctx context.Context, dirPath weed_util.FullPath, startFileName string, includeStartFile bool, limit int64, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {
	directoryPrefix := store.genDirectoryKeyPrefix(dirPath, "/")
	lastFileName = directoryPrefix

	resp, _, err := store.client.KV().List(directoryPrefix, &consul.QueryOptions{})
	if err != nil {
		return lastFileName, fmt.Errorf("list %s : %v", dirPath, err)
	}

	for _, kv := range resp {
		fileName := store.getNameFromKey(kv.Key)
		if fileName == "" {
			continue
		}
		if fileName < startFileName && !includeStartFile {
			continue
		}
		limit--
		if limit < 0 {
			lastFileName = fileName
			break
		}
		entry := &filer.Entry{
			FullPath: weed_util.NewFullPath(string(dirPath), fileName),
		}
		if decodeErr := entry.DecodeAttributesAndChunks(weed_util.MaybeDecompressData(kv.Value)); decodeErr != nil {
			err = decodeErr
			break
		}
		if !eachEntryFunc(entry) {
			break
		}
		lastFileName = fileName
	}

	return lastFileName, err
}

func (store *ConsulStore) key(key string) string {
	if store.consulKeyPrefix == "" {
		return key
	}
	return strings.TrimSuffix(store.consulKeyPrefix+DIR_FILE_SEPARATOR+strings.TrimPrefix(key, DIR_FILE_SEPARATOR), DIR_FILE_SEPARATOR)
}

func (store *ConsulStore) genKey(dirPath, fileName string) (key string) {
	if fileName == "" {
		return store.key(dirPath)
	}
	return store.key(dirPath + DIR_FILE_SEPARATOR + fileName)
}

func (store *ConsulStore) genDirectoryKeyPrefix(fullpath weed_util.FullPath, startFileName string) (keyPrefix string) {
	keyPrefix = string(fullpath)
	keyPrefix += DIR_FILE_SEPARATOR
	if startFileName != "" {
		keyPrefix += startFileName
	}
	return store.key(keyPrefix)
}

func (store *ConsulStore) getNameFromKey(key string) string {
	comps := strings.Split(key, DIR_FILE_SEPARATOR)
	return comps[len(comps)-1]
}

func (store *ConsulStore) Shutdown() {}
