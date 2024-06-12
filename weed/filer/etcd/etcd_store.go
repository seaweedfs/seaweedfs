package etcd

import (
	"context"
	"crypto/tls"
	"fmt"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	"strings"
	"time"

	"go.etcd.io/etcd/client/v3"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	weed_util "github.com/seaweedfs/seaweedfs/weed/util"
)

const (
	DIR_FILE_SEPARATOR = byte(0x00)
)

func init() {
	filer.Stores = append(filer.Stores, &EtcdStore{})
}

type EtcdStore struct {
	client        *clientv3.Client
	etcdKeyPrefix string
	timeout       time.Duration
}

func (store *EtcdStore) GetName() string {
	return "etcd"
}

func (store *EtcdStore) Initialize(configuration weed_util.Configuration, prefix string) error {
	configuration.SetDefault(prefix+"servers", "localhost:2379")
	configuration.SetDefault(prefix+"timeout", "3s")

	servers := configuration.GetString(prefix + "servers")

	username := configuration.GetString(prefix + "username")
	password := configuration.GetString(prefix + "password")
	store.etcdKeyPrefix = configuration.GetString(prefix + "key_prefix")

	timeoutStr := configuration.GetString(prefix + "timeout")
	timeout, err := time.ParseDuration(timeoutStr)
	if err != nil {
		return fmt.Errorf("parse etcd store timeout: %v", err)
	}
	store.timeout = timeout

	certFile := configuration.GetString(prefix + "tls_client_crt_file")
	keyFile := configuration.GetString(prefix + "tls_client_key_file")
	caFile := configuration.GetString(prefix + "tls_ca_file")

	var tlsConfig *tls.Config
	if caFile != "" {
		tlsInfo := transport.TLSInfo{
			CertFile:      certFile,
			KeyFile:       keyFile,
			TrustedCAFile: caFile,
		}
		var err error
		tlsConfig, err = tlsInfo.ClientConfig()
		if err != nil {
			return fmt.Errorf("TLS client configuration error: %v", err)
		}
	}

	return store.initialize(servers, username, password, store.timeout, tlsConfig)
}

func (store *EtcdStore) initialize(servers, username, password string, timeout time.Duration, tlsConfig *tls.Config) error {
	glog.Infof("filer store etcd: %s", servers)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(servers, ","),
		Username:    username,
		Password:    password,
		DialTimeout: timeout,
		TLS:         tlsConfig,
	})
	if err != nil {
		return fmt.Errorf("connect to etcd %s: %s", servers, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), store.timeout)
	defer cancel()

	resp, err := client.Status(ctx, client.Endpoints()[0])
	if err != nil {
		client.Close()
		return fmt.Errorf("error checking etcd connection: %s", err)
	}

	glog.V(0).Infof("сonnection to etcd has been successfully verified. etcd version: %s", resp.Version)
	store.client = client

	return nil
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

	if len(entry.GetChunks()) > filer.CountEntryChunksForGzip {
		meta = weed_util.MaybeGzipData(meta)
	}

	if _, err := store.client.Put(ctx, store.etcdKeyPrefix+string(key), string(meta)); err != nil {
		return fmt.Errorf("persisting %s : %v", entry.FullPath, err)
	}

	return nil
}

func (store *EtcdStore) UpdateEntry(ctx context.Context, entry *filer.Entry) (err error) {
	return store.InsertEntry(ctx, entry)
}

func (store *EtcdStore) FindEntry(ctx context.Context, fullpath weed_util.FullPath) (entry *filer.Entry, err error) {
	key := genKey(fullpath.DirAndName())

	resp, err := store.client.Get(ctx, store.etcdKeyPrefix+string(key))
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

	if _, err := store.client.Delete(ctx, store.etcdKeyPrefix+string(key)); err != nil {
		return fmt.Errorf("delete %s : %v", fullpath, err)
	}

	return nil
}

func (store *EtcdStore) DeleteFolderChildren(ctx context.Context, fullpath weed_util.FullPath) (err error) {
	directoryPrefix := genDirectoryKeyPrefix(fullpath, "")

	if _, err := store.client.Delete(ctx, store.etcdKeyPrefix+string(directoryPrefix), clientv3.WithPrefix()); err != nil {
		return fmt.Errorf("deleteFolderChildren %s : %v", fullpath, err)
	}

	return nil
}

func (store *EtcdStore) ListDirectoryPrefixedEntries(ctx context.Context, dirPath weed_util.FullPath, startFileName string, includeStartFile bool, limit int64, prefix string, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {
	directoryPrefix := genDirectoryKeyPrefix(dirPath, prefix)
	lastFileStart := directoryPrefix
	if startFileName != "" {
		lastFileStart = genDirectoryKeyPrefix(dirPath, startFileName)
	}

	resp, err := store.client.Get(ctx, store.etcdKeyPrefix+string(lastFileStart),
		clientv3.WithRange(clientv3.GetPrefixRangeEnd(store.etcdKeyPrefix+string(directoryPrefix))),
		clientv3.WithLimit(limit+1))
	if err != nil {
		return lastFileName, fmt.Errorf("list %s : %v", dirPath, err)
	}

	for _, kv := range resp.Kvs {
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

func (store *EtcdStore) ListDirectoryEntries(ctx context.Context, dirPath weed_util.FullPath, startFileName string, includeStartFile bool, limit int64, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {
	return store.ListDirectoryPrefixedEntries(ctx, dirPath, startFileName, includeStartFile, limit, "", eachEntryFunc)
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
