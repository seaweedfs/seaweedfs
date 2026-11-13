package etcd

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/transport"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	weed_util "github.com/seaweedfs/seaweedfs/weed/util"
)

const (
	DIR_FILE_SEPARATOR = byte(0x00)

	// keepaliveTime is how long a connection with calls in flight may go
	// without receiving anything before the client pings the member. It is
	// gRPC's minimum: grpc.WithKeepaliveParams raises anything lower to 10s.
	keepaliveTime = 10 * time.Second
)

func init() {
	filer.Stores = append(filer.Stores, &EtcdStore{})
}

type EtcdStore struct {
	client           *clientv3.Client
	etcdKeyPrefix    string
	timeout          time.Duration
	requestTimeout   time.Duration
	keepaliveTimeout time.Duration
}

func (store *EtcdStore) GetName() string {
	return "etcd"
}

func (store *EtcdStore) Initialize(configuration weed_util.Configuration, prefix string) error {
	configuration.SetDefault(prefix+"servers", "localhost:2379")
	configuration.SetDefault(prefix+"timeout", "3s")
	configuration.SetDefault(prefix+"request_timeout", "3s")
	configuration.SetDefault(prefix+"keepalive_timeout", "3s")

	servers := configuration.GetString(prefix + "servers")

	username := configuration.GetString(prefix + "username")
	password := configuration.GetString(prefix + "password")
	store.etcdKeyPrefix = configuration.GetString(prefix + "key_prefix")

	timeoutStr := configuration.GetString(prefix + "timeout")
	timeout, err := time.ParseDuration(timeoutStr)
	if err != nil {
		return fmt.Errorf("parse etcd store timeout: %w", err)
	}
	store.timeout = timeout

	if store.requestTimeout, err = time.ParseDuration(configuration.GetString(prefix + "request_timeout")); err != nil {
		return fmt.Errorf("parse etcd store request_timeout: %w", err)
	}
	if store.keepaliveTimeout, err = time.ParseDuration(configuration.GetString(prefix + "keepalive_timeout")); err != nil {
		return fmt.Errorf("parse etcd store keepalive_timeout: %w", err)
	}

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
			return fmt.Errorf("TLS client configuration error: %w", err)
		}
	}

	return store.initialize(servers, username, password, store.timeout, tlsConfig)
}

func (store *EtcdStore) initialize(servers, username, password string, timeout time.Duration, tlsConfig *tls.Config) error {
	glog.Infof("filer store etcd: %s", servers)

	config := clientv3.Config{
		Endpoints:   strings.Split(servers, ","),
		Username:    username,
		Password:    password,
		DialTimeout: timeout,
		TLS:         tlsConfig,
	}
	// Keepalive closes the connection to a member that stops answering, so the
	// balancer stops sending requests to it. gRPC also sets TCP_USER_TIMEOUT to
	// keepaliveTimeout on Linux: a connection whose sent data stays
	// unacknowledged that long is closed by the kernel, which catches a dead
	// node or a network partition without waiting for keepaliveTime.
	if store.keepaliveTimeout > 0 {
		config.DialKeepAliveTime = keepaliveTime
		config.DialKeepAliveTimeout = store.keepaliveTimeout
	}
	client, err := clientv3.New(config)
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

	glog.V(0).InfofCtx(ctx, "сonnection to etcd has been successfully verified. etcd version: %s", resp.Version)
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

// withTimeout bounds one etcd call by the store's request timeout, unless the
// caller has set a deadline of its own. A call without a deadline that is balanced
// onto a member that stops answering waits until keepalive closes the
// connection, or forever if keepalive is off.
func (store *EtcdStore) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, hasDeadline := ctx.Deadline(); hasDeadline || store.requestTimeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, store.requestTimeout)
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

	// No request deadline here: a Put that times out on the client may still
	// have been committed, and callers such as the filer's upload handler treat
	// the error as a failed write and delete the chunks the saved entry points
	// at. etcd's own request timeout still ends a Put that a live member cannot
	// commit, and keepalive ends one on a member that goes silent.
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

	opCtx, cancel := store.withTimeout(ctx)
	defer cancel()
	resp, err := store.client.Get(opCtx, store.etcdKeyPrefix+string(key), atScanRevision(ctx)...)
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

	opCtx, cancel := store.withTimeout(ctx)
	defer cancel()
	if _, err := store.client.Delete(opCtx, store.etcdKeyPrefix+string(key)); err != nil {
		return fmt.Errorf("delete %s : %v", fullpath, err)
	}

	return nil
}

// DeleteFolderChildren removes every entry under fullpath in one DeleteRange.
// It runs without the store's timeout: the range grows with the directory
// (about 3.5 s for 600,000 entries), and keepalive still fails the call if the
// member stops answering.
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
	if startFileName > prefix {
		lastFileStart = genDirectoryKeyPrefix(dirPath, startFileName)
	}

	opCtx, cancel := store.withTimeout(ctx)
	defer cancel()
	resp, err := store.client.Get(opCtx, store.etcdKeyPrefix+string(lastFileStart),
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
