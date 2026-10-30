package keeper

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/go-zookeeper/zk"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	weed_util "github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	filer.Stores = append(filer.Stores, &KeeperStore{})
}

// KeeperStore stores filer metadata in ClickHouse Keeper (ZooKeeper-compatible).
// Directory listing uses getChildren + client-side sort/pagination until Keeper
// exposes a server-side range list API.
type KeeperStore struct {
	conn           *zk.Conn
	keyPrefix      string
	kvPrefix       string
	sessionTimeout time.Duration
}

func (store *KeeperStore) GetName() string {
	return "keeper"
}

func (store *KeeperStore) Initialize(configuration weed_util.Configuration, prefix string) error {
	configuration.SetDefault(prefix+"servers", "localhost:9181")
	configuration.SetDefault(prefix+"session_timeout", "10s")
	configuration.SetDefault(prefix+"key_prefix", "/seaweedfs/filer")

	servers := configuration.GetString(prefix + "servers")
	keyPrefix := configuration.GetString(prefix + "key_prefix")
	timeoutStr := configuration.GetString(prefix + "session_timeout")
	timeout, err := time.ParseDuration(timeoutStr)
	if err != nil {
		return fmt.Errorf("parse keeper session_timeout: %w", err)
	}

	return store.initialize(servers, keyPrefix, timeout)
}

func (store *KeeperStore) initialize(servers, keyPrefix string, sessionTimeout time.Duration) error {
	glog.Infof("filer store keeper: %s prefix=%s", servers, keyPrefix)

	keyPrefix = strings.TrimRight(keyPrefix, "/")
	if keyPrefix == "" {
		keyPrefix = "/seaweedfs/filer"
	}
	if !strings.HasPrefix(keyPrefix, "/") {
		keyPrefix = "/" + keyPrefix
	}

	conn, events, err := zk.Connect(strings.Split(servers, ","), sessionTimeout)
	if err != nil {
		return fmt.Errorf("connect to keeper %s: %w", servers, err)
	}
	go func() {
		for range events {
		}
	}()

	store.conn = conn
	store.keyPrefix = keyPrefix
	store.kvPrefix = keyPrefix + "_kv"
	store.sessionTimeout = sessionTimeout

	if err := store.ensurePath(store.keyPrefix); err != nil {
		conn.Close()
		store.conn = nil
		return fmt.Errorf("ensure key_prefix %s: %w", store.keyPrefix, err)
	}
	if err := store.ensurePath(store.kvPrefix); err != nil {
		conn.Close()
		store.conn = nil
		return fmt.Errorf("ensure kv_prefix %s: %w", store.kvPrefix, err)
	}

	if _, _, err := conn.Get(store.keyPrefix); err != nil {
		conn.Close()
		store.conn = nil
		return fmt.Errorf("check keeper connection: %w", err)
	}

	glog.V(0).Infof("connection to keeper has been verified; key_prefix=%s", store.keyPrefix)
	return nil
}

func (store *KeeperStore) BeginTransaction(ctx context.Context) (context.Context, error) {
	return ctx, nil
}

func (store *KeeperStore) CommitTransaction(ctx context.Context) error {
	return nil
}

func (store *KeeperStore) RollbackTransaction(ctx context.Context) error {
	return nil
}

func (store *KeeperStore) InsertEntry(ctx context.Context, entry *filer.Entry) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}

	zkPath := store.entryPath(entry.FullPath)
	meta, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		return fmt.Errorf("encoding %s %+v: %v", entry.FullPath, entry.Attr, err)
	}
	if len(entry.GetChunks()) > filer.CountEntryChunksForGzip {
		meta = weed_util.MaybeGzipData(meta)
	}

	return store.createOrSet(zkPath, meta)
}

func (store *KeeperStore) UpdateEntry(ctx context.Context, entry *filer.Entry) (err error) {
	return store.InsertEntry(ctx, entry)
}

func (store *KeeperStore) FindEntry(ctx context.Context, fullpath weed_util.FullPath) (entry *filer.Entry, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	zkPath := store.entryPath(fullpath)
	data, _, err := store.conn.Get(zkPath)
	if err != nil {
		if err == zk.ErrNoNode {
			return nil, filer_pb.ErrNotFound
		}
		return nil, fmt.Errorf("get %s : %v", fullpath, err)
	}
	// Placeholder nodes from ensurePath have empty data until filer writes an entry.
	if len(data) == 0 {
		return nil, filer_pb.ErrNotFound
	}

	entry = &filer.Entry{FullPath: fullpath}
	if err = entry.DecodeAttributesAndChunks(weed_util.MaybeDecompressData(data)); err != nil {
		return entry, fmt.Errorf("decode %s : %v", entry.FullPath, err)
	}
	return entry, nil
}

func (store *KeeperStore) DeleteEntry(ctx context.Context, fullpath weed_util.FullPath) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}

	zkPath := store.entryPath(fullpath)
	err = store.conn.Delete(zkPath, -1)
	if err == zk.ErrNoNode {
		return nil
	}
	if err != nil {
		return fmt.Errorf("delete %s : %v", fullpath, err)
	}
	return nil
}

func (store *KeeperStore) DeleteFolderChildren(ctx context.Context, fullpath weed_util.FullPath) (err error) {
	if err := ctx.Err(); err != nil {
		return err
	}

	zkPath := store.entryPath(fullpath)
	children, _, err := store.conn.Children(zkPath)
	if err != nil {
		if err == zk.ErrNoNode {
			return nil
		}
		return fmt.Errorf("deleteFolderChildren list %s : %v", fullpath, err)
	}
	for _, name := range children {
		if err := store.deleteRecursive(path.Join(zkPath, name)); err != nil {
			return fmt.Errorf("deleteFolderChildren %s : %v", fullpath, err)
		}
	}
	return nil
}

func (store *KeeperStore) ListDirectoryPrefixedEntries(ctx context.Context, dirPath weed_util.FullPath, startFileName string, includeStartFile bool, limit int64, prefix string, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}

	zkPath := store.entryPath(dirPath)
	children, _, err := store.conn.Children(zkPath)
	if err != nil {
		if err == zk.ErrNoNode {
			return "", nil
		}
		return "", fmt.Errorf("list %s : %v", dirPath, err)
	}

	sort.Strings(children)

	for _, fileName := range children {
		if prefix != "" && !strings.HasPrefix(fileName, prefix) {
			continue
		}
		if startFileName != "" {
			if includeStartFile {
				if fileName < startFileName {
					continue
				}
			} else if fileName <= startFileName {
				continue
			}
		}

		limit--
		if limit < 0 {
			break
		}

		childPath := weed_util.NewFullPath(string(dirPath), fileName)
		entry, findErr := store.FindEntry(ctx, childPath)
		if findErr != nil {
			if findErr == filer_pb.ErrNotFound {
				continue
			}
			return lastFileName, fmt.Errorf("list %s : %v", childPath, findErr)
		}

		ok, eachErr := eachEntryFunc(entry)
		if eachErr != nil {
			return lastFileName, fmt.Errorf("failed to process eachEntryFunc: %w", eachErr)
		}
		lastFileName = fileName
		if !ok {
			break
		}
	}

	return lastFileName, nil
}

func (store *KeeperStore) ListDirectoryEntries(ctx context.Context, dirPath weed_util.FullPath, startFileName string, includeStartFile bool, limit int64, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {
	return store.ListDirectoryPrefixedEntries(ctx, dirPath, startFileName, includeStartFile, limit, "", eachEntryFunc)
}

func (store *KeeperStore) Shutdown() {
	if store.conn != nil {
		store.conn.Close()
		store.conn = nil
	}
}

func (store *KeeperStore) entryPath(fullpath weed_util.FullPath) string {
	p := string(fullpath)
	if p == "/" || p == "" {
		return store.keyPrefix
	}
	return path.Join(store.keyPrefix, strings.TrimPrefix(p, "/"))
}

func (store *KeeperStore) createOrSet(zkPath string, data []byte) error {
	_, err := store.conn.Create(zkPath, data, 0, zk.WorldACL(zk.PermAll))
	if err == nil {
		return nil
	}
	if err == zk.ErrNodeExists {
		_, setErr := store.conn.Set(zkPath, data, -1)
		if setErr != nil {
			return fmt.Errorf("set %s : %v", zkPath, setErr)
		}
		return nil
	}
	if err == zk.ErrNoNode {
		if ensureErr := store.ensurePath(path.Dir(zkPath)); ensureErr != nil {
			return fmt.Errorf("ensure parent of %s : %v", zkPath, ensureErr)
		}
		_, err = store.conn.Create(zkPath, data, 0, zk.WorldACL(zk.PermAll))
		if err == zk.ErrNodeExists {
			_, setErr := store.conn.Set(zkPath, data, -1)
			if setErr != nil {
				return fmt.Errorf("set %s : %v", zkPath, setErr)
			}
			return nil
		}
		if err != nil {
			return fmt.Errorf("create %s : %v", zkPath, err)
		}
		return nil
	}
	return fmt.Errorf("create %s : %v", zkPath, err)
}

func (store *KeeperStore) ensurePath(zkPath string) error {
	if zkPath == "" || zkPath == "/" {
		return nil
	}
	parts := strings.Split(strings.Trim(zkPath, "/"), "/")
	current := ""
	for _, part := range parts {
		current += "/" + part
		exists, _, err := store.conn.Exists(current)
		if err != nil {
			return err
		}
		if exists {
			continue
		}
		_, err = store.conn.Create(current, []byte{}, 0, zk.WorldACL(zk.PermAll))
		if err != nil && err != zk.ErrNodeExists {
			return err
		}
	}
	return nil
}

func (store *KeeperStore) deleteRecursive(zkPath string) error {
	children, _, err := store.conn.Children(zkPath)
	if err != nil {
		if err == zk.ErrNoNode {
			return nil
		}
		return err
	}
	for _, name := range children {
		if err := store.deleteRecursive(path.Join(zkPath, name)); err != nil {
			return err
		}
	}
	err = store.conn.Delete(zkPath, -1)
	if err == zk.ErrNoNode {
		return nil
	}
	return err
}
