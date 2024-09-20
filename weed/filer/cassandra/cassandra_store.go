package cassandra

import (
	"context"
	"errors"
	"fmt"
	"github.com/gocql/gocql"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	filer.Stores = append(filer.Stores, &CassandraStore{})
}

type CassandraStore struct {
	cluster                 *gocql.ClusterConfig
	session                 *gocql.Session
	superLargeDirectoryHash map[string]string
}

func (store *CassandraStore) GetName() string {
	return "cassandra"
}

func (store *CassandraStore) Initialize(configuration util.Configuration, prefix string) (err error) {
	return store.initialize(
		configuration.GetString(prefix+"keyspace"),
		configuration.GetStringSlice(prefix+"hosts"),
		configuration.GetString(prefix+"username"),
		configuration.GetString(prefix+"password"),
		configuration.GetStringSlice(prefix+"superLargeDirectories"),
		configuration.GetString(prefix+"localDC"),
		configuration.GetInt(prefix+"connection_timeout_millisecond"),
	)
}

func (store *CassandraStore) isSuperLargeDirectory(dir string) (dirHash string, isSuperLargeDirectory bool) {
	dirHash, isSuperLargeDirectory = store.superLargeDirectoryHash[dir]
	return
}

func (store *CassandraStore) initialize(keyspace string, hosts []string, username string, password string, superLargeDirectories []string, localDC string, timeout int) (err error) {
	store.cluster = gocql.NewCluster(hosts...)
	if username != "" && password != "" {
		store.cluster.Authenticator = gocql.PasswordAuthenticator{Username: username, Password: password}
	}
	store.cluster.Keyspace = keyspace
	store.cluster.Timeout = time.Duration(timeout) * time.Millisecond
	glog.V(0).Infof("timeout = %d", timeout)
	fallback := gocql.RoundRobinHostPolicy()
	if localDC != "" {
		fallback = gocql.DCAwareRoundRobinPolicy(localDC)
	}
	store.cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(fallback)
	store.cluster.Consistency = gocql.LocalQuorum

	store.session, err = store.cluster.CreateSession()
	if err != nil {
		glog.V(0).Infof("Failed to open cassandra store, hosts %v, keyspace %s", hosts, keyspace)
	}

	// set directory hash
	store.superLargeDirectoryHash = make(map[string]string)
	existingHash := make(map[string]string)
	for _, dir := range superLargeDirectories {
		// adding dir hash to avoid duplicated names
		dirHash := util.Md5String([]byte(dir))[:4]
		store.superLargeDirectoryHash[dir] = dirHash
		if existingDir, found := existingHash[dirHash]; found {
			glog.Fatalf("directory %s has the same hash as %s", dir, existingDir)
		}
		existingHash[dirHash] = dir
	}
	return
}

func (store *CassandraStore) BeginTransaction(ctx context.Context) (context.Context, error) {
	return ctx, nil
}
func (store *CassandraStore) CommitTransaction(ctx context.Context) error {
	return nil
}
func (store *CassandraStore) RollbackTransaction(ctx context.Context) error {
	return nil
}

func (store *CassandraStore) InsertEntry(ctx context.Context, entry *filer.Entry) (err error) {

	dir, name := entry.FullPath.DirAndName()
	if dirHash, ok := store.isSuperLargeDirectory(dir); ok {
		dir, name = dirHash+name, ""
	}

	meta, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		return fmt.Errorf("encode %s: %s", entry.FullPath, err)
	}

	if len(entry.GetChunks()) > filer.CountEntryChunksForGzip {
		meta = util.MaybeGzipData(meta)
	}

	if err := store.session.Query(
		"INSERT INTO filemeta (directory,name,meta) VALUES(?,?,?) USING TTL ? ",
		dir, name, meta, entry.TtlSec).Exec(); err != nil {
		return fmt.Errorf("insert %s: %s", entry.FullPath, err)
	}

	return nil
}

func (store *CassandraStore) UpdateEntry(ctx context.Context, entry *filer.Entry) (err error) {

	return store.InsertEntry(ctx, entry)
}

func (store *CassandraStore) FindEntry(ctx context.Context, fullpath util.FullPath) (entry *filer.Entry, err error) {

	dir, name := fullpath.DirAndName()
	if dirHash, ok := store.isSuperLargeDirectory(dir); ok {
		dir, name = dirHash+name, ""
	}

	var data []byte
	if err := store.session.Query(
		"SELECT meta FROM filemeta WHERE directory=? AND name=?",
		dir, name).Scan(&data); err != nil {
		if errors.Is(err, gocql.ErrNotFound) {
			return nil, filer_pb.ErrNotFound
		}
		return nil, err
	}

	entry = &filer.Entry{
		FullPath: fullpath,
	}
	err = entry.DecodeAttributesAndChunks(util.MaybeDecompressData(data))
	if err != nil {
		return entry, fmt.Errorf("decode %s : %v", entry.FullPath, err)
	}

	return entry, nil
}

func (store *CassandraStore) DeleteEntry(ctx context.Context, fullpath util.FullPath) error {

	dir, name := fullpath.DirAndName()
	if dirHash, ok := store.isSuperLargeDirectory(dir); ok {
		dir, name = dirHash+name, ""
	}

	if err := store.session.Query(
		"DELETE FROM filemeta WHERE directory=? AND name=?",
		dir, name).Exec(); err != nil {
		return fmt.Errorf("delete %s : %v", fullpath, err)
	}

	return nil
}

func (store *CassandraStore) DeleteFolderChildren(ctx context.Context, fullpath util.FullPath) error {
	if _, ok := store.isSuperLargeDirectory(string(fullpath)); ok {
		return nil // filer.ErrUnsupportedSuperLargeDirectoryListing
	}

	if err := store.session.Query(
		"DELETE FROM filemeta WHERE directory=?",
		fullpath).Exec(); err != nil {
		return fmt.Errorf("delete %s : %v", fullpath, err)
	}

	return nil
}

func (store *CassandraStore) ListDirectoryPrefixedEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, prefix string, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {
	return lastFileName, filer.ErrUnsupportedListDirectoryPrefixed
}

func (store *CassandraStore) ListDirectoryEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {

	if _, ok := store.isSuperLargeDirectory(string(dirPath)); ok {
		return // nil, filer.ErrUnsupportedSuperLargeDirectoryListing
	}

	cqlStr := "SELECT NAME, meta FROM filemeta WHERE directory=? AND name>? ORDER BY NAME ASC LIMIT ?"
	if includeStartFile {
		cqlStr = "SELECT NAME, meta FROM filemeta WHERE directory=? AND name>=? ORDER BY NAME ASC LIMIT ?"
	}

	var data []byte
	var name string
	iter := store.session.Query(cqlStr, string(dirPath), startFileName, limit+1).Iter()
	for iter.Scan(&name, &data) {
		entry := &filer.Entry{
			FullPath: util.NewFullPath(string(dirPath), name),
		}
		lastFileName = name
		if decodeErr := entry.DecodeAttributesAndChunks(util.MaybeDecompressData(data)); decodeErr != nil {
			err = decodeErr
			glog.V(0).Infof("list %s : %v", entry.FullPath, err)
			break
		}
		if !eachEntryFunc(entry) {
			break
		}
	}
	if err := iter.Close(); err != nil {
		glog.V(0).Infof("list iterator close: %v", err)
	}

	return lastFileName, err
}

func (store *CassandraStore) Shutdown() {
	store.session.Close()
}
