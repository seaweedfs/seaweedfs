package cassandra

import (
	"context"
	"fmt"

	"github.com/gocql/gocql"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func init() {
	filer2.Stores = append(filer2.Stores, &CassandraStore{})
}

type CassandraStore struct {
	cluster *gocql.ClusterConfig
	session *gocql.Session
}

func (store *CassandraStore) GetName() string {
	return "cassandra"
}

func (store *CassandraStore) Initialize(configuration util.Configuration, prefix string) (err error) {
	return store.initialize(
		configuration.GetString(prefix+"keyspace"),
		configuration.GetStringSlice(prefix+"hosts"),
	)
}

func (store *CassandraStore) initialize(keyspace string, hosts []string) (err error) {
	store.cluster = gocql.NewCluster(hosts...)
	store.cluster.Keyspace = keyspace
	store.cluster.Consistency = gocql.LocalQuorum
	store.session, err = store.cluster.CreateSession()
	if err != nil {
		glog.V(0).Infof("Failed to open cassandra store, hosts %v, keyspace %s", hosts, keyspace)
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

func (store *CassandraStore) InsertEntry(ctx context.Context, entry *filer2.Entry) (err error) {

	dir, name := entry.FullPath.DirAndName()
	meta, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		return fmt.Errorf("encode %s: %s", entry.FullPath, err)
	}

	if err := store.session.Query(
		"INSERT INTO filemeta (directory,name,meta) VALUES(?,?,?) USING TTL ? ",
		dir, name, meta, entry.TtlSec).Exec(); err != nil {
		return fmt.Errorf("insert %s: %s", entry.FullPath, err)
	}

	return nil
}

func (store *CassandraStore) UpdateEntry(ctx context.Context, entry *filer2.Entry) (err error) {

	return store.InsertEntry(ctx, entry)
}

func (store *CassandraStore) FindEntry(ctx context.Context, fullpath util.FullPath) (entry *filer2.Entry, err error) {

	dir, name := fullpath.DirAndName()
	var data []byte
	if err := store.session.Query(
		"SELECT meta FROM filemeta WHERE directory=? AND name=?",
		dir, name).Consistency(gocql.One).Scan(&data); err != nil {
		if err != gocql.ErrNotFound {
			return nil, filer_pb.ErrNotFound
		}
	}

	if len(data) == 0 {
		return nil, filer_pb.ErrNotFound
	}

	entry = &filer2.Entry{
		FullPath: fullpath,
	}
	err = entry.DecodeAttributesAndChunks(data)
	if err != nil {
		return entry, fmt.Errorf("decode %s : %v", entry.FullPath, err)
	}

	return entry, nil
}

func (store *CassandraStore) DeleteEntry(ctx context.Context, fullpath util.FullPath) error {

	dir, name := fullpath.DirAndName()

	if err := store.session.Query(
		"DELETE FROM filemeta WHERE directory=? AND name=?",
		dir, name).Exec(); err != nil {
		return fmt.Errorf("delete %s : %v", fullpath, err)
	}

	return nil
}

func (store *CassandraStore) DeleteFolderChildren(ctx context.Context, fullpath util.FullPath) error {

	if err := store.session.Query(
		"DELETE FROM filemeta WHERE directory=?",
		fullpath).Exec(); err != nil {
		return fmt.Errorf("delete %s : %v", fullpath, err)
	}

	return nil
}

func (store *CassandraStore) ListDirectoryEntries(ctx context.Context, fullpath util.FullPath, startFileName string, inclusive bool,
	limit int) (entries []*filer2.Entry, err error) {

	cqlStr := "SELECT NAME, meta FROM filemeta WHERE directory=? AND name>? ORDER BY NAME ASC LIMIT ?"
	if inclusive {
		cqlStr = "SELECT NAME, meta FROM filemeta WHERE directory=? AND name>=? ORDER BY NAME ASC LIMIT ?"
	}

	var data []byte
	var name string
	iter := store.session.Query(cqlStr, string(fullpath), startFileName, limit).Iter()
	for iter.Scan(&name, &data) {
		entry := &filer2.Entry{
			FullPath: util.NewFullPath(string(fullpath), name),
		}
		if decodeErr := entry.DecodeAttributesAndChunks(data); decodeErr != nil {
			err = decodeErr
			glog.V(0).Infof("list %s : %v", entry.FullPath, err)
			break
		}
		entries = append(entries, entry)
	}
	if err := iter.Close(); err != nil {
		glog.V(0).Infof("list iterator close: %v", err)
	}

	return entries, err
}

func (store *CassandraStore) Shutdown() {
	store.session.Close()
}
