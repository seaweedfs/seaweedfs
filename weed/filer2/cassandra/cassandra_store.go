package cassandra

import (
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/gocql/gocql"
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

func (store *CassandraStore) Initialize(configuration util.Configuration) (err error) {
	return store.initialize(
		configuration.GetString("keyspace"),
		configuration.GetStringSlice("hosts"),
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

func (store *CassandraStore) InsertEntry(entry *filer2.Entry) (err error) {

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

func (store *CassandraStore) UpdateEntry(entry *filer2.Entry) (err error) {

	return store.InsertEntry(entry)
}

func (store *CassandraStore) FindEntry(fullpath filer2.FullPath) (entry *filer2.Entry, err error) {

	dir, name := fullpath.DirAndName()
	var data []byte
	if err := store.session.Query(
		"SELECT meta FROM filemeta WHERE directory=? AND name=?",
		dir, name).Consistency(gocql.One).Scan(&data); err != nil {
		if err != gocql.ErrNotFound {
			return nil, filer2.ErrNotFound
		}
	}

	if len(data) == 0 {
		return nil, fmt.Errorf("not found: %s", fullpath)
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

func (store *CassandraStore) DeleteEntry(fullpath filer2.FullPath) error {

	dir, name := fullpath.DirAndName()

	if err := store.session.Query(
		"DELETE FROM filemeta WHERE directory=? AND name=?",
		dir, name).Exec(); err != nil {
		return fmt.Errorf("delete %s : %v", fullpath, err)
	}

	return nil
}

func (store *CassandraStore) ListDirectoryEntries(fullpath filer2.FullPath, startFileName string, inclusive bool,
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
			FullPath: filer2.NewFullPath(string(fullpath), name),
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
