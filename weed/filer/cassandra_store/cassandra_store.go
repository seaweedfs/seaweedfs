package cassandra_store

import (
	"fmt"

	"github.com/chrislusf/seaweedfs/weed/glog"

	"github.com/gocql/gocql"
)

/*

Basically you need a table just like this:

CREATE TABLE seaweed_files (
   path varchar,
   fids list<varchar>,
   PRIMARY KEY (path)
);

Need to match flat_namespace.FlatNamespaceStore interface
	Put(fullFileName string, fid string) (err error)
	Get(fullFileName string) (fid string, err error)
	Delete(fullFileName string) (fid string, err error)

*/
type CassandraStore struct {
	cluster *gocql.ClusterConfig
	session *gocql.Session
}

func NewCassandraStore(keyspace string, hosts ...string) (c *CassandraStore, err error) {
	c = &CassandraStore{}
	c.cluster = gocql.NewCluster(hosts...)
	c.cluster.Keyspace = keyspace
	c.cluster.Consistency = gocql.Quorum
	c.session, err = c.cluster.CreateSession()
	if err != nil {
		glog.V(0).Infof("Failed to open cassandra store, hosts %v, keyspace %s", hosts, keyspace)
	}
	return
}

func (c *CassandraStore) Put(fullFileName string, fid string) (err error) {
	var input []string
	input = append(input, fid)
	if err := c.session.Query(
		`INSERT INTO seaweed_files (path, fids) VALUES (?, ?)`,
		fullFileName, input).Exec(); err != nil {
		glog.V(0).Infof("Failed to save file %s with id %s: %v", fullFileName, fid, err)
		return err
	}
	return nil
}
func (c *CassandraStore) Get(fullFileName string) (fid string, err error) {
	var output []string
	if err := c.session.Query(
		`select fids FROM seaweed_files WHERE path = ? LIMIT 1`,
		fullFileName).Consistency(gocql.One).Scan(&output); err != nil {
		if err != gocql.ErrNotFound {
			glog.V(0).Infof("Failed to find file %s: %v", fullFileName, fid, err)
		}
	}
	if len(output) == 0 {
		return "", fmt.Errorf("No file id found for %s", fullFileName)
	}
	return output[0], nil
}

// Currently the fid is not returned
func (c *CassandraStore) Delete(fullFileName string) (fid string, err error) {
	if err := c.session.Query(
		`DELETE FROM seaweed_files WHERE path = ?`,
		fullFileName).Exec(); err != nil {
		if err != gocql.ErrNotFound {
			glog.V(0).Infof("Failed to delete file %s: %v", fullFileName, err)
		}
		return "", err
	}
	return "", nil
}

func (c *CassandraStore) Close() {
	if c.session != nil {
		c.session.Close()
	}
}
