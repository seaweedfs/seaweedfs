package cassandra

import (
	"context"
	"encoding/base64"
	"fmt"
	"github.com/gocql/gocql"
	"github.com/seaweedfs/seaweedfs/weed/filer"
)

func (store *CassandraStore) KvPut(ctx context.Context, key []byte, value []byte) (err error) {
	dir, name := genDirAndName(key)

	if err := store.session.Query(
		"INSERT INTO filemeta (directory,name,meta) VALUES(?,?,?) USING TTL ? ",
		dir, name, value, 0).Exec(); err != nil {
		return fmt.Errorf("kv insert: %s", err)
	}

	return nil
}

func (store *CassandraStore) KvGet(ctx context.Context, key []byte) (data []byte, err error) {
	dir, name := genDirAndName(key)

	if err := store.session.Query(
		"SELECT meta FROM filemeta WHERE directory=? AND name=?",
		dir, name).Scan(&data); err != nil {
		if err != gocql.ErrNotFound {
			return nil, filer.ErrKvNotFound
		}
	}

	if len(data) == 0 {
		return nil, filer.ErrKvNotFound
	}

	return data, nil
}

func (store *CassandraStore) KvDelete(ctx context.Context, key []byte) (err error) {
	dir, name := genDirAndName(key)

	if err := store.session.Query(
		"DELETE FROM filemeta WHERE directory=? AND name=?",
		dir, name).Exec(); err != nil {
		return fmt.Errorf("kv delete: %v", err)
	}

	return nil
}

func genDirAndName(key []byte) (dir string, name string) {
	for len(key) < 8 {
		key = append(key, 0)
	}

	dir = base64.StdEncoding.EncodeToString(key[:8])
	name = base64.StdEncoding.EncodeToString(key[8:])

	return
}
