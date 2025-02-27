package cassandra2

import (
	"context"
	"encoding/base64"
	"fmt"
	"github.com/gocql/gocql"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func (store *Cassandra2Store) KvPut(ctx context.Context, key []byte, value []byte) (err error) {
	dir, name := genDirAndName(key)

	if err := store.session.Query(
		"INSERT INTO filemeta (dirhash,directory,name,meta) VALUES(?,?,?,?) USING TTL ? ",
		util.HashStringToLong(dir), dir, name, value, 0).Exec(); err != nil {
		return fmt.Errorf("kv insert: %s", err)
	}

	return nil
}

func (store *Cassandra2Store) KvGet(ctx context.Context, key []byte) (data []byte, err error) {
	dir, name := genDirAndName(key)

	if err := store.session.Query(
		"SELECT meta FROM filemeta WHERE dirhash=? AND directory=? AND name=?",
		util.HashStringToLong(dir), dir, name).Scan(&data); err != nil {
		if err != gocql.ErrNotFound {
			return nil, filer.ErrKvNotFound
		}
	}

	if len(data) == 0 {
		return nil, filer.ErrKvNotFound
	}

	return data, nil
}

func (store *Cassandra2Store) KvDelete(ctx context.Context, key []byte) (err error) {
	dir, name := genDirAndName(key)

	if err := store.session.Query(
		"DELETE FROM filemeta WHERE dirhash=? AND directory=? AND name=?",
		util.HashStringToLong(dir), dir, name).Exec(); err != nil {
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
