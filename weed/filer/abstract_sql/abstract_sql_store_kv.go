package abstract_sql

import (
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/util"
	"strings"
)

func (store *AbstractSqlStore) KvPut(ctx context.Context, key []byte, value []byte) (err error) {

	dirStr, dirHash, name := genDirAndName(key)

	res, err := store.getTxOrDB(ctx).ExecContext(ctx, store.SqlInsert, dirHash, name, dirStr, value)
	if err == nil {
		return
	}

	if !strings.Contains(strings.ToLower(err.Error()), "duplicate") {
		// return fmt.Errorf("kv insert: %s", err)
		// skip this since the error can be in a different language
	}

	// now the insert failed possibly due to duplication constraints
	glog.V(1).Infof("kv insert falls back to update: %s", err)

	res, err = store.getTxOrDB(ctx).ExecContext(ctx, store.SqlUpdate, value, dirHash, name, dirStr)
	if err != nil {
		return fmt.Errorf("kv upsert: %s", err)
	}

	_, err = res.RowsAffected()
	if err != nil  {
		return fmt.Errorf("kv upsert no rows affected: %s", err)
	}
	return nil

}

func (store *AbstractSqlStore) KvGet(ctx context.Context, key []byte) (value []byte, err error) {

	dirStr, dirHash, name := genDirAndName(key)
	row := store.getTxOrDB(ctx).QueryRowContext(ctx, store.SqlFind, dirHash, name, dirStr)

	err = row.Scan(&value)

	if err == sql.ErrNoRows {
		return nil, filer.ErrKvNotFound
	}

	if err != nil {
		return nil, fmt.Errorf("kv get: %v", err)
	}

	return
}

func (store *AbstractSqlStore) KvDelete(ctx context.Context, key []byte) (err error) {

	dirStr, dirHash, name := genDirAndName(key)

	res, err := store.getTxOrDB(ctx).ExecContext(ctx, store.SqlDelete, dirHash, name, dirStr)
	if err != nil {
		return fmt.Errorf("kv delete: %s", err)
	}

	_, err = res.RowsAffected()
	if err != nil {
		return fmt.Errorf("kv delete no rows affected: %s", err)
	}

	return nil

}

func genDirAndName(key []byte) (dirStr string, dirHash int64, name string) {
	for len(key) < 8 {
		key = append(key, 0)
	}

	dirHash = int64(util.BytesToUint64(key[:8]))
	dirStr = base64.StdEncoding.EncodeToString(key[:8])
	name = base64.StdEncoding.EncodeToString(key[8:])

	return
}
