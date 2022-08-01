//go:build ydb
// +build ydb

package ydb

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/filer/abstract_sql"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func (store *YdbStore) KvPut(ctx context.Context, key []byte, value []byte) (err error) {
	dirStr, dirHash, name := abstract_sql.GenDirAndName(key)
	fileMeta := FileMeta{dirHash, name, dirStr, value}
	return store.DB.Table().Do(ctx, func(ctx context.Context, s table.Session) (err error) {
		_, _, err = s.Execute(ctx, rwTX, *withPragma(&store.tablePathPrefix, upsertQuery),
			fileMeta.queryParameters(0))
		if err != nil {
			return fmt.Errorf("kv put execute %s: %v", util.NewFullPath(dirStr, name).Name(), err)
		}
		return nil
	})
}

func (store *YdbStore) KvGet(ctx context.Context, key []byte) (value []byte, err error) {
	dirStr, dirHash, name := abstract_sql.GenDirAndName(key)
	valueFound := false
	err = store.DB.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		_, res, err := s.Execute(ctx, roTX, *withPragma(&store.tablePathPrefix, findQuery),
			table.NewQueryParameters(
				table.ValueParam("$dir_hash", types.Int64Value(dirHash)),
				table.ValueParam("$name", types.UTF8Value(name))))
		if err != nil {
			return fmt.Errorf("kv get execute %s: %v", util.NewFullPath(dirStr, name).Name(), err)
		}
		defer func() { _ = res.Close() }()
		if !res.NextResultSet(ctx) || !res.HasNextRow() {
			return nil
		}
		for res.NextRow() {
			if err := res.ScanNamed(named.OptionalWithDefault("meta", &value)); err != nil {
				return fmt.Errorf("scanNamed %s : %v", util.NewFullPath(dirStr, name).Name(), err)
			}
			valueFound = true
			return nil
		}
		return res.Err()
	})

	if !valueFound {
		return nil, filer.ErrKvNotFound
	}

	return value, nil
}

func (store *YdbStore) KvDelete(ctx context.Context, key []byte) (err error) {
	dirStr, dirHash, name := abstract_sql.GenDirAndName(key)
	return store.DB.Table().Do(ctx, func(ctx context.Context, s table.Session) (err error) {
		_, _, err = s.Execute(ctx, rwTX, *withPragma(&store.tablePathPrefix, deleteQuery),
			table.NewQueryParameters(
				table.ValueParam("$dir_hash", types.Int64Value(dirHash)),
				table.ValueParam("$name", types.UTF8Value(name))))
		if err != nil {
			return fmt.Errorf("kv delete %s: %v", util.NewFullPath(dirStr, name).Name(), err)
		}
		return nil
	})

}
