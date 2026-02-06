//go:build ydb
// +build ydb

package ydb

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/filer/abstract_sql"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func (store *YdbStore) KvPut(ctx context.Context, key []byte, value []byte) (err error) {
	dirStr, dirHash, name := abstract_sql.GenDirAndName(key)
	fileMeta := FileMeta{dirHash, name, dirStr, value}
	return store.DB.Query().Do(ctx, func(ctx context.Context, s query.Session) (err error) {
		_, err = s.Query(ctx, *withPragma(&store.tablePathPrefix, upsertQuery),
			query.WithParameters(fileMeta.queryParameters(0)), rwQC)
		if err != nil {
			return fmt.Errorf("kv put execute %s: %v", util.NewFullPath(dirStr, name).Name(), err)
		}
		return nil
	}, query.WithIdempotent())
}

func (store *YdbStore) KvGet(ctx context.Context, key []byte) (value []byte, err error) {
	dirStr, dirHash, name := abstract_sql.GenDirAndName(key)
	valueFound := false
	err = store.DB.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
		res, err := s.Query(ctx, *withPragma(&store.tablePathPrefix, findQuery),
			query.WithParameters(table.NewQueryParameters(
				table.ValueParam("$dir_hash", types.Int64Value(dirHash)),
				table.ValueParam("$directory", types.UTF8Value(dirStr)),
				table.ValueParam("$name", types.UTF8Value(name)))), roQC)
		if err != nil {
			return fmt.Errorf("kv get execute %s: %v", util.NewFullPath(dirStr, name).Name(), err)
		}
		defer func() { _ = res.Close(ctx) }()
		for rs, err := range res.ResultSets(ctx) {
			if err != nil {
				return err
			}
			for row, err := range rs.Rows(ctx) {
				if err != nil {
					return err
				}
				if err := row.Scan(&value); err != nil {
					return fmt.Errorf("scan %s : %v", util.NewFullPath(dirStr, name).Name(), err)
				}
				valueFound = true
				return nil
			}
		}
		return nil
	}, query.WithIdempotent())

	if !valueFound {
		return nil, filer.ErrKvNotFound
	}

	return value, nil
}

func (store *YdbStore) KvDelete(ctx context.Context, key []byte) (err error) {
	dirStr, dirHash, name := abstract_sql.GenDirAndName(key)
	return store.DB.Query().Do(ctx, func(ctx context.Context, s query.Session) (err error) {
		_, err = s.Query(ctx, *withPragma(&store.tablePathPrefix, deleteQuery),
			query.WithParameters(table.NewQueryParameters(
				table.ValueParam("$dir_hash", types.Int64Value(dirHash)),
				table.ValueParam("$directory", types.UTF8Value(dirStr)),
				table.ValueParam("$name", types.UTF8Value(name)))), rwQC)
		if err != nil {
			return fmt.Errorf("kv delete %s: %v", util.NewFullPath(dirStr, name).Name(), err)
		}
		return nil
	}, query.WithIdempotent())

}
