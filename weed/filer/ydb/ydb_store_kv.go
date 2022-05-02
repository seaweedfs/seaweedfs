package ydb

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/filer/abstract_sql"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func (store *YdbStore) KvPut(ctx context.Context, key []byte, value []byte) (err error) {
	dirStr, dirHash, name := abstract_sql.GenDirAndName(key)
	fileMeta := FileMeta{dirHash, name, dirStr, value}
	return store.DB.Table().Do(ctx, func(ctx context.Context, s table.Session) (err error) {
		stmt, err := s.Prepare(ctx, withPragma(store.getPrefix(ctx, dirStr), insertQuery))
		if err != nil {
			return fmt.Errorf("Prepare %s: %v", util.NewFullPath(dirStr, name), err)
		}
		_, _, err = stmt.Execute(ctx, rwTX, fileMeta.queryParameters())
		if err != nil {
			return fmt.Errorf("kv put %s: %v", util.NewFullPath(dirStr, name), err)
		}
		return nil
	})
}

func (store *YdbStore) KvGet(ctx context.Context, key []byte) (value []byte, err error) {
	dirStr, dirHash, name := abstract_sql.GenDirAndName(key)
	valueFound := false
	err = store.DB.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		stmt, err := s.Prepare(ctx, withPragma(store.getPrefix(ctx, dirStr), findQuery))
		if err != nil {
			return fmt.Errorf("Prepare %s: %v", util.NewFullPath(dirStr, name), err)
		}
		_, res, err := stmt.Execute(ctx, roTX, table.NewQueryParameters(
			table.ValueParam("$dir_hash", types.Int64Value(dirHash)),
			table.ValueParam("$name", types.UTF8Value(name))))
		if err != nil {
			return fmt.Errorf("kv get %s: %v", util.NewFullPath(dirStr, name), err)
		}
		defer func() { _ = res.Close() }()
		for res.NextRow() {
			if err := res.ScanNamed(named.Required("meta", &value)); err != nil {
				return fmt.Errorf("scanNamed %s : %v", util.NewFullPath(dirStr, name), err)
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
		stmt, err := s.Prepare(ctx, withPragma(store.getPrefix(ctx, dirStr), insertQuery))
		if err != nil {
			return fmt.Errorf("Prepare %s: %v", util.NewFullPath(dirStr, name), err)
		}
		_, _, err = stmt.Execute(ctx, rwTX, table.NewQueryParameters(
			table.ValueParam("$dir_hash", types.Int64Value(dirHash)),
			table.ValueParam("$name", types.UTF8Value(name))))
		if err != nil {
			return fmt.Errorf("kv delete %s: %v", util.NewFullPath(dirStr, name), err)
		}
		return nil
	})

}
