package ydb

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/filer/abstract_sql"
	"github.com/yandex-cloud/ydb-go-sdk/v2"
	"github.com/yandex-cloud/ydb-go-sdk/v2/table"
)

func (store *YdbStore) KvPut(ctx context.Context, key []byte, value []byte) (err error) {
	dirStr, dirHash, name := abstract_sql.GenDirAndName(key)
	fileMeta := FileMeta{dirHash, name, dirStr, value}
	return table.Retry(ctx, store.DB.Table().Pool(),
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			stmt, err := s.Prepare(ctx, store.withPragma(store.getPrefix(dirStr), insertQuery))
			if err != nil {
				return fmt.Errorf("kv put: %v", err)
			}
			_, _, err = stmt.Execute(ctx, rwTX, fileMeta.QueryParameters())
			return fmt.Errorf("kv put: %v", err)
		}),
	)
}

func (store *YdbStore) KvGet(ctx context.Context, key []byte) (value []byte, err error) {
	dirStr, dirHash, name := abstract_sql.GenDirAndName(key)
	var res *table.Result
	err = table.Retry(ctx, store.DB.Table().Pool(),
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			stmt, err := s.Prepare(ctx, store.withPragma(store.getPrefix(dirStr), findQuery))
			if err != nil {
				return err
			}
			_, res, err = stmt.Execute(ctx, roTX, table.NewQueryParameters(
				table.ValueParam("$dir_hash", ydb.Int64Value(dirHash)),
				table.ValueParam("$name", ydb.UTF8Value(name))))
			return err
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("kv get: %v", err)
	}
	defer res.Close()

	for res.NextResultSet(ctx) {
		for res.NextRow() {
			if err = res.Scan(&value); err != nil {
				return nil, fmt.Errorf("kv get: %v", err)
			}
			return
		}
	}
	return nil, filer.ErrKvNotFound
}

func (store *YdbStore) KvDelete(ctx context.Context, key []byte) (err error) {
	dirStr, dirHash, name := abstract_sql.GenDirAndName(key)
	return table.Retry(ctx, store.DB.Table().Pool(),
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			stmt, err := s.Prepare(ctx, store.withPragma(store.getPrefix(dirStr), deleteQuery))
			if err != nil {
				return fmt.Errorf("kv delete: %s", err)
			}
			_, _, err = stmt.Execute(ctx, rwTX, table.NewQueryParameters(
				table.ValueParam("$dir_hash", ydb.Int64Value(dirHash)),
				table.ValueParam("$name", ydb.UTF8Value(name))))
			return fmt.Errorf("kv delete: %s", err)
		}),
	)
}
