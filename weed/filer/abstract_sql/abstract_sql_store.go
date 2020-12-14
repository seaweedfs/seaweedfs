package abstract_sql

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"strings"
)

type AbstractSqlStore struct {
	DB                      *sql.DB
	SqlInsert               string
	SqlUpdate               string
	SqlFind                 string
	SqlDelete               string
	SqlDeleteFolderChildren string
	SqlListExclusive        string
	SqlListInclusive        string
}

type TxOrDB interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

func (store *AbstractSqlStore) BeginTransaction(ctx context.Context) (context.Context, error) {
	tx, err := store.DB.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
		ReadOnly:  false,
	})
	if err != nil {
		return ctx, err
	}

	return context.WithValue(ctx, "tx", tx), nil
}
func (store *AbstractSqlStore) CommitTransaction(ctx context.Context) error {
	if tx, ok := ctx.Value("tx").(*sql.Tx); ok {
		return tx.Commit()
	}
	return nil
}
func (store *AbstractSqlStore) RollbackTransaction(ctx context.Context) error {
	if tx, ok := ctx.Value("tx").(*sql.Tx); ok {
		return tx.Rollback()
	}
	return nil
}

func (store *AbstractSqlStore) getTxOrDB(ctx context.Context) TxOrDB {
	if tx, ok := ctx.Value("tx").(*sql.Tx); ok {
		return tx
	}
	return store.DB
}

func (store *AbstractSqlStore) InsertEntry(ctx context.Context, entry *filer.Entry) (err error) {

	dir, name := entry.FullPath.DirAndName()
	meta, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		return fmt.Errorf("encode %s: %s", entry.FullPath, err)
	}

	if len(entry.Chunks) > 50 {
		meta = util.MaybeGzipData(meta)
	}

	res, err := store.getTxOrDB(ctx).ExecContext(ctx, store.SqlInsert, util.HashStringToLong(dir), name, dir, meta)
	if err == nil {
		return
	}

	if !strings.Contains(strings.ToLower(err.Error()), "duplicate") {
		// return fmt.Errorf("insert: %s", err)
		// skip this since the error can be in a different language
	}

	// now the insert failed possibly due to duplication constraints
	glog.V(1).Infof("insert %s falls back to update: %v", entry.FullPath, err)

	res, err = store.getTxOrDB(ctx).ExecContext(ctx, store.SqlUpdate, meta, util.HashStringToLong(dir), name, dir)
	if err != nil {
		return fmt.Errorf("upsert %s: %s", entry.FullPath, err)
	}

	_, err = res.RowsAffected()
	if err != nil {
		return fmt.Errorf("upsert %s but no rows affected: %s", entry.FullPath, err)
	}
	return nil

}

func (store *AbstractSqlStore) UpdateEntry(ctx context.Context, entry *filer.Entry) (err error) {

	dir, name := entry.FullPath.DirAndName()
	meta, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		return fmt.Errorf("encode %s: %s", entry.FullPath, err)
	}

	res, err := store.getTxOrDB(ctx).ExecContext(ctx, store.SqlUpdate, meta, util.HashStringToLong(dir), name, dir)
	if err != nil {
		return fmt.Errorf("update %s: %s", entry.FullPath, err)
	}

	_, err = res.RowsAffected()
	if err != nil {
		return fmt.Errorf("update %s but no rows affected: %s", entry.FullPath, err)
	}
	return nil
}

func (store *AbstractSqlStore) FindEntry(ctx context.Context, fullpath util.FullPath) (*filer.Entry, error) {

	dir, name := fullpath.DirAndName()
	row := store.getTxOrDB(ctx).QueryRowContext(ctx, store.SqlFind, util.HashStringToLong(dir), name, dir)

	var data []byte
	if err := row.Scan(&data); err != nil {
		if err == sql.ErrNoRows {
			return nil, filer_pb.ErrNotFound
		}
		return nil, fmt.Errorf("find %s: %v", fullpath, err)
	}

	entry := &filer.Entry{
		FullPath: fullpath,
	}
	if err := entry.DecodeAttributesAndChunks(util.MaybeDecompressData(data)); err != nil {
		return entry, fmt.Errorf("decode %s : %v", entry.FullPath, err)
	}

	return entry, nil
}

func (store *AbstractSqlStore) DeleteEntry(ctx context.Context, fullpath util.FullPath) error {

	dir, name := fullpath.DirAndName()

	res, err := store.getTxOrDB(ctx).ExecContext(ctx, store.SqlDelete, util.HashStringToLong(dir), name, dir)
	if err != nil {
		return fmt.Errorf("delete %s: %s", fullpath, err)
	}

	_, err = res.RowsAffected()
	if err != nil {
		return fmt.Errorf("delete %s but no rows affected: %s", fullpath, err)
	}

	return nil
}

func (store *AbstractSqlStore) DeleteFolderChildren(ctx context.Context, fullpath util.FullPath) error {

	res, err := store.getTxOrDB(ctx).ExecContext(ctx, store.SqlDeleteFolderChildren, util.HashStringToLong(string(fullpath)), fullpath)
	if err != nil {
		return fmt.Errorf("deleteFolderChildren %s: %s", fullpath, err)
	}

	_, err = res.RowsAffected()
	if err != nil {
		return fmt.Errorf("deleteFolderChildren %s but no rows affected: %s", fullpath, err)
	}

	return nil
}

func (store *AbstractSqlStore) ListDirectoryPrefixedEntries(ctx context.Context, fullpath util.FullPath, startFileName string, inclusive bool, limit int, prefix string) (entries []*filer.Entry, err error) {
	sqlText := store.SqlListExclusive
	if inclusive {
		sqlText = store.SqlListInclusive
	}

	rows, err := store.getTxOrDB(ctx).QueryContext(ctx, sqlText, util.HashStringToLong(string(fullpath)), startFileName, string(fullpath), prefix+"%", limit)
	if err != nil {
		return nil, fmt.Errorf("list %s : %v", fullpath, err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var data []byte
		if err = rows.Scan(&name, &data); err != nil {
			glog.V(0).Infof("scan %s : %v", fullpath, err)
			return nil, fmt.Errorf("scan %s: %v", fullpath, err)
		}

		entry := &filer.Entry{
			FullPath: util.NewFullPath(string(fullpath), name),
		}
		if err = entry.DecodeAttributesAndChunks(util.MaybeDecompressData(data)); err != nil {
			glog.V(0).Infof("scan decode %s : %v", entry.FullPath, err)
			return nil, fmt.Errorf("scan decode %s : %v", entry.FullPath, err)
		}

		entries = append(entries, entry)
	}

	return entries, nil
}

func (store *AbstractSqlStore) ListDirectoryEntries(ctx context.Context, fullpath util.FullPath, startFileName string, inclusive bool, limit int) (entries []*filer.Entry, err error) {
	return store.ListDirectoryPrefixedEntries(ctx, fullpath, startFileName, inclusive, limit, "")
}

func (store *AbstractSqlStore) Shutdown() {
	store.DB.Close()
}
