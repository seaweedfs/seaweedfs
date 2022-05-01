package ydb

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/yandex-cloud/ydb-go-sdk/v2"
	"github.com/yandex-cloud/ydb-go-sdk/v2/connect"
	"github.com/yandex-cloud/ydb-go-sdk/v2/table"
	"strings"
	"time"
)

var (
	roTX = table.TxControl(
		table.BeginTx(table.WithOnlineReadOnly()),
		table.CommitTx(),
	)
	rwTX = table.TxControl(
		table.BeginTx(table.WithSerializableReadWrite()),
		table.CommitTx(),
	)
)

const (
	createQuery = `
		PRAGMA TablePathPrefix("%s");
		CREATE TABLE file_meta (
			dir_hash int64,
			name Utf8,
			directory Utf8,
			meta String,
			PRIMARY KEY (dir_hash, name)
		);`
	insertQuery = `
		DECLARE $dir_hash int64;
		DECLARE $name AS Utf8;
		DECLARE $directory AS Utf8;
		DECLARE $meta AS String;

		UPSERT INTO file_meta
			(dir_hash, name, directory, meta)
		VALUES
			($dir_hash, $name, $directory, $meta);`
	updateQuery = `
		DECLARE $dir_hash int64;
		DECLARE $name AS Utf8;
		DECLARE $directory AS Utf8;
		DECLARE $meta AS String;

		REPLACE INTO file_meta
			(dir_hash, name, directory, meta)
		VALUES
			($dir_hash, $name, $directory, $meta)
		COMMIT;`
	deleteQuery = `
		DECLARE $dir_hash int64;
		DECLARE $name AS Utf8;

		DELETE FROM file_meta 
		WHERE dir_hash == $dir_hash AND name == $name;
		COMMIT;`
	findQuery = `
		DECLARE $dir_hash int64;
		DECLARE $name AS Utf8;
		
		SELECT meta
		FROM file_meta
		WHERE dir_hash == $dir_hash AND name == $name;`
)

type YdbStore struct {
	SupportBucketTable bool
	DB                 *connect.Connection
	connParams         connect.ConnectParams
	connCtx            context.Context
	dirBuckets         string
	tablePathPrefix    string
}

func init() {
	filer.Stores = append(filer.Stores, &YdbStore{})
}

func (store *YdbStore) GetName() string {
	return "ydb"
}

func (store *YdbStore) Initialize(configuration util.Configuration, prefix string) (err error) {
	return store.initialize(configuration.GetString(prefix + "coonectionUrl"))
}

func (store *YdbStore) initialize(sqlUrl string) (err error) {
	store.SupportBucketTable = false
	var cancel context.CancelFunc
	store.connCtx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	store.connParams = connect.MustConnectionString(sqlUrl)
	store.DB, err = connect.New(store.connCtx, store.connParams)
	if err != nil {
		store.DB.Close()
		store.DB = nil
		return fmt.Errorf("can not connect to %s error:%v", sqlUrl, err)
	}
	defer store.DB.Close()

	if err = store.DB.EnsurePathExists(store.connCtx, store.connParams.Database()); err != nil {
		return fmt.Errorf("connect to %s error:%v", sqlUrl, err)
	}
	return nil
}

func (store *YdbStore) insertOrUpdateEntry(ctx context.Context, entry *filer.Entry, query string) (err error) {
	dir, name := entry.FullPath.DirAndName()
	meta, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		return fmt.Errorf("encode %s: %s", entry.FullPath, err)
	}
	fileMeta := FileMeta{util.HashStringToLong(dir), name, dir, meta}
	return table.Retry(ctx, store.DB.Table().Pool(),
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			stmt, err := s.Prepare(ctx, store.withPragma(store.getPrefix(dir), query))
			if err != nil {
				return err
			}
			_, _, err = stmt.Execute(ctx, rwTX, fileMeta.QueryParameters())
			return err
		}),
	)
}

func (store *YdbStore) InsertEntry(ctx context.Context, entry *filer.Entry) (err error) {
	return store.insertOrUpdateEntry(ctx, entry, insertQuery)
}

func (store *YdbStore) UpdateEntry(ctx context.Context, entry *filer.Entry) (err error) {
	return store.insertOrUpdateEntry(ctx, entry, updateQuery)
}

func (store *YdbStore) FindEntry(ctx context.Context, fullpath util.FullPath) (entry *filer.Entry, err error) {
	dir, name := fullpath.DirAndName()
	var res *table.Result
	err = table.Retry(ctx, store.DB.Table().Pool(),
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			stmt, err := s.Prepare(ctx, store.withPragma(store.getPrefix(dir), findQuery))
			if err != nil {
				return err
			}
			_, res, err = stmt.Execute(ctx, rwTX, table.NewQueryParameters(
				table.ValueParam("$dir_hash", ydb.Int64Value(util.HashStringToLong(dir))),
				table.ValueParam("$name", ydb.UTF8Value(name))))
			return err
		}),
	)
	if err != nil {
		return nil, err
	}
	for res.NextSet() {
		for res.NextRow() {
			res.SeekItem("meta")
			entry.FullPath = fullpath
			if err := entry.DecodeAttributesAndChunks(util.MaybeDecompressData(res.String())); err != nil {
				return entry, fmt.Errorf("decode %s : %v", entry.FullPath, err)
			}
			return entry, nil
		}
	}

	return nil, filer_pb.ErrNotFound
}

func (store *YdbStore) DeleteEntry(ctx context.Context, fullpath util.FullPath) (err error) {
	dir, name := fullpath.DirAndName()
	return table.Retry(ctx, store.DB.Table().Pool(),
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			stmt, err := s.Prepare(ctx, store.withPragma(store.getPrefix(dir), deleteQuery))
			if err != nil {
				return err
			}
			_, _, err = stmt.Execute(ctx, rwTX, table.NewQueryParameters(
				table.ValueParam("$dir_hash", ydb.Int64Value(util.HashStringToLong(dir))),
				table.ValueParam("$name", ydb.UTF8Value(name))))
			return err
		}),
	)
}

func (store *YdbStore) DeleteFolderChildren(ctx context.Context, fullpath util.FullPath) (err error) {
	return nil
}

func (store *YdbStore) ListDirectoryEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {
	//TODO implement me
	panic("implement me")
}

func (store *YdbStore) ListDirectoryPrefixedEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, prefix string, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {
	//TODO implement me
	panic("implement me")
}

func (store *YdbStore) BeginTransaction(ctx context.Context) (context.Context, error) {
	//TODO implement me
	panic("implement me")
}

func (store *YdbStore) CommitTransaction(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (store *YdbStore) RollbackTransaction(ctx context.Context) error {
	//TODO implement me
	panic("implement me")
}

func (store *YdbStore) KvPut(ctx context.Context, key []byte, value []byte) (err error) {
	//TODO implement me
	panic("implement me")
}

func (store *YdbStore) KvGet(ctx context.Context, key []byte) (value []byte, err error) {
	//TODO implement me
	panic("implement me")
}

func (store *YdbStore) KvDelete(ctx context.Context, key []byte) (err error) {
	//TODO implement me
	panic("implement me")
}

func (store *YdbStore) Shutdown() {
	//TODO implement me
	panic("implement me")
}

func (store *YdbStore) getPrefix(dir string) string {
	prefixBuckets := store.dirBuckets + "/"
	if strings.HasPrefix(dir, prefixBuckets) {
		// detect bucket
		bucketAndDir := dir[len(prefixBuckets):]
		if t := strings.Index(bucketAndDir, "/"); t > 0 {
			return bucketAndDir[:t]
		}
	}
	return ""
}

func (store *YdbStore) withPragma(prefix, query string) string {
	if store.tablePathPrefix != "" && store.tablePathPrefix != "/" {
		prefix = store.tablePathPrefix + "/" + prefix
	}
	return `PRAGMA TablePathPrefix("` + prefix + `");` + query
}
