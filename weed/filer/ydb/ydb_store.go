package ydb

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"path"
	"strings"
	"time"
)

const (
	defaultConnectionTimeOut = 10
)

var (
	roTX = table.TxControl(
		table.BeginTx(table.WithOnlineReadOnly()),
		table.CommitTx(),
	)
	rwTX = table.DefaultTxControl()
)

type YdbStore struct {
	SupportBucketTable bool
	DB                 ydb.Connection
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
	return store.initialize(
		configuration.GetString("filer.options.buckets_folder"),
		configuration.GetString(prefix+"dsn"),
		configuration.GetString(prefix+"tablePathPrefix"),
		configuration.GetBool(prefix+"useBucketPrefix"),
		configuration.GetInt(prefix+"connectionTimeOut"),
		configuration.GetInt(prefix+"poolSizeLimit"),
	)
}

func (store *YdbStore) initialize(dirBuckets string, dsn string, tablePathPrefix string, useBucketPrefix bool, connectionTimeOut int, poolSizeLimit int) (err error) {
	store.dirBuckets = dirBuckets
	store.tablePathPrefix = tablePathPrefix
	store.SupportBucketTable = useBucketPrefix
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if connectionTimeOut == 0 {
		connectionTimeOut = defaultConnectionTimeOut
	}
	opts := []ydb.Option{
		environ.WithEnvironCredentials(ctx),
		ydb.WithDialTimeout(time.Duration(connectionTimeOut) * time.Second),
	}
	if poolSizeLimit > 0 {
		opts = append(opts, ydb.WithSessionPoolSizeLimit(poolSizeLimit))
	}
	store.DB, err = ydb.Open(ctx, dsn, opts...)
	if err != nil {
		_ = store.DB.Close(ctx)
		store.DB = nil
		return fmt.Errorf("can not connect to %s error:%v", dsn, err)
	}
	defer func() { _ = store.DB.Close(ctx) }()
	store.tablePathPrefix = path.Join(store.DB.Name(), tablePathPrefix)
	if err = sugar.RemoveRecursive(ctx, store.DB, store.tablePathPrefix); err != nil {
		return fmt.Errorf("RemoveRecursive %s : %v", store.tablePathPrefix, err)
	}
	if err = sugar.MakeRecursive(ctx, store.DB, store.tablePathPrefix); err != nil {
		return fmt.Errorf("MakeRecursive %s : %v", store.tablePathPrefix, err)
	}
	return nil
}

func (store *YdbStore) insertOrUpdateEntry(ctx context.Context, entry *filer.Entry, query string) (err error) {
	dir, name := entry.FullPath.DirAndName()
	meta, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		return fmt.Errorf("encode %s: %s", entry.FullPath, err)
	}

	if len(entry.Chunks) > filer.CountEntryChunksForGzip {
		meta = util.MaybeGzipData(meta)
	}

	fileMeta := FileMeta{util.HashStringToLong(dir), name, dir, meta}
	return store.DB.Table().Do(ctx, func(ctx context.Context, s table.Session) (err error) {
		stmt, err := s.Prepare(ctx, store.withPragma(store.getPrefix(dir), query))
		if err != nil {
			return fmt.Errorf("Prepare %s : %v", dir, err)
		}
		_, _, err = stmt.Execute(ctx, rwTX, fileMeta.QueryParameters())
		return err
	})
}

func (store *YdbStore) InsertEntry(ctx context.Context, entry *filer.Entry) (err error) {
	return store.insertOrUpdateEntry(ctx, entry, insertQuery)
}

func (store *YdbStore) UpdateEntry(ctx context.Context, entry *filer.Entry) (err error) {
	return store.insertOrUpdateEntry(ctx, entry, updateQuery)
}

func (store *YdbStore) FindEntry(ctx context.Context, fullpath util.FullPath) (entry *filer.Entry, err error) {
	dir, name := fullpath.DirAndName()
	var data []byte
	entryFound := false
	err = store.DB.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		stmt, err := s.Prepare(ctx, store.withPragma(store.getPrefix(dir), findQuery))
		if err != nil {
			return fmt.Errorf("Prepare %s : %v", entry.FullPath, err)
		}
		_, res, err := stmt.Execute(ctx, roTX, table.NewQueryParameters(
			table.ValueParam("$dir_hash", types.Int64Value(util.HashStringToLong(dir))),
			table.ValueParam("$name", types.UTF8Value(name))))
		if err != nil {
			return fmt.Errorf("Execute %s : %v", entry.FullPath, err)
		}
		defer func() {
			_ = res.Close()
		}()
		for res.NextRow() {
			if err := res.ScanNamed(named.Required("meta", &data)); err != nil {
				return fmt.Errorf("scanNamed %s : %v", entry.FullPath, err)
			}
			entryFound = true
			return nil
		}
		return res.Err()
	})
	if err != nil {
		return nil, err
	}
	if !entryFound {
		return nil, filer_pb.ErrNotFound
	}
	entry.FullPath = fullpath
	if err := entry.DecodeAttributesAndChunks(util.MaybeDecompressData(data)); err != nil {
		return nil, fmt.Errorf("decode %s : %v", entry.FullPath, err)
	}

	return entry, nil
}

func (store *YdbStore) DeleteEntry(ctx context.Context, fullpath util.FullPath) (err error) {
	dir, name := fullpath.DirAndName()
	return store.DB.Table().Do(ctx, func(ctx context.Context, s table.Session) (err error) {
		stmt, err := s.Prepare(ctx, store.withPragma(store.getPrefix(dir), deleteQuery))
		if err != nil {
			return fmt.Errorf("Prepare %s : %v", dir, err)
		}
		_, _, err = stmt.Execute(ctx, rwTX, table.NewQueryParameters(
			table.ValueParam("$dir_hash", types.Int64Value(util.HashStringToLong(dir))),
			table.ValueParam("$name", types.UTF8Value(name))))
		return err
	})
}

func (store *YdbStore) DeleteFolderChildren(ctx context.Context, fullpath util.FullPath) (err error) {
	dir, _ := fullpath.DirAndName()
	return store.DB.Table().Do(ctx, func(ctx context.Context, s table.Session) (err error) {
		stmt, err := s.Prepare(ctx, store.withPragma(store.getPrefix(dir), deleteFolderChildrenQuery))
		if err != nil {
			return fmt.Errorf("Prepare %s : %v", dir, err)
		}
		_, _, err = stmt.Execute(ctx, rwTX, table.NewQueryParameters(
			table.ValueParam("$dir_hash", types.Int64Value(util.HashStringToLong(dir))),
			table.ValueParam("$directory", types.UTF8Value(dir))))
		return err
	})
}

func (store *YdbStore) ListDirectoryEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {
	return store.ListDirectoryPrefixedEntries(ctx, dirPath, startFileName, includeStartFile, limit, "", nil)
}

func (store *YdbStore) ListDirectoryPrefixedEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, prefix string, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {
	dir := string(dirPath)
	startFileCompOp := ">"
	if includeStartFile {
		startFileCompOp = ">="
	}
	err = store.DB.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		stmt, err := s.Prepare(ctx, store.withPragma(store.getPrefix(dir), fmt.Sprintf(ListDirectoryQuery, startFileCompOp)))
		if err != nil {
			return fmt.Errorf("Prepare %s : %v", dir, err)
		}
		_, res, err := stmt.Execute(ctx, roTX, table.NewQueryParameters(
			table.ValueParam("$dir_hash", types.Int64Value(util.HashStringToLong(dir))),
			table.ValueParam("$directory", types.UTF8Value(dir)),
			table.ValueParam("$start_name", types.UTF8Value(startFileName)),
			table.ValueParam("$prefix", types.UTF8Value(prefix)),
			table.ValueParam("$limit", types.Int64Value(limit)),
		))
		if err != nil {
			return fmt.Errorf("Execute %s : %v", dir, err)
		}
		defer func() {
			_ = res.Close()
		}()
		for res.NextResultSet(ctx) {
			for res.NextRow() {
				var name string
				var data []byte
				if err := res.ScanNamed(
					named.Required("name", &name),
					named.Required("meta", &data)); err != nil {
					return fmt.Errorf("scanNamed %s : %v", dir, err)
				}
				lastFileName = name
				entry := &filer.Entry{
					FullPath: util.NewFullPath(dir, name),
				}
				if err = entry.DecodeAttributesAndChunks(util.MaybeDecompressData(data)); err != nil {
					glog.V(0).Infof("scan decode %s : %v", entry.FullPath, err)
					return fmt.Errorf("scan decode %s : %v", entry.FullPath, err)
				}
				if !eachEntryFunc(entry) {
					break
				}
			}
		}
		return res.Err()
	})
	if err != nil {
		return lastFileName, err
	}
	return lastFileName, nil
}

func (store *YdbStore) BeginTransaction(ctx context.Context) (context.Context, error) {
	session, err := store.DB.Table().CreateSession(ctx)
	if err != nil {
		return ctx, err
	}
	tx, err := session.BeginTransaction(ctx, table.TxSettings(table.WithSerializableReadWrite()))
	if err != nil {
		return ctx, err
	}
	return context.WithValue(ctx, "tx", tx), nil
}

func (store *YdbStore) CommitTransaction(ctx context.Context) error {
	if tx, ok := ctx.Value("tx").(table.Transaction); ok {
		_, err := tx.CommitTx(ctx)
		return err
	}
	return nil
}

func (store *YdbStore) RollbackTransaction(ctx context.Context) error {
	if tx, ok := ctx.Value("tx").(table.Transaction); ok {
		return tx.Rollback(ctx)
	}
	return nil
}

func (store *YdbStore) Shutdown() {
	_ = store.DB.Close(context.Background())
}

func (store *YdbStore) getPrefix(dir string) string {
	if !store.SupportBucketTable {
		return store.tablePathPrefix
	}

	prefixBuckets := store.dirBuckets + "/"
	if strings.HasPrefix(dir, prefixBuckets) {
		// detect bucket
		bucketAndDir := dir[len(prefixBuckets):]
		if t := strings.Index(bucketAndDir, "/"); t > 0 {
			return path.Join(bucketAndDir[:t], store.tablePathPrefix)
		}
	}
	return store.tablePathPrefix
}

func (store *YdbStore) withPragma(prefix, query string) string {
	return `PRAGMA TablePathPrefix("` + prefix + `");` + query
}
