package ydb

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/filer/abstract_sql"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"os"
	"path"
	"strings"
	"sync"
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
	DB                 ydb.Connection
	dirBuckets         string
	tablePathPrefix    string
	SupportBucketTable bool
	dbs                map[string]bool
	dbsLock            sync.Mutex
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
		configuration.GetString(prefix+"prefix"),
		configuration.GetBool(prefix+"useBucketPrefix"),
		configuration.GetInt(prefix+"connectionTimeOut"),
		configuration.GetInt(prefix+"poolSizeLimit"),
	)
}

func (store *YdbStore) initialize(dirBuckets string, dsn string, tablePathPrefix string, useBucketPrefix bool, connectionTimeOut int, poolSizeLimit int) (err error) {
	store.dirBuckets = dirBuckets
	store.SupportBucketTable = useBucketPrefix
	store.dbs = make(map[string]bool)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if connectionTimeOut == 0 {
		connectionTimeOut = defaultConnectionTimeOut
	}
	opts := []ydb.Option{
		ydb.WithDialTimeout(time.Duration(connectionTimeOut) * time.Second),
		environ.WithEnvironCredentials(ctx),
	}
	if poolSizeLimit > 0 {
		opts = append(opts, ydb.WithSessionPoolSizeLimit(poolSizeLimit))
	}
	if dsn == "" {
		dsn = os.Getenv("YDB_CONNECTION_STRING")
	}
	store.DB, err = ydb.Open(ctx, dsn, opts...)
	if err != nil || store.DB == nil {
		if store.DB != nil {
			_ = store.DB.Close(ctx)
			store.DB = nil
		}
		return fmt.Errorf("can not connect to %s error: %v", dsn, err)
	}

	store.tablePathPrefix = path.Join(store.DB.Name(), tablePathPrefix)
	if err = sugar.MakeRecursive(ctx, store.DB, store.tablePathPrefix); err != nil {
		return fmt.Errorf("MakeRecursive %s : %v", store.tablePathPrefix, err)
	}

	if err = store.createTable(ctx, store.tablePathPrefix); err != nil {
		glog.Errorf("createTable %s: %v", store.tablePathPrefix, err)
	}
	return err
}

func (store *YdbStore) doTxOrDB(ctx context.Context, query *string, params *table.QueryParameters, tc *table.TransactionControl, processResultFunc func(res result.Result) error) (err error) {
	var res result.Result
	if tx, ok := ctx.Value("tx").(table.Transaction); ok {
		res, err = tx.Execute(ctx, *query, params, options.WithQueryCachePolicy(options.WithQueryCachePolicyKeepInCache()))
		if err != nil {
			return fmt.Errorf("execute transaction: %v", err)
		}
	} else {
		err = store.DB.Table().Do(ctx, func(ctx context.Context, s table.Session) (err error) {
			_, res, err = s.Execute(ctx, tc, *query,
				params, options.WithQueryCachePolicy(options.WithQueryCachePolicyKeepInCache()))
			if err != nil {
				return fmt.Errorf("execute statement: %v", err)
			}
			return nil
		})
	}
	if err != nil {
		return err
	}
	if res != nil {
		defer func() { _ = res.Close() }()
		if processResultFunc != nil {
			if err = processResultFunc(res); err != nil {
				return fmt.Errorf("process result: %v", err)
			}
		}
	}
	return err
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
	queryWithPragma := withPragma(store.getPrefix(ctx, dir), query)
	fileMeta := FileMeta{util.HashStringToLong(dir), name, dir, meta}
	return store.doTxOrDB(ctx, &queryWithPragma, fileMeta.queryParameters(entry.TtlSec), rwTX, nil)
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
	queryWithPragma := withPragma(store.getPrefix(ctx, dir), findQuery)
	queryParams := table.NewQueryParameters(
		table.ValueParam("$dir_hash", types.Int64Value(util.HashStringToLong(dir))),
		table.ValueParam("$name", types.UTF8Value(name)))

	err = store.doTxOrDB(ctx, &queryWithPragma, queryParams, roTX, func(res result.Result) error {
		for res.NextResultSet(ctx) {
			for res.NextRow() {
				if err = res.ScanNamed(named.OptionalWithDefault("meta", &data)); err != nil {
					return fmt.Errorf("scanNamed %s : %v", fullpath, err)
				}
				entryFound = true
				return nil
			}
		}
		return res.Err()
	})
	if err != nil {
		return nil, err
	}
	if !entryFound {
		return nil, filer_pb.ErrNotFound
	}

	entry = &filer.Entry{
		FullPath: fullpath,
	}
	if err := entry.DecodeAttributesAndChunks(util.MaybeDecompressData(data)); err != nil {
		return nil, fmt.Errorf("decode %s : %v", fullpath, err)
	}

	return entry, nil
}

func (store *YdbStore) DeleteEntry(ctx context.Context, fullpath util.FullPath) (err error) {
	dir, name := fullpath.DirAndName()
	queryWithPragma := withPragma(store.getPrefix(ctx, dir), deleteQuery)
	queryParams := table.NewQueryParameters(
		table.ValueParam("$dir_hash", types.Int64Value(util.HashStringToLong(dir))),
		table.ValueParam("$name", types.UTF8Value(name)))

	return store.doTxOrDB(ctx, &queryWithPragma, queryParams, rwTX, nil)
}

func (store *YdbStore) DeleteFolderChildren(ctx context.Context, fullpath util.FullPath) (err error) {
	dir, _ := fullpath.DirAndName()
	queryWithPragma := withPragma(store.getPrefix(ctx, dir), deleteFolderChildrenQuery)
	queryParams := table.NewQueryParameters(
		table.ValueParam("$dir_hash", types.Int64Value(util.HashStringToLong(dir))),
		table.ValueParam("$directory", types.UTF8Value(dir)))

	return store.doTxOrDB(ctx, &queryWithPragma, queryParams, rwTX, nil)
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
	queryWithPragma := withPragma(store.getPrefix(ctx, dir), fmt.Sprintf(listDirectoryQuery, startFileCompOp))
	queryParams := table.NewQueryParameters(
		table.ValueParam("$dir_hash", types.Int64Value(util.HashStringToLong(dir))),
		table.ValueParam("$directory", types.UTF8Value(dir)),
		table.ValueParam("$start_name", types.UTF8Value(startFileName)),
		table.ValueParam("$prefix", types.UTF8Value(prefix+"%")),
		table.ValueParam("$limit", types.Uint64Value(uint64(limit))),
	)
	err = store.doTxOrDB(ctx, &queryWithPragma, queryParams, roTX, func(res result.Result) error {
		var name string
		var data []byte
		for res.NextResultSet(ctx) {
			for res.NextRow() {
				if err := res.ScanNamed(
					named.OptionalWithDefault("name", &name),
					named.OptionalWithDefault("meta", &data)); err != nil {
					return fmt.Errorf("list scanNamed %s : %v", dir, err)
				}
				lastFileName = name
				entry := &filer.Entry{
					FullPath: util.NewFullPath(dir, name),
				}
				if err = entry.DecodeAttributesAndChunks(util.MaybeDecompressData(data)); err != nil {
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

func (store *YdbStore) CanDropWholeBucket() bool {
	return store.SupportBucketTable
}

func (store *YdbStore) OnBucketCreation(bucket string) {
	store.dbsLock.Lock()
	defer store.dbsLock.Unlock()

	if err := store.createTable(context.Background(),
		path.Join(store.tablePathPrefix, bucket)); err != nil {
		glog.Errorf("createTable %s: %v", bucket, err)
	}

	if store.dbs == nil {
		return
	}
	store.dbs[bucket] = true
}

func (store *YdbStore) OnBucketDeletion(bucket string) {
	store.dbsLock.Lock()
	defer store.dbsLock.Unlock()

	if err := store.deleteTable(context.Background(),
		path.Join(store.tablePathPrefix, bucket)); err != nil {
		glog.Errorf("deleteTable %s: %v", bucket, err)
	}

	if store.dbs == nil {
		return
	}
	delete(store.dbs, bucket)
}

func (store *YdbStore) createTable(ctx context.Context, prefix string) error {
	return store.DB.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		return s.CreateTable(ctx, path.Join(prefix, abstract_sql.DEFAULT_TABLE), createTableOptions()...)
	})
}

func (store *YdbStore) deleteTable(ctx context.Context, prefix string) error {
	if !store.SupportBucketTable {
		return nil
	}
	return store.DB.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		return s.DropTable(ctx, path.Join(prefix, abstract_sql.DEFAULT_TABLE))
	})
}

func (store *YdbStore) getPrefix(ctx context.Context, dir string) (tablePathPrefix string) {
	tablePathPrefix = store.tablePathPrefix
	if !store.SupportBucketTable {
		return
	}

	prefixBuckets := store.dirBuckets + "/"
	if strings.HasPrefix(dir, prefixBuckets) {
		// detect bucket
		bucketAndDir := dir[len(prefixBuckets):]
		t := strings.Index(bucketAndDir, "/")
		if t < 0 {
			return
		}
		bucket := bucketAndDir[:t]

		if bucket != "" {
			return
		}
		store.dbsLock.Lock()
		defer store.dbsLock.Unlock()

		tablePathPrefix = path.Join(store.tablePathPrefix, bucket)
		if _, found := store.dbs[bucket]; !found {
			if err := store.createTable(ctx, tablePathPrefix); err == nil {
				store.dbs[bucket] = true
			} else {
				glog.Errorf("createTable %s: %v", tablePathPrefix, err)
			}
		}
	}
	return
}
