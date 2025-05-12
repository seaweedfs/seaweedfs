//go:build ydb
// +build ydb

package ydb

import (
	"context"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/filer/abstract_sql"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

const (
	defaultDialTimeOut = 10
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
		configuration.GetInt(prefix+"dialTimeOut"),
		configuration.GetInt(prefix+"poolSizeLimit"),
	)
}

func (store *YdbStore) initialize(dirBuckets string, dsn string, tablePathPrefix string, useBucketPrefix bool, dialTimeOut int, poolSizeLimit int) (err error) {
	store.dirBuckets = dirBuckets
	store.SupportBucketTable = useBucketPrefix
	if store.SupportBucketTable {
		glog.V(0).Infof("enabled BucketPrefix")
	}
	store.dbs = make(map[string]bool)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if dialTimeOut == 0 {
		dialTimeOut = defaultDialTimeOut
	}
	opts := []ydb.Option{
		ydb.WithDialTimeout(time.Duration(dialTimeOut) * time.Second),
		environ.WithEnvironCredentials(),
	}
	if poolSizeLimit > 0 {
		opts = append(opts, ydb.WithSessionPoolSizeLimit(poolSizeLimit))
	}
	if dsn == "" {
		dsn = os.Getenv("YDB_CONNECTION_STRING")
	}
	store.DB, err = ydb.Open(ctx, dsn, opts...)
	if err != nil {
		if store.DB != nil {
			_ = store.DB.Close(ctx)
			store.DB = nil
		}
		return fmt.Errorf("can not connect to %s error: %v", dsn, err)
	}

	store.tablePathPrefix = path.Join(store.DB.Name(), tablePathPrefix)

	if err := store.ensureTables(ctx); err != nil {
		return err
	}
	return err
}

func (store *YdbStore) doTxOrDB(ctx context.Context, query *string, params *table.QueryParameters, tc *table.TransactionControl, processResultFunc func(res result.Result) error) (err error) {
	var res result.Result
	if tx, ok := ctx.Value("tx").(table.Transaction); ok {
		res, err = tx.Execute(ctx, *query, params)
		if err != nil {
			return fmt.Errorf("execute transaction: %v", err)
		}
	} else {
		err = store.DB.Table().Do(ctx, func(ctx context.Context, s table.Session) (err error) {
			_, res, err = s.Execute(ctx, tc, *query, params)
			if err != nil {
				return fmt.Errorf("execute statement: %v", err)
			}
			return nil
		},
			table.WithIdempotent(),
		)
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

func (store *YdbStore) insertOrUpdateEntry(ctx context.Context, entry *filer.Entry) (err error) {
	dir, name := entry.FullPath.DirAndName()
	meta, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		return fmt.Errorf("encode %s: %s", entry.FullPath, err)
	}

	if len(entry.GetChunks()) > filer.CountEntryChunksForGzip {
		meta = util.MaybeGzipData(meta)
	}
	tablePathPrefix, shortDir := store.getPrefix(ctx, &dir)
	fileMeta := FileMeta{util.HashStringToLong(dir), name, *shortDir, meta}
	return store.doTxOrDB(ctx, withPragma(tablePathPrefix, upsertQuery), fileMeta.queryParameters(entry.TtlSec), rwTX, nil)
}

func (store *YdbStore) InsertEntry(ctx context.Context, entry *filer.Entry) (err error) {
	return store.insertOrUpdateEntry(ctx, entry)
}

func (store *YdbStore) UpdateEntry(ctx context.Context, entry *filer.Entry) (err error) {
	return store.insertOrUpdateEntry(ctx, entry)
}

func (store *YdbStore) FindEntry(ctx context.Context, fullpath util.FullPath) (entry *filer.Entry, err error) {
	dir, name := fullpath.DirAndName()
	var data []byte
	entryFound := false
	tablePathPrefix, shortDir := store.getPrefix(ctx, &dir)
	query := withPragma(tablePathPrefix, findQuery)
	queryParams := table.NewQueryParameters(
		table.ValueParam("$dir_hash", types.Int64Value(util.HashStringToLong(*shortDir))),
		table.ValueParam("$name", types.UTF8Value(name)))

	err = store.doTxOrDB(ctx, query, queryParams, roTX, func(res result.Result) error {
		if !res.NextResultSet(ctx) || !res.HasNextRow() {
			return nil
		}
		for res.NextRow() {
			if err = res.ScanNamed(named.OptionalWithDefault("meta", &data)); err != nil {
				return fmt.Errorf("scanNamed %s : %v", fullpath, err)
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
	tablePathPrefix, shortDir := store.getPrefix(ctx, &dir)
	query := withPragma(tablePathPrefix, deleteQuery)
	glog.V(4).Infof("DeleteEntry %s, tablePathPrefix %s, shortDir %s", fullpath, *tablePathPrefix, *shortDir)
	queryParams := table.NewQueryParameters(
		table.ValueParam("$dir_hash", types.Int64Value(util.HashStringToLong(*shortDir))),
		table.ValueParam("$name", types.UTF8Value(name)))

	return store.doTxOrDB(ctx, query, queryParams, rwTX, nil)
}

func (store *YdbStore) DeleteFolderChildren(ctx context.Context, fullpath util.FullPath) (err error) {
	dir := string(fullpath)
	tablePathPrefix, shortDir := store.getPrefix(ctx, &dir)
	query := withPragma(tablePathPrefix, deleteFolderChildrenQuery)
	queryParams := table.NewQueryParameters(
		table.ValueParam("$dir_hash", types.Int64Value(util.HashStringToLong(*shortDir))),
		table.ValueParam("$directory", types.UTF8Value(*shortDir)))

	return store.doTxOrDB(ctx, query, queryParams, rwTX, nil)
}

func (store *YdbStore) ListDirectoryEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {
	return store.ListDirectoryPrefixedEntries(ctx, dirPath, startFileName, includeStartFile, limit, "", eachEntryFunc)
}

func (store *YdbStore) ListDirectoryPrefixedEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, prefix string, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {
	dir := string(dirPath)
	tablePathPrefix, shortDir := store.getPrefix(ctx, &dir)
	var query *string
	if includeStartFile {
		query = withPragma(tablePathPrefix, listInclusiveDirectoryQuery)
	} else {
		query = withPragma(tablePathPrefix, listDirectoryQuery)
	}
	truncated := true
	eachEntryFuncIsNotBreake := true
	entryCount := int64(0)
	for truncated && eachEntryFuncIsNotBreake {
		if lastFileName != "" {
			startFileName = lastFileName
			if includeStartFile {
				query = withPragma(tablePathPrefix, listDirectoryQuery)
			}
		}
		restLimit := limit - entryCount
		const maxChunk = int64(1000)
		chunkLimit := restLimit
		if chunkLimit > maxChunk {
			chunkLimit = maxChunk
		}
		glog.V(4).Infof("startFileName %s, restLimit %d, chunkLimit %d", startFileName, restLimit, chunkLimit)

		queryParams := table.NewQueryParameters(
			table.ValueParam("$dir_hash", types.Int64Value(util.HashStringToLong(*shortDir))),
			table.ValueParam("$directory", types.UTF8Value(*shortDir)),
			table.ValueParam("$start_name", types.UTF8Value(startFileName)),
			table.ValueParam("$prefix", types.UTF8Value(prefix+"%")),
			table.ValueParam("$limit", types.Uint64Value(uint64(chunkLimit))),
		)
		err = store.doTxOrDB(ctx, query, queryParams, roTX, func(res result.Result) error {
			var name string
			var data []byte
			if !res.NextResultSet(ctx) || !res.HasNextRow() {
				truncated = false
				return nil
			}
			truncated = res.CurrentResultSet().Truncated()
			glog.V(4).Infof("truncated %v, entryCount %d", truncated, entryCount)
			for res.NextRow() {
				if err := res.ScanNamed(
					named.OptionalWithDefault("name", &name),
					named.OptionalWithDefault("meta", &data)); err != nil {
					return fmt.Errorf("list scanNamed %s : %v", dir, err)
				}
				glog.V(8).Infof("name %s, fullpath %s", name, util.NewFullPath(dir, name))
				lastFileName = name
				entry := &filer.Entry{
					FullPath: util.NewFullPath(dir, name),
				}
				if err = entry.DecodeAttributesAndChunks(util.MaybeDecompressData(data)); err != nil {
					return fmt.Errorf("scan decode %s : %v", entry.FullPath, err)
				}
				if !eachEntryFunc(entry) {
					eachEntryFuncIsNotBreake = false
					break
				}
				entryCount += 1
			}
			return res.Err()
		})
	}
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

var _ filer.BucketAware = (*YdbStore)(nil)

func (store *YdbStore) CanDropWholeBucket() bool {
	return store.SupportBucketTable
}

func (store *YdbStore) OnBucketCreation(bucket string) {
	if !store.SupportBucketTable {
		return
	}
	prefix := path.Join(store.tablePathPrefix, bucket)

	store.dbsLock.Lock()
	defer store.dbsLock.Unlock()

	if err := store.createTable(context.Background(), prefix); err != nil {
		glog.Errorf("createTable %s: %v", prefix, err)
	}

	if store.dbs == nil {
		return
	}
	store.dbs[bucket] = true
}

func (store *YdbStore) OnBucketDeletion(bucket string) {
	if !store.SupportBucketTable {
		return
	}
	store.dbsLock.Lock()
	defer store.dbsLock.Unlock()

	prefix := path.Join(store.tablePathPrefix, bucket)
	glog.V(4).Infof("deleting table %s", prefix)

	if err := store.deleteTable(context.Background(), prefix); err != nil {
		glog.Errorf("deleteTable %s: %v", prefix, err)
	}

	if err := store.DB.Scheme().RemoveDirectory(context.Background(), prefix); err != nil {
		glog.Errorf("remove directory %s: %v", prefix, err)
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
	if err := store.DB.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		return s.DropTable(ctx, path.Join(prefix, abstract_sql.DEFAULT_TABLE))
	}); err != nil {
		return err
	}
	glog.V(4).Infof("deleted table %s", prefix)

	return nil
}

func (store *YdbStore) getPrefix(ctx context.Context, dir *string) (tablePathPrefix *string, shortDir *string) {
	tablePathPrefix = &store.tablePathPrefix
	shortDir = dir
	if !store.SupportBucketTable {
		return
	}

	prefixBuckets := store.dirBuckets + "/"
	glog.V(4).Infof("dir: %s, prefixBuckets: %s", *dir, prefixBuckets)
	if strings.HasPrefix(*dir, prefixBuckets) {
		// detect bucket
		bucketAndDir := (*dir)[len(prefixBuckets):]
		glog.V(4).Infof("bucketAndDir: %s", bucketAndDir)
		var bucket string
		if t := strings.Index(bucketAndDir, "/"); t > 0 {
			bucket = bucketAndDir[:t]
		} else if t < 0 {
			bucket = bucketAndDir
		}
		if bucket == "" {
			return
		}

		store.dbsLock.Lock()
		defer store.dbsLock.Unlock()

		tablePathPrefixWithBucket := path.Join(store.tablePathPrefix, bucket)
		if _, found := store.dbs[bucket]; !found {
			if err := store.createTable(ctx, tablePathPrefixWithBucket); err == nil {
				store.dbs[bucket] = true
				glog.V(4).Infof("created table %s", tablePathPrefixWithBucket)
			} else {
				glog.Errorf("createTable %s: %v", tablePathPrefixWithBucket, err)
			}
		}
		tablePathPrefix = &tablePathPrefixWithBucket
	}
	return
}

func (store *YdbStore) ensureTables(ctx context.Context) error {
	prefixFull := store.tablePathPrefix

	glog.V(4).Infof("creating base table %s", prefixFull)
	baseTable := path.Join(prefixFull, abstract_sql.DEFAULT_TABLE)
	if err := store.DB.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		return s.CreateTable(ctx, baseTable, createTableOptions()...)
	}); err != nil {
		return fmt.Errorf("failed to create base table %s: %v", baseTable, err)
	}

	glog.V(4).Infof("creating bucket tables")
	if store.SupportBucketTable {
		store.dbsLock.Lock()
		defer store.dbsLock.Unlock()
		for bucket := range store.dbs {
			glog.V(4).Infof("creating bucket table %s", bucket)
			bucketTable := path.Join(prefixFull, bucket, abstract_sql.DEFAULT_TABLE)
			if err := store.DB.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
				return s.CreateTable(ctx, bucketTable, createTableOptions()...)
			}); err != nil {
				glog.Errorf("failed to create bucket table %s: %v", bucketTable, err)
			}
		}
	}
	return nil
}
