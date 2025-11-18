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

	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/filer/abstract_sql"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	environ "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

const (
	defaultDialTimeOut            = 10
	defaultPartitionBySizeEnabled = true
	defaultPartitionSizeMb        = 200
	defaultPartitionByLoadEnabled = true
	defaultMinPartitionsCount     = 5
	defaultMaxPartitionsCount     = 1000
	defaultMaxListChunk           = 2000
)

var (
	roQC = query.WithTxControl(query.OnlineReadOnlyTxControl())
	rwQC = query.WithTxControl(query.DefaultTxControl())
)

type YdbStore struct {
	DB                     *ydb.Driver
	dirBuckets             string
	tablePathPrefix        string
	SupportBucketTable     bool
	partitionBySizeEnabled options.FeatureFlag
	partitionSizeMb        uint64
	partitionByLoadEnabled options.FeatureFlag
	minPartitionsCount     uint64
	maxPartitionsCount     uint64
	maxListChunk           int
	dbs                    map[string]bool
	dbsLock                sync.Mutex
}

func init() {
	filer.Stores = append(filer.Stores, &YdbStore{})
}

func (store *YdbStore) GetName() string {
	return "ydb"
}

func (store *YdbStore) Initialize(configuration util.Configuration, prefix string) (err error) {
	configuration.SetDefault(prefix+"partitionBySizeEnabled", defaultPartitionBySizeEnabled)
	configuration.SetDefault(prefix+"partitionSizeMb", defaultPartitionSizeMb)
	configuration.SetDefault(prefix+"partitionByLoadEnabled", defaultPartitionByLoadEnabled)
	configuration.SetDefault(prefix+"minPartitionsCount", defaultMinPartitionsCount)
	configuration.SetDefault(prefix+"maxPartitionsCount", defaultMaxPartitionsCount)
	configuration.SetDefault(prefix+"maxListChunk", defaultMaxListChunk)
	return store.initialize(
		configuration.GetString("filer.options.buckets_folder"),
		configuration.GetString(prefix+"dsn"),
		configuration.GetString(prefix+"prefix"),
		configuration.GetBool(prefix+"useBucketPrefix"),
		configuration.GetInt(prefix+"dialTimeOut"),
		configuration.GetInt(prefix+"poolSizeLimit"),
		configuration.GetBool(prefix+"partitionBySizeEnabled"),
		uint64(configuration.GetInt(prefix+"partitionSizeMb")),
		configuration.GetBool(prefix+"partitionByLoadEnabled"),
		uint64(configuration.GetInt(prefix+"minPartitionsCount")),
		uint64(configuration.GetInt(prefix+"maxPartitionsCount")),
		configuration.GetInt(prefix+"maxListChunk"),
	)
}

func (store *YdbStore) initialize(dirBuckets string, dsn string, tablePathPrefix string, useBucketPrefix bool, dialTimeOut int, poolSizeLimit int, partitionBySizeEnabled bool, partitionSizeMb uint64, partitionByLoadEnabled bool, minPartitionsCount uint64, maxPartitionsCount uint64, maxListChunk int) (err error) {
	store.dirBuckets = dirBuckets
	store.SupportBucketTable = useBucketPrefix
	if partitionBySizeEnabled {
		store.partitionBySizeEnabled = options.FeatureEnabled
	} else {
		store.partitionBySizeEnabled = options.FeatureDisabled
	}
	if partitionByLoadEnabled {
		store.partitionByLoadEnabled = options.FeatureEnabled
	} else {
		store.partitionByLoadEnabled = options.FeatureDisabled
	}
	store.partitionSizeMb = partitionSizeMb
	store.minPartitionsCount = minPartitionsCount
	store.maxPartitionsCount = maxPartitionsCount
	store.maxListChunk = maxListChunk
	if store.SupportBucketTable {
		glog.V(0).Infof("enabled BucketPrefix")
	}
	store.dbs = make(map[string]bool)
	ctx := context.Background()
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
		return fmt.Errorf("can not connect to %s: %w", dsn, err)
	}

	store.tablePathPrefix = path.Join(store.DB.Name(), tablePathPrefix)

	if err := store.ensureTables(ctx); err != nil {
		return err
	}
	return err
}

func (store *YdbStore) doTxOrDB(ctx context.Context, q *string, params *table.QueryParameters, ts query.ExecuteOption, processResultFunc func(res query.Result) error) (err error) {
	var res query.Result
	if tx, ok := ctx.Value("tx").(query.Transaction); ok {
		res, err = tx.Query(ctx, *q, query.WithParameters(params))
		if err != nil {
			return fmt.Errorf("execute transaction: %w", err)
		}
	} else {
		err = store.DB.Query().Do(ctx, func(ctx context.Context, s query.Session) (err error) {
			res, err = s.Query(ctx, *q, query.WithParameters(params), ts)
			if err != nil {
				return fmt.Errorf("execute statement: %w", err)
			}
			return nil
		}, query.WithIdempotent())
	}
	if err != nil {
		return err
	}
	if res != nil {
		defer func() { _ = res.Close(ctx) }()
		if processResultFunc != nil {
			if err = processResultFunc(res); err != nil {
				return fmt.Errorf("process result: %w", err)
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
	return store.doTxOrDB(ctx, withPragma(tablePathPrefix, upsertQuery), fileMeta.queryParameters(entry.TtlSec), rwQC, nil)
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
	q := withPragma(tablePathPrefix, findQuery)
	queryParams := table.NewQueryParameters(
		table.ValueParam("$dir_hash", types.Int64Value(util.HashStringToLong(*shortDir))),
		table.ValueParam("$directory", types.UTF8Value(*shortDir)),
		table.ValueParam("$name", types.UTF8Value(name)))

	err = store.doTxOrDB(ctx, q, queryParams, roQC, func(res query.Result) error {
		for rs, err := range res.ResultSets(ctx) {
			if err != nil {
				return err
			}
			for row, err := range rs.Rows(ctx) {
				if err != nil {
					return err
				}
				if scanErr := row.Scan(&data); scanErr != nil {
					return fmt.Errorf("scan %s: %v", fullpath, scanErr)
				}
				entryFound = true
				return nil
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if !entryFound {
		return nil, filer_pb.ErrNotFound
	}

	entry = &filer.Entry{FullPath: fullpath}
	if decodeErr := entry.DecodeAttributesAndChunks(util.MaybeDecompressData(data)); decodeErr != nil {
		return nil, fmt.Errorf("decode %s: %v", fullpath, decodeErr)
	}
	return entry, nil
}

func (store *YdbStore) DeleteEntry(ctx context.Context, fullpath util.FullPath) (err error) {
	dir, name := fullpath.DirAndName()
	tablePathPrefix, shortDir := store.getPrefix(ctx, &dir)
	q := withPragma(tablePathPrefix, deleteQuery)
	glog.V(4).InfofCtx(ctx, "DeleteEntry %s, tablePathPrefix %s, shortDir %s", fullpath, *tablePathPrefix, *shortDir)
	queryParams := table.NewQueryParameters(
		table.ValueParam("$dir_hash", types.Int64Value(util.HashStringToLong(*shortDir))),
		table.ValueParam("$directory", types.UTF8Value(*shortDir)),
		table.ValueParam("$name", types.UTF8Value(name)))

	return store.doTxOrDB(ctx, q, queryParams, rwQC, nil)
}

func (store *YdbStore) DeleteFolderChildren(ctx context.Context, fullpath util.FullPath) (err error) {
	dir := string(fullpath)
	tablePathPrefix, shortDir := store.getPrefix(ctx, &dir)
	q := withPragma(tablePathPrefix, deleteFolderChildrenQuery)
	queryParams := table.NewQueryParameters(
		table.ValueParam("$dir_hash", types.Int64Value(util.HashStringToLong(*shortDir))),
		table.ValueParam("$directory", types.UTF8Value(*shortDir)))

	return store.doTxOrDB(ctx, q, queryParams, rwQC, nil)
}

func (store *YdbStore) ListDirectoryEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {
	return store.ListDirectoryPrefixedEntries(ctx, dirPath, startFileName, includeStartFile, limit, "", eachEntryFunc)
}

func (store *YdbStore) ListDirectoryPrefixedEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, prefix string, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {
	dir := string(dirPath)
	tablePathPrefix, shortDir := store.getPrefix(ctx, &dir)
	baseInclusive := withPragma(tablePathPrefix, listInclusiveDirectoryQuery)
	baseExclusive := withPragma(tablePathPrefix, listDirectoryQuery)
	var entryCount int64
	var prevFetchedLessThanChunk bool
	for entryCount < limit {
		if prevFetchedLessThanChunk {
			break
		}
		var q *string
		if entryCount == 0 && includeStartFile {
			q = baseInclusive
		} else {
			q = baseExclusive
		}
		rest := limit - entryCount
		chunkLimit := rest
		if chunkLimit > int64(store.maxListChunk) {
			chunkLimit = int64(store.maxListChunk)
		}
		var rowCount int64

		params := table.NewQueryParameters(
			table.ValueParam("$dir_hash", types.Int64Value(util.HashStringToLong(*shortDir))),
			table.ValueParam("$directory", types.UTF8Value(*shortDir)),
			table.ValueParam("$start_name", types.UTF8Value(startFileName)),
			table.ValueParam("$prefix", types.UTF8Value(prefix+"%")),
			table.ValueParam("$limit", types.Uint64Value(uint64(chunkLimit))),
		)

		err := store.doTxOrDB(ctx, q, params, roQC, func(res query.Result) error {
			for rs, err := range res.ResultSets(ctx) {
				if err != nil {
					return err
				}
				for row, err := range rs.Rows(ctx) {
					if err != nil {
						return err
					}

					var name string
					var data []byte
					if scanErr := row.Scan(&name, &data); scanErr != nil {
						return fmt.Errorf("scan %s: %w", dir, scanErr)
					}

					lastFileName = name
					entry := &filer.Entry{FullPath: util.NewFullPath(dir, name)}
					if decodeErr := entry.DecodeAttributesAndChunks(util.MaybeDecompressData(data)); decodeErr != nil {
						return fmt.Errorf("decode entry %s: %w", entry.FullPath, decodeErr)
					}

					resEachEntryFunc, resEachEntryFuncErr := eachEntryFunc(entry)
					if resEachEntryFuncErr != nil {
						return fmt.Errorf("failed to process eachEntryFunc: %w", resEachEntryFuncErr)
					}

					if !resEachEntryFunc {
						return nil
					}

					rowCount++
					entryCount++
					startFileName = lastFileName

					if entryCount >= limit {
						return nil
					}
				}
			}
			return nil
		})
		if err != nil {
			return lastFileName, err
		}

		if rowCount < chunkLimit {
			prevFetchedLessThanChunk = true
		}
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
		return s.CreateTable(ctx, path.Join(prefix, abstract_sql.DEFAULT_TABLE), store.createTableOptions()...)
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
	glog.V(4).InfofCtx(ctx, "deleted table %s", prefix)

	return nil
}

func (store *YdbStore) getPrefix(ctx context.Context, dir *string) (tablePathPrefix *string, shortDir *string) {
	tablePathPrefix = &store.tablePathPrefix
	shortDir = dir
	if !store.SupportBucketTable {
		return
	}

	prefixBuckets := store.dirBuckets + "/"
	glog.V(4).InfofCtx(ctx, "dir: %s, prefixBuckets: %s", *dir, prefixBuckets)
	if strings.HasPrefix(*dir, prefixBuckets) {
		// detect bucket
		bucketAndDir := (*dir)[len(prefixBuckets):]
		glog.V(4).InfofCtx(ctx, "bucketAndDir: %s", bucketAndDir)
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

		if _, found := store.dbs[bucket]; !found {
			glog.V(4).InfofCtx(ctx, "bucket %q not in cache, verifying existence via DescribeTable", bucket)
			tablePath := path.Join(store.tablePathPrefix, bucket, abstract_sql.DEFAULT_TABLE)
			err2 := store.DB.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
				_, err3 := s.DescribeTable(ctx, tablePath)
				return err3
			})
			if err2 != nil {
				glog.V(4).InfofCtx(ctx, "bucket %q not found (DescribeTable %s failed)", bucket, tablePath)
				return
			}
			glog.V(4).InfofCtx(ctx, "bucket %q exists, adding to cache", bucket)
			store.dbs[bucket] = true
		}
		bucketPrefix := path.Join(store.tablePathPrefix, bucket)
		tablePathPrefix = &bucketPrefix
	}
	return
}

func (store *YdbStore) ensureTables(ctx context.Context) error {
	prefixFull := store.tablePathPrefix

	glog.V(4).InfofCtx(ctx, "creating base table %s", prefixFull)
	baseTable := path.Join(prefixFull, abstract_sql.DEFAULT_TABLE)
	if err := store.DB.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		return s.CreateTable(ctx, baseTable, store.createTableOptions()...)
	}); err != nil {
		return fmt.Errorf("failed to create base table %s: %v", baseTable, err)
	}

	glog.V(4).InfofCtx(ctx, "creating bucket tables")
	if store.SupportBucketTable {
		store.dbsLock.Lock()
		defer store.dbsLock.Unlock()
		for bucket := range store.dbs {
			glog.V(4).InfofCtx(ctx, "creating bucket table %s", bucket)
			bucketTable := path.Join(prefixFull, bucket, abstract_sql.DEFAULT_TABLE)
			if err := store.DB.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
				return s.CreateTable(ctx, bucketTable, store.createTableOptions()...)
			}); err != nil {
				glog.ErrorfCtx(ctx, "failed to create bucket table %s: %v", bucketTable, err)
			}
		}
	}
	return nil
}
