package abstract_sql

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3bucket"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"strings"
	"sync"
)

type SqlGenerator interface {
	GetSqlInsert(tableName string) string
	GetSqlUpdate(tableName string) string
	GetSqlFind(tableName string) string
	GetSqlDelete(tableName string) string
	GetSqlDeleteFolderChildren(tableName string) string
	GetSqlListExclusive(tableName string) string
	GetSqlListInclusive(tableName string) string
	GetSqlCreateTable(tableName string) string
	GetSqlDropTable(tableName string) string
}

type AbstractSqlStore struct {
	SqlGenerator
	DB                 *sql.DB
	SupportBucketTable bool
	dbs                map[string]bool
	dbsLock            sync.Mutex
}

var _ filer.BucketAware = (*AbstractSqlStore)(nil)

func (store *AbstractSqlStore) CanDropWholeBucket() bool {
	return store.SupportBucketTable
}
func (store *AbstractSqlStore) OnBucketCreation(bucket string) {
	store.dbsLock.Lock()
	defer store.dbsLock.Unlock()

	store.CreateTable(context.Background(), bucket)

	if store.dbs == nil {
		return
	}
	store.dbs[bucket] = true
}
func (store *AbstractSqlStore) OnBucketDeletion(bucket string) {
	store.dbsLock.Lock()
	defer store.dbsLock.Unlock()

	store.deleteTable(context.Background(), bucket)

	if store.dbs == nil {
		return
	}
	delete(store.dbs, bucket)
}

const (
	DEFAULT_TABLE = "filemeta"
)

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

func (store *AbstractSqlStore) getTxOrDB(ctx context.Context, fullpath util.FullPath, isForChildren bool) (txOrDB TxOrDB, bucket string, shortPath util.FullPath, err error) {

	shortPath = fullpath
	bucket = DEFAULT_TABLE

	if tx, ok := ctx.Value("tx").(*sql.Tx); ok {
		txOrDB = tx
	} else {
		txOrDB = store.DB
	}

	if !store.SupportBucketTable {
		return
	}

	if !strings.HasPrefix(string(fullpath), "/buckets/") {
		return
	}

	// detect bucket
	bucketAndObjectKey := string(fullpath)[len("/buckets/"):]
	t := strings.Index(bucketAndObjectKey, "/")
	if t < 0 && !isForChildren {
		return
	}
	bucket = bucketAndObjectKey
	shortPath = "/"
	if t > 0 {
		bucket = bucketAndObjectKey[:t]
		shortPath = util.FullPath(bucketAndObjectKey[t:])
	}

	if isValidBucket(bucket) {
		store.dbsLock.Lock()
		defer store.dbsLock.Unlock()

		if store.dbs == nil {
			store.dbs = make(map[string]bool)
		}

		if _, found := store.dbs[bucket]; !found {
			if err = store.CreateTable(ctx, bucket); err == nil {
				store.dbs[bucket] = true
			}
		}

	} else {
		err = fmt.Errorf("invalid bucket name %s", bucket)
	}

	return
}

func (store *AbstractSqlStore) InsertEntry(ctx context.Context, entry *filer.Entry) (err error) {

	db, bucket, shortPath, err := store.getTxOrDB(ctx, entry.FullPath, false)
	if err != nil {
		return fmt.Errorf("findDB %s : %v", entry.FullPath, err)
	}

	dir, name := shortPath.DirAndName()
	meta, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		return fmt.Errorf("encode %s: %s", entry.FullPath, err)
	}

	if len(entry.GetChunks()) > filer.CountEntryChunksForGzip {
		meta = util.MaybeGzipData(meta)
	}
	sqlInsert := "insert"
	res, err := db.ExecContext(ctx, store.GetSqlInsert(bucket), util.HashStringToLong(dir), name, dir, meta)
	if err != nil && strings.Contains(strings.ToLower(err.Error()), "duplicate entry") {
		// now the insert failed possibly due to duplication constraints
		sqlInsert = "falls back to update"
		glog.V(1).Infof("insert %s %s: %v", entry.FullPath, sqlInsert, err)
		res, err = db.ExecContext(ctx, store.GetSqlUpdate(bucket), meta, util.HashStringToLong(dir), name, dir)
	}
	if err != nil {
		return fmt.Errorf("%s %s: %s", sqlInsert, entry.FullPath, err)
	}

	_, err = res.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s %s but no rows affected: %s", sqlInsert, entry.FullPath, err)
	}

	return nil
}

func (store *AbstractSqlStore) UpdateEntry(ctx context.Context, entry *filer.Entry) (err error) {

	db, bucket, shortPath, err := store.getTxOrDB(ctx, entry.FullPath, false)
	if err != nil {
		return fmt.Errorf("findDB %s : %v", entry.FullPath, err)
	}

	dir, name := shortPath.DirAndName()
	meta, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		return fmt.Errorf("encode %s: %s", entry.FullPath, err)
	}

	res, err := db.ExecContext(ctx, store.GetSqlUpdate(bucket), meta, util.HashStringToLong(dir), name, dir)
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

	db, bucket, shortPath, err := store.getTxOrDB(ctx, fullpath, false)
	if err != nil {
		return nil, fmt.Errorf("findDB %s : %v", fullpath, err)
	}

	dir, name := shortPath.DirAndName()
	row := db.QueryRowContext(ctx, store.GetSqlFind(bucket), util.HashStringToLong(dir), name, dir)

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

	db, bucket, shortPath, err := store.getTxOrDB(ctx, fullpath, false)
	if err != nil {
		return fmt.Errorf("findDB %s : %v", fullpath, err)
	}

	dir, name := shortPath.DirAndName()

	res, err := db.ExecContext(ctx, store.GetSqlDelete(bucket), util.HashStringToLong(dir), name, dir)
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

	db, bucket, shortPath, err := store.getTxOrDB(ctx, fullpath, true)
	if err != nil {
		return fmt.Errorf("findDB %s : %v", fullpath, err)
	}

	if isValidBucket(bucket) && shortPath == "/" {
		if err = store.deleteTable(ctx, bucket); err == nil {
			store.dbsLock.Lock()
			delete(store.dbs, bucket)
			store.dbsLock.Unlock()
			return nil
		} else {
			return err
		}
	}

	glog.V(4).Infof("delete %s SQL %s %d", string(shortPath), store.GetSqlDeleteFolderChildren(bucket), util.HashStringToLong(string(shortPath)))
	res, err := db.ExecContext(ctx, store.GetSqlDeleteFolderChildren(bucket), util.HashStringToLong(string(shortPath)), string(shortPath))
	if err != nil {
		return fmt.Errorf("deleteFolderChildren %s: %s", fullpath, err)
	}

	_, err = res.RowsAffected()
	if err != nil {
		return fmt.Errorf("deleteFolderChildren %s but no rows affected: %s", fullpath, err)
	}
	return nil
}

func (store *AbstractSqlStore) ListDirectoryPrefixedEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, prefix string, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {

	db, bucket, shortPath, err := store.getTxOrDB(ctx, dirPath, true)
	if err != nil {
		return lastFileName, fmt.Errorf("findDB %s : %v", dirPath, err)
	}

	sqlText := store.GetSqlListExclusive(bucket)
	if includeStartFile {
		sqlText = store.GetSqlListInclusive(bucket)
	}

	rows, err := db.QueryContext(ctx, sqlText, util.HashStringToLong(string(shortPath)), startFileName, string(shortPath), prefix+"%", limit+1)
	if err != nil {
		return lastFileName, fmt.Errorf("list %s : %v", dirPath, err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var data []byte
		if err = rows.Scan(&name, &data); err != nil {
			glog.V(0).Infof("scan %s : %v", dirPath, err)
			return lastFileName, fmt.Errorf("scan %s: %v", dirPath, err)
		}
		lastFileName = name

		entry := &filer.Entry{
			FullPath: util.NewFullPath(string(dirPath), name),
		}
		if err = entry.DecodeAttributesAndChunks(util.MaybeDecompressData(data)); err != nil {
			glog.V(0).Infof("scan decode %s : %v", entry.FullPath, err)
			return lastFileName, fmt.Errorf("scan decode %s : %v", entry.FullPath, err)
		}

		if !eachEntryFunc(entry) {
			break
		}

	}

	return lastFileName, nil
}

func (store *AbstractSqlStore) ListDirectoryEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {
	return store.ListDirectoryPrefixedEntries(ctx, dirPath, startFileName, includeStartFile, limit, "", nil)
}

func (store *AbstractSqlStore) Shutdown() {
	store.DB.Close()
}

func isValidBucket(bucket string) bool {
	if s3bucket.VerifyS3BucketName(bucket) != nil {
		return false
	}
	return bucket != DEFAULT_TABLE && bucket != ""
}

func (store *AbstractSqlStore) CreateTable(ctx context.Context, bucket string) error {
	if !store.SupportBucketTable {
		return nil
	}
	_, err := store.DB.ExecContext(ctx, store.SqlGenerator.GetSqlCreateTable(bucket))
	return err
}

func (store *AbstractSqlStore) deleteTable(ctx context.Context, bucket string) error {
	if !store.SupportBucketTable {
		return nil
	}
	_, err := store.DB.ExecContext(ctx, store.SqlGenerator.GetSqlDropTable(bucket))
	return err
}
