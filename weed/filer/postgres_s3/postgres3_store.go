package postgres_s3

/*
 * Copyright 2022 Splunk Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/lib/pq"
	_ "github.com/lib/pq"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/filer/abstract_sql"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const (
	CONNECTION_URL_PATTERN = "host=%s port=%d sslmode=%s connect_timeout=30"

	createTablePattern = `CREATE TABLE IF NOT EXISTS "%s" (
		key       varchar(65535) PRIMARY KEY,
		name      varchar(65535),
		prefixes  bigint[],
		meta      bytea
	)`
	createTableIndexPattern = `CREATE INDEX on "%s" USING gin (prefixes);`
	deleteTablePattern      = `DROP TABLE "%s";`
	insertEntryPattern      = `INSERT INTO "%s" (key, name, prefixes, meta) VALUES ($1, $2, $3, $4)
	ON CONFLICT (key)
	DO
	  UPDATE SET meta = EXCLUDED.meta;`
	findEntryPattern      = `SELECT meta FROM "%s" WHERE key = $1`
	deleteEntryPattern    = `DELETE FROM "%s" WHERE key = $1`
	listEntryQueryPattern = `SELECT key, name, isdir, meta FROM
	(
		SELECT key, name, false as isdir, meta FROM "%s"
		WHERE prefixes @> $1 AND cardinality(prefixes) < $5
		AND name __COMPARISON__ $3 AND name LIKE $4 ORDER BY key ASC LIMIT $6
	) s1
	UNION
	(
		SELECT dir, dir, true isdir, NULL::bytea meta FROM
		(
			SELECT DISTINCT split_part(key, '/', $2) AS dir FROM "%s"
			WHERE prefixes @> $1 AND cardinality(prefixes) > $5 - 1 ORDER BY dir ASC
		) t1
		WHERE t1.dir > $3 AND t1.dir LIKE $4 ORDER BY dir ASC
	)
	ORDER BY name ASC LIMIT $6`
	deleteFolderChildrenPattern = `DELETE FROM "%s" WHERE prefixes @> $1 and key like $2`
)

var (
	listEntryExclusivePattern string
	listEntryInclusivePattern string
)

var _ filer.BucketAware = (*PostgresS3Store)(nil)

func init() {
	filer.Stores = append(filer.Stores, &PostgresS3Store{})

	listEntryExclusivePattern = strings.ReplaceAll(listEntryQueryPattern, "__COMPARISON__", ">")
	listEntryInclusivePattern = strings.ReplaceAll(listEntryQueryPattern, "__COMPARISON__", ">=")
}

type PostgresS3Store struct {
	DB                 *sql.DB
	SupportBucketTable bool
	dbs                map[string]bool
	dbsLock            sync.Mutex
}

func (store *PostgresS3Store) GetName() string {
	return "postgres_s3"
}

func (store *PostgresS3Store) Initialize(configuration util.Configuration, prefix string) error {
	return store.initialize(
		configuration.GetString(prefix+"username"),
		configuration.GetString(prefix+"password"),
		configuration.GetString(prefix+"hostname"),
		configuration.GetInt(prefix+"port"),
		configuration.GetString(prefix+"database"),
		configuration.GetString(prefix+"schema"),
		configuration.GetString(prefix+"sslmode"),
		configuration.GetInt(prefix+"connection_max_idle"),
		configuration.GetInt(prefix+"connection_max_open"),
		configuration.GetInt(prefix+"connection_max_lifetime_seconds"),
	)
}

func (store *PostgresS3Store) initialize(user, password, hostname string, port int, database, schema, sslmode string, maxIdle, maxOpen, maxLifetimeSeconds int) (err error) {
	store.SupportBucketTable = true
	sqlUrl := fmt.Sprintf(CONNECTION_URL_PATTERN, hostname, port, sslmode)
	if user != "" {
		sqlUrl += " user=" + user
	}
	adaptedSqlUrl := sqlUrl
	if password != "" {
		sqlUrl += " password=" + password
		adaptedSqlUrl += " password=ADAPTED"
	}
	if database != "" {
		sqlUrl += " dbname=" + database
		adaptedSqlUrl += " dbname=" + database
	}
	if schema != "" {
		sqlUrl += " search_path=" + schema
		adaptedSqlUrl += " search_path=" + schema
	}
	var dbErr error
	store.DB, dbErr = sql.Open("postgres", sqlUrl)
	if dbErr != nil {
		store.DB.Close()
		store.DB = nil
		return fmt.Errorf("can not connect to %s error:%v", adaptedSqlUrl, err)
	}

	store.DB.SetMaxIdleConns(maxIdle)
	store.DB.SetMaxOpenConns(maxOpen)
	store.DB.SetConnMaxLifetime(time.Duration(maxLifetimeSeconds) * time.Second)

	if err = store.DB.Ping(); err != nil {
		return fmt.Errorf("connect to %s error:%v", sqlUrl, err)
	}

	if err = store.CreateTable(context.Background(), abstract_sql.DEFAULT_TABLE); err != nil {
		return fmt.Errorf("init table %s: %v", abstract_sql.DEFAULT_TABLE, err)
	}

	return nil
}

func (store *PostgresS3Store) CanDropWholeBucket() bool {
	return store.SupportBucketTable
}

func (store *PostgresS3Store) OnBucketCreation(bucket string) {
	store.dbsLock.Lock()
	defer store.dbsLock.Unlock()

	store.CreateTable(context.Background(), bucket)

	if store.dbs == nil {
		return
	}
	store.dbs[bucket] = true
}

func (store *PostgresS3Store) OnBucketDeletion(bucket string) {
	store.dbsLock.Lock()
	defer store.dbsLock.Unlock()

	store.deleteTable(context.Background(), bucket)

	if store.dbs == nil {
		return
	}
	delete(store.dbs, bucket)
}

func (store *PostgresS3Store) getTxOrDB(ctx context.Context, fullpath util.FullPath, isForChildren bool) (txOrDB abstract_sql.TxOrDB, bucket string, shortPath util.FullPath, err error) {

	shortPath = fullpath
	bucket = abstract_sql.DEFAULT_TABLE

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

	}

	return
}

func (store *PostgresS3Store) InsertEntry(ctx context.Context, entry *filer.Entry) error {
	db, bucket, shortPath, err := store.getTxOrDB(ctx, entry.FullPath, false)
	if err != nil {
		return fmt.Errorf("findDB %s : %v", entry.FullPath, err)
	}

	if entry.IsDirectory() {
		if isValidBucket(bucket) && !strings.HasPrefix(string(shortPath), "/.uploads") {
			// Ignore directory creations, but not bucket creations or multipart uploads
			return nil
		}
	}

	meta, err := entry.EncodeAttributesAndChunks()
	if err != nil {
		return fmt.Errorf("encode %s: %s", entry.FullPath, err)
	}

	if len(entry.Chunks) > 50 {
		meta = util.MaybeGzipData(meta)
	}

	prefixes := calculatePrefixes(string(shortPath))
	hashedPrefixes := hashPrefixArray(prefixes)
	_, err = db.ExecContext(ctx, fmt.Sprintf(insertEntryPattern, bucket), shortPath, path.Base(string(shortPath)), pq.Array(hashedPrefixes), meta)
	if err != nil {
		return fmt.Errorf("insert/upsert %s: %s", entry.FullPath, err)
	}
	return nil
}

func (store *PostgresS3Store) UpdateEntry(ctx context.Context, entry *filer.Entry) error {
	return store.InsertEntry(ctx, entry)
}

func (store *PostgresS3Store) FindEntry(ctx context.Context, fullpath util.FullPath) (*filer.Entry, error) {

	db, bucket, shortPath, err := store.getTxOrDB(ctx, fullpath, false)
	if err != nil {
		return nil, fmt.Errorf("findDB %s : %v", fullpath, err)
	}

	row := db.QueryRowContext(ctx, fmt.Sprintf(findEntryPattern, bucket), shortPath)

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

func (store *PostgresS3Store) DeleteEntry(ctx context.Context, fullpath util.FullPath) error {
	db, bucket, shortPath, err := store.getTxOrDB(ctx, fullpath, false)
	if err != nil {
		return fmt.Errorf("findDB %s : %v", fullpath, err)
	}

	res, err := db.ExecContext(ctx, fmt.Sprintf(deleteEntryPattern, bucket), shortPath)
	if err != nil {
		return fmt.Errorf("delete %s: %s", fullpath, err)
	}

	_, err = res.RowsAffected()
	if err != nil {
		return fmt.Errorf("delete %s but no rows affected: %s", fullpath, err)
	}

	return nil
}

func (store *PostgresS3Store) DeleteFolderChildren(ctx context.Context, fullpath util.FullPath) (err error) {
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

	sqlText := fmt.Sprintf(deleteFolderChildrenPattern, bucket)
	prefixes := calculatePrefixes(string(shortPath))
	hashedPrefixes := hashPrefixArray(prefixes)
	glog.V(4).Infof("delete %s SQL %s %d", string(shortPath), sqlText, hashedPrefixes)
	res, err := db.ExecContext(ctx, sqlText, pq.Array(hashedPrefixes), string(shortPath)+"/%")
	if err != nil {
		return fmt.Errorf("deleteFolderChildren %s: %s", fullpath, err)
	}

	_, err = res.RowsAffected()
	if err != nil {
		return fmt.Errorf("deleteFolderChildren %s but no rows affected: %s", fullpath, err)
	}
	return nil
}

func (store *PostgresS3Store) ListDirectoryEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {
	return store.ListDirectoryPrefixedEntries(ctx, dirPath, startFileName, includeStartFile, limit, "", nil)
}

func (store *PostgresS3Store) ListDirectoryPrefixedEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, prefix string, eachEntryFunc filer.ListEachEntryFunc) (lastFileName string, err error) {
	db, bucket, shortPath, err := store.getTxOrDB(ctx, dirPath, true)
	if err != nil {
		return lastFileName, fmt.Errorf("findDB %s : %v", dirPath, err)
	}

	slashedShortPath := appendSlash(string(shortPath))
	shortPathParts := len(strings.Split(slashedShortPath, "/"))

	sqlText := fmt.Sprintf(listEntryExclusivePattern, bucket, bucket)
	if includeStartFile {
		sqlText = fmt.Sprintf(listEntryInclusivePattern, bucket, bucket)
	}

	prefixes := calculatePrefixes(string(slashedShortPath))
	hashedPrefixes := hashPrefixArray(prefixes)

	rows, err := db.QueryContext(ctx, sqlText,
		pq.Array(hashedPrefixes),
		shortPathParts,
		startFileName,
		prefix+"%",
		shortPathParts-1,
		limit+1)

	if err != nil {
		return lastFileName, fmt.Errorf("list %s : %v", dirPath, err)
	}
	defer rows.Close()

	for rows.Next() {
		var key string
		var name string
		var isDir bool
		var data []byte
		if err = rows.Scan(&key, &name, &isDir, &data); err != nil {
			glog.V(0).Infof("scan %s : %v", dirPath, err)
			return lastFileName, fmt.Errorf("scan %s: %v", dirPath, err)
		}

		if !isDir {
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
		} else {
			lastFileName = key
			dirName := key
			entry := &filer.Entry{
				FullPath: util.NewFullPath(string(dirPath), dirName),
			}

			entry.Attr.Mode |= os.ModeDir | 0775
			if !eachEntryFunc(entry) {
				break
			}
		}
	}

	return lastFileName, nil
}

func (store *PostgresS3Store) BeginTransaction(ctx context.Context) (context.Context, error) {
	tx, err := store.DB.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
		ReadOnly:  false,
	})
	if err != nil {
		return ctx, err
	}

	return context.WithValue(ctx, "tx", tx), nil
}

func (store *PostgresS3Store) CommitTransaction(ctx context.Context) error {
	if tx, ok := ctx.Value("tx").(*sql.Tx); ok {
		return tx.Commit()
	}
	return nil
}

func (store *PostgresS3Store) RollbackTransaction(ctx context.Context) error {
	if tx, ok := ctx.Value("tx").(*sql.Tx); ok {
		return tx.Rollback()
	}
	return nil
}

func (store *PostgresS3Store) Shutdown() {
	store.DB.Close()
}

func (store *PostgresS3Store) CreateTable(ctx context.Context, bucket string) error {
	_, err := store.DB.ExecContext(ctx, fmt.Sprintf(createTablePattern, bucket))
	if err != nil {
		return fmt.Errorf("create bucket table: %v", err)
	}

	_, err = store.DB.ExecContext(ctx, fmt.Sprintf(createTableIndexPattern, bucket))
	if err != nil {
		return fmt.Errorf("create bucket index: %v", err)
	}
	return err
}

func (store *PostgresS3Store) deleteTable(ctx context.Context, bucket string) error {
	if !store.SupportBucketTable {
		return nil
	}
	_, err := store.DB.ExecContext(ctx, fmt.Sprintf(deleteTablePattern, bucket))
	return err
}

func isValidBucket(bucket string) bool {
	return bucket != abstract_sql.DEFAULT_TABLE && bucket != ""
}

// calculatePrefixes returns the prefixes for a given path. The root prefix "/" is ignored to
// save space in the returned array
func calculatePrefixes(fullPath string) []string {
	res := strings.Split(fullPath, "/")
	maxPrefixes := len(res)

	var retval []string
	for i := 1; i < maxPrefixes; i++ {
		calculatedPrefix := strings.Join(res[0:i], "/") + "/"
		if calculatedPrefix == "/" {
			continue
		}
		retval = append(retval, calculatedPrefix)
	}
	return retval
}

// hashPrefixArray converts input prefix array into int64 hashes
func hashPrefixArray(a []string) []int64 {
	hashed := make([]int64, len(a))
	for i := range a {
		hashed[i] = util.HashStringToLong(a[i])
	}
	return hashed
}

func appendSlash(s string) string {
	if !strings.HasSuffix(s, "/") {
		return s + "/"
	}
	return s
}
