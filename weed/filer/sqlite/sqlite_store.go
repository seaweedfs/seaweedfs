//go:build (linux || darwin || windows) && sqlite

// limited GOOS due to modernc.org/libc/unistd

package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/filer/abstract_sql"
	"github.com/seaweedfs/seaweedfs/weed/filer/mysql"
	"github.com/seaweedfs/seaweedfs/weed/util"
	_ "modernc.org/sqlite"
)

func init() {
	filer.Stores = append(filer.Stores, &SqliteStore{})
}

type SqliteStore struct {
	abstract_sql.AbstractSqlStore
}

func (store *SqliteStore) GetName() string {
	return "sqlite"
}

// sqliteSqlGen is the MySQL generator with SQLite's own page cursor: SQLite
// seeks a row-value comparison on the primary key but scans the OR form.
type sqliteSqlGen struct {
	*mysql.SqlGenMysql
}

func (gen sqliteSqlGen) GetSqlScanPage(tableName string, dirhash int64, name string, limit int) (string, []interface{}) {
	return fmt.Sprintf("SELECT `dirhash`, `name`, `directory`, `meta` FROM `%s` WHERE (`dirhash`, `name`) > (?, ?) ORDER BY `dirhash` ASC, `name` ASC LIMIT ?", tableName),
		[]interface{}{dirhash, name, limit}
}

// ScanEntries offers the snapshot scan only in WAL mode: with a rollback
// journal a long read transaction holds a shared lock and writers fail after
// the busy timeout.
func (store *SqliteStore) ScanEntries(ctx context.Context, dirPath util.FullPath, fn func(ctx context.Context, entry *filer.Entry) error) error {
	var mode string
	if err := store.DB.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&mode); err != nil {
		return fmt.Errorf("%w: journal_mode: %v", filer.ErrUnsupportedEntryScan, err)
	}
	if !strings.EqualFold(mode, "wal") {
		return fmt.Errorf("%w: sqlite journal_mode is %q, the scan needs wal", filer.ErrUnsupportedEntryScan, mode)
	}
	return store.AbstractSqlStore.ScanEntries(ctx, dirPath, fn)
}

func (store *SqliteStore) Initialize(configuration util.Configuration, prefix string) (err error) {
	dbFile := configuration.GetString(prefix + "dbFile")
	createTable := `CREATE TABLE IF NOT EXISTS "%s" (
		dirhash BIGINT,
		name VARCHAR(1000),
		directory TEXT,
		meta BLOB,
		PRIMARY KEY (dirhash, name)
	) WITHOUT ROWID;`
	upsertQuery := `INSERT INTO "%s"(dirhash,name,directory,meta)VALUES(?,?,?,?)
	ON CONFLICT(dirhash,name) DO UPDATE SET
		directory=excluded.directory,
		meta=excluded.meta;
	`
	return store.initialize(
		dbFile,
		createTable,
		upsertQuery,
	)
}

// sqliteDSN keeps the store's two pools on one database and lets a writer that
// meets the other pool's reader wait for it instead of failing outright. A bare
// :memory: is private to each connection, so it has to be named and shared.
func sqliteDSN(dbFile string) string {
	dsn := dbFile
	if dsn == ":memory:" {
		dsn = fmt.Sprintf("file:seaweedfs%d?mode=memory&cache=shared", time.Now().UnixNano())
	}
	separator := "?"
	if strings.Contains(dsn, "?") {
		separator = "&"
	}
	return dsn + separator + "_pragma=busy_timeout(10000)"
}

func (store *SqliteStore) initialize(dbFile, createTable, upsertQuery string) (err error) {

	store.SupportBucketTable = true
	store.SqlGenerator = sqliteSqlGen{&mysql.SqlGenMysql{
		CreateTableSqlTemplate: createTable,
		DropTableSqlTemplate:   "drop table if exists `%s`",
		UpsertQueryTemplate:    upsertQuery,
	}}

	dsn := sqliteDSN(dbFile)

	db, dbErr := sql.Open("sqlite", dsn)
	if dbErr != nil {
		if db != nil {
			db.Close()
		}
		return fmt.Errorf("can not connect to %s error:%v", dbFile, dbErr)
	}

	if err = db.Ping(); err != nil {
		return fmt.Errorf("connect to %s error:%v", dbFile, err)
	}

	if err = store.UseConnectionPools(db, func() (*sql.DB, error) {
		return sql.Open("sqlite", dsn)
	}, 1, 1, 0); err != nil {
		return err
	}
	// The main pool is one connection, which a long scan must not hold.
	if store.ScanDB, err = sql.Open("sqlite", dsn); err != nil {
		return fmt.Errorf("open scan connection to %s: %v", dbFile, err)
	}
	store.ScanDB.SetMaxOpenConns(1)

	if err = store.CreateTable(context.Background(), abstract_sql.DEFAULT_TABLE); err != nil {
		return fmt.Errorf("init table %s: %v", abstract_sql.DEFAULT_TABLE, err)
	}

	return nil
}
