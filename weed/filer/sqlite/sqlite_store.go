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
	store.SqlGenerator = &mysql.SqlGenMysql{
		CreateTableSqlTemplate: createTable,
		DropTableSqlTemplate:   "drop table if exists `%s`",
		UpsertQueryTemplate:    upsertQuery,
	}

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

	if err = store.CreateTable(context.Background(), abstract_sql.DEFAULT_TABLE); err != nil {
		return fmt.Errorf("init table %s: %v", abstract_sql.DEFAULT_TABLE, err)
	}

	return nil
}
