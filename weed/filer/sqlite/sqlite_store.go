//go:build (linux || darwin || windows) && sqlite
// +build linux darwin windows
// +build sqlite

// limited GOOS due to modernc.org/libc/unistd

package sqlite

import (
	"context"
	"database/sql"
	"fmt"

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

func (store *SqliteStore) initialize(dbFile, createTable, upsertQuery string) (err error) {

	store.SupportBucketTable = true
	store.SqlGenerator = &mysql.SqlGenMysql{
		CreateTableSqlTemplate: createTable,
		DropTableSqlTemplate:   "drop table `%s`",
		UpsertQueryTemplate:    upsertQuery,
	}

	var dbErr error
	store.DB, dbErr = sql.Open("sqlite", dbFile)
	if dbErr != nil {
		if store.DB != nil {
			store.DB.Close()
			store.DB = nil
		}
		return fmt.Errorf("can not connect to %s error:%v", dbFile, dbErr)
	}

	if err = store.DB.Ping(); err != nil {
		return fmt.Errorf("connect to %s error:%v", dbFile, err)
	}

	store.DB.SetMaxOpenConns(1)

	if err = store.CreateTable(context.Background(), abstract_sql.DEFAULT_TABLE); err != nil {
		return fmt.Errorf("init table %s: %v", abstract_sql.DEFAULT_TABLE, err)
	}

	return nil
}
