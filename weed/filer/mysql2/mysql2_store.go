package mysql2

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/filer/abstract_sql"
	"github.com/seaweedfs/seaweedfs/weed/filer/mysql"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const (
	CONNECTION_URL_PATTERN = "%s:%s@tcp(%s:%d)/%s?collation=utf8mb4_bin"
)

var _ filer.BucketAware = (*MysqlStore2)(nil)

func init() {
	filer.Stores = append(filer.Stores, &MysqlStore2{})
}

type MysqlStore2 struct {
	abstract_sql.AbstractSqlStore
}

func (store *MysqlStore2) GetName() string {
	return "mysql2"
}

func (store *MysqlStore2) Initialize(configuration util.Configuration, prefix string) (err error) {
	return store.initialize(
		configuration.GetString(prefix+"createTable"),
		configuration.GetString(prefix+"upsertQuery"),
		configuration.GetBool(prefix+"enableUpsert"),
		configuration.GetString(prefix+"username"),
		configuration.GetString(prefix+"password"),
		configuration.GetString(prefix+"hostname"),
		configuration.GetInt(prefix+"port"),
		configuration.GetString(prefix+"database"),
		configuration.GetInt(prefix+"connection_max_idle"),
		configuration.GetInt(prefix+"connection_max_open"),
		configuration.GetInt(prefix+"connection_max_lifetime_seconds"),
		configuration.GetBool(prefix+"interpolateParams"),
	)
}

func (store *MysqlStore2) initialize(createTable, upsertQuery string, enableUpsert bool, user, password, hostname string, port int, database string, maxIdle, maxOpen,
	maxLifetimeSeconds int, interpolateParams bool) (err error) {

	store.SupportBucketTable = true
	if !enableUpsert {
		upsertQuery = ""
	}
	store.SqlGenerator = &mysql.SqlGenMysql{
		CreateTableSqlTemplate: createTable,
		DropTableSqlTemplate:   "DROP TABLE `%s`",
		UpsertQueryTemplate:    upsertQuery,
	}

	sqlUrl := fmt.Sprintf(CONNECTION_URL_PATTERN, user, password, hostname, port, database)
	adaptedSqlUrl := fmt.Sprintf(CONNECTION_URL_PATTERN, user, "<ADAPTED>", hostname, port, database)
	if interpolateParams {
		sqlUrl += "&interpolateParams=true"
		adaptedSqlUrl += "&interpolateParams=true"
	}

	var dbErr error
	store.DB, dbErr = sql.Open("mysql", sqlUrl)
	if dbErr != nil {
		store.DB.Close()
		store.DB = nil
		return fmt.Errorf("can not connect to %s error:%v", adaptedSqlUrl, err)
	}

	store.DB.SetMaxIdleConns(maxIdle)
	store.DB.SetMaxOpenConns(maxOpen)
	store.DB.SetConnMaxLifetime(time.Duration(maxLifetimeSeconds) * time.Second)

	if err = store.DB.Ping(); err != nil {
		return fmt.Errorf("connect to %s error:%v", adaptedSqlUrl, err)
	}

	if err = store.CreateTable(context.Background(), abstract_sql.DEFAULT_TABLE); err != nil && !strings.Contains(err.Error(), "table already exist") {
		return fmt.Errorf("init table %s: %v", abstract_sql.DEFAULT_TABLE, err)
	}

	return nil
}
