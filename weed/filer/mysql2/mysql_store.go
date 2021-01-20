package mysql2

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/filer/abstract_sql"
	"github.com/chrislusf/seaweedfs/weed/filer/mysql"
	"github.com/chrislusf/seaweedfs/weed/util"
	_ "github.com/go-sql-driver/mysql"
)

const (
	CONNECTION_URL_PATTERN = "%s:%s@tcp(%s:%d)/%s?charset=utf8"
)

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

func (store *MysqlStore2) initialize(createTable, user, password, hostname string, port int, database string, maxIdle, maxOpen,
	maxLifetimeSeconds int, interpolateParams bool) (err error) {

	store.SupportBucketTable = true
	store.SqlGenerator = &mysql.SqlGenMysql{
		CreateTableSqlTemplate: createTable,
		DropTableSqlTemplate:   "drop table %s",
	}

	sqlUrl := fmt.Sprintf(CONNECTION_URL_PATTERN, user, password, hostname, port, database)
	if interpolateParams {
		sqlUrl += "&interpolateParams=true"
	}

	var dbErr error
	store.DB, dbErr = sql.Open("mysql", sqlUrl)
	if dbErr != nil {
		store.DB.Close()
		store.DB = nil
		return fmt.Errorf("can not connect to %s error:%v", sqlUrl, err)
	}

	store.DB.SetMaxIdleConns(maxIdle)
	store.DB.SetMaxOpenConns(maxOpen)
	store.DB.SetConnMaxLifetime(time.Duration(maxLifetimeSeconds) * time.Second)

	if err = store.DB.Ping(); err != nil {
		return fmt.Errorf("connect to %s error:%v", sqlUrl, err)
	}

	return nil
}
