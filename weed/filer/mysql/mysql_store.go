package mysql

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/filer/abstract_sql"
	"github.com/chrislusf/seaweedfs/weed/util"
	_ "github.com/go-sql-driver/mysql"
)

const (
	CONNECTION_URL_PATTERN = "%s:%s@tcp(%s:%d)/%s?charset=utf8"
)

type SqlGenMysql struct {
	createTableSqlTemplate string
	dropTableSqlTemplate string
}

var (
	_ = abstract_sql.SqlGenerator(&SqlGenMysql{})
)

func (gen *SqlGenMysql) GetSqlInsert(bucket string) string {
	return "INSERT INTO filemeta (dirhash,name,directory,meta) VALUES(?,?,?,?)"
}

func (gen *SqlGenMysql) GetSqlUpdate(bucket string) string {
	return "UPDATE filemeta SET meta=? WHERE dirhash=? AND name=? AND directory=?"
}

func (gen *SqlGenMysql) GetSqlFind(bucket string) string {
	return "SELECT meta FROM filemeta WHERE dirhash=? AND name=? AND directory=?"
}

func (gen *SqlGenMysql) GetSqlDelete(bucket string) string {
	return "DELETE FROM filemeta WHERE dirhash=? AND name=? AND directory=?"
}

func (gen *SqlGenMysql) GetSqlDeleteFolderChildren(bucket string) string {
	return "DELETE FROM filemeta WHERE dirhash=? AND directory=?"
}

func (gen *SqlGenMysql) GetSqlListExclusive(bucket string) string {
	return "SELECT NAME, meta FROM filemeta WHERE dirhash=? AND name>? AND directory=? AND name like ? ORDER BY NAME ASC LIMIT ?"
}

func (gen *SqlGenMysql) GetSqlListInclusive(bucket string) string {
	return "SELECT NAME, meta FROM filemeta WHERE dirhash=? AND name>=? AND directory=? AND name like ? ORDER BY NAME ASC LIMIT ?"
}

func (gen *SqlGenMysql) GetSqlCreateTable(bucket string) string {
	return fmt.Sprintf(gen.createTableSqlTemplate, bucket)
}

func (gen *SqlGenMysql) GetSqlDropTable(bucket string) string {
	return fmt.Sprintf(gen.dropTableSqlTemplate, bucket)
}

func init() {
	filer.Stores = append(filer.Stores, &MysqlStore{})
}

type MysqlStore struct {
	abstract_sql.AbstractSqlStore
}

func (store *MysqlStore) GetName() string {
	return "mysql"
}

func (store *MysqlStore) Initialize(configuration util.Configuration, prefix string) (err error) {
	return store.initialize(
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

func (store *MysqlStore) initialize(user, password, hostname string, port int, database string, maxIdle, maxOpen,
	maxLifetimeSeconds int, interpolateParams bool) (err error) {

	store.SupportBucketTable = false
	store.SqlGenerator = &SqlGenMysql{
		createTableSqlTemplate: "",
		dropTableSqlTemplate: "drop table %s",
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
