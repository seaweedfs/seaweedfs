package mysql

import (
	"database/sql"
	"fmt"

	"github.com/HZ89/seaweedfs/weed/filer2"
	"github.com/HZ89/seaweedfs/weed/filer2/abstract_sql"
	"github.com/HZ89/seaweedfs/weed/util"
	_ "github.com/go-sql-driver/mysql"
)

const (
	CONNECTION_URL_PATTERN = "%s:%s@tcp(%s:%d)/%s?charset=utf8"
)

func init() {
	filer2.Stores = append(filer2.Stores, &MysqlStore{})
}

type MysqlStore struct {
	abstract_sql.AbstractSqlStore
}

func (store *MysqlStore) GetName() string {
	return "mysql"
}

func (store *MysqlStore) Initialize(configuration util.Configuration) (err error) {
	return store.initialize(
		configuration.GetString("username"),
		configuration.GetString("password"),
		configuration.GetString("hostname"),
		configuration.GetInt("port"),
		configuration.GetString("database"),
		configuration.GetInt("connection_max_idle"),
		configuration.GetInt("connection_max_open"),
	)
}

func (store *MysqlStore) initialize(user, password, hostname string, port int, database string, maxIdle, maxOpen int) (err error) {

	store.SqlInsert = "INSERT INTO filemeta (dirhash,name,directory,meta) VALUES(?,?,?,?)"
	store.SqlUpdate = "UPDATE filemeta SET meta=? WHERE dirhash=? AND name=? AND directory=?"
	store.SqlFind = "SELECT meta FROM filemeta WHERE dirhash=? AND name=? AND directory=?"
	store.SqlDelete = "DELETE FROM filemeta WHERE dirhash=? AND name=? AND directory=?"
	store.SqlListExclusive = "SELECT NAME, meta FROM filemeta WHERE dirhash=? AND name>? AND directory=? ORDER BY NAME ASC LIMIT ?"
	store.SqlListInclusive = "SELECT NAME, meta FROM filemeta WHERE dirhash=? AND name>=? AND directory=? ORDER BY NAME ASC LIMIT ?"

	sqlUrl := fmt.Sprintf(CONNECTION_URL_PATTERN, user, password, hostname, port, database)
	var dbErr error
	store.DB, dbErr = sql.Open("mysql", sqlUrl)
	if dbErr != nil {
		store.DB.Close()
		store.DB = nil
		return fmt.Errorf("can not connect to %s error:%v", sqlUrl, err)
	}

	store.DB.SetMaxIdleConns(maxIdle)
	store.DB.SetMaxOpenConns(maxOpen)

	if err = store.DB.Ping(); err != nil {
		return fmt.Errorf("connect to %s error:%v", sqlUrl, err)
	}

	return nil
}
