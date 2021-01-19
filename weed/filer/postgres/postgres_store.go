package postgres

import (
	"database/sql"
	"fmt"

	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/filer/abstract_sql"
	"github.com/chrislusf/seaweedfs/weed/util"
	_ "github.com/lib/pq"
)

const (
	CONNECTION_URL_PATTERN = "host=%s port=%d sslmode=%s connect_timeout=30"
)

type SqlGenPostgres struct {
}

var (
	_ = abstract_sql.SqlGenerator(&SqlGenPostgres{})
)

func (gen *SqlGenPostgres) GetSqlInsert(bucket string) string {
	return "INSERT INTO filemeta (dirhash,name,directory,meta) VALUES($1,$2,$3,$4)"
}

func (gen *SqlGenPostgres) GetSqlUpdate(bucket string) string {
	return "UPDATE filemeta SET meta=$1 WHERE dirhash=$2 AND name=$3 AND directory=$4"
}

func (gen *SqlGenPostgres) GetSqlFind(bucket string) string {
	return "SELECT meta FROM filemeta WHERE dirhash=$1 AND name=$2 AND directory=$3"
}

func (gen *SqlGenPostgres) GetSqlDelete(bucket string) string {
	return "DELETE FROM filemeta WHERE dirhash=$1 AND name=$2 AND directory=$3"
}

func (gen *SqlGenPostgres) GetSqlDeleteFolderChildren(bucket string) string {
	return "DELETE FROM filemeta WHERE dirhash=$1 AND directory=$2"
}

func (gen *SqlGenPostgres) GetSqlListExclusive(bucket string) string {
	return "SELECT NAME, meta FROM filemeta WHERE dirhash=$1 AND name>$2 AND directory=$3 AND name like $4 ORDER BY NAME ASC LIMIT $5"
}

func (gen *SqlGenPostgres) GetSqlListInclusive(bucket string) string {
	return "SELECT NAME, meta FROM filemeta WHERE dirhash=$1 AND name>=$2 AND directory=$3 AND name like $4 ORDER BY NAME ASC LIMIT $5"
}

func init() {
	filer.Stores = append(filer.Stores, &PostgresStore{})
}

type PostgresStore struct {
	abstract_sql.AbstractSqlStore
}

func (store *PostgresStore) GetName() string {
	return "postgres"
}

func (store *PostgresStore) Initialize(configuration util.Configuration, prefix string) (err error) {
	return store.initialize(
		configuration.GetString(prefix+"username"),
		configuration.GetString(prefix+"password"),
		configuration.GetString(prefix+"hostname"),
		configuration.GetInt(prefix+"port"),
		configuration.GetString(prefix+"database"),
		configuration.GetString(prefix+"sslmode"),
		configuration.GetInt(prefix+"connection_max_idle"),
		configuration.GetInt(prefix+"connection_max_open"),
	)
}

func (store *PostgresStore) initialize(user, password, hostname string, port int, database, sslmode string, maxIdle, maxOpen int) (err error) {

	store.SqlGenerator = &SqlGenPostgres{}

	sqlUrl := fmt.Sprintf(CONNECTION_URL_PATTERN, hostname, port, sslmode)
	if user != "" {
		sqlUrl += " user=" + user
	}
	if password != "" {
		sqlUrl += " password=" + password
	}
	if database != "" {
		sqlUrl += " dbname=" + database
	}
	var dbErr error
	store.DB, dbErr = sql.Open("postgres", sqlUrl)
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
