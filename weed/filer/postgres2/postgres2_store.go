package postgres2

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"

	_ "github.com/lib/pq"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/filer/abstract_sql"
	"github.com/seaweedfs/seaweedfs/weed/filer/postgres"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var _ filer.BucketAware = (*PostgresStore2)(nil)

func init() {
	filer.Stores = append(filer.Stores, &PostgresStore2{})
}

type PostgresStore2 struct {
	abstract_sql.AbstractSqlStore
}

func (store *PostgresStore2) GetName() string {
	return "postgres2"
}

func (store *PostgresStore2) Initialize(configuration util.Configuration, prefix string) (err error) {
	return store.initialize(
		configuration.GetString(prefix+"createTable"),
		configuration.GetString(prefix+"upsertQuery"),
		configuration.GetBool(prefix+"enableUpsert"),
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

func (store *PostgresStore2) initialize(createTable, upsertQuery string, enableUpsert bool, user, password, hostname string, port int, database, schema, sslmode string, maxIdle, maxOpen, maxLifetimeSeconds int) (err error) {

	store.SupportBucketTable = true
	if !enableUpsert {
		upsertQuery = ""
	}
	store.SqlGenerator = &postgres.SqlGenPostgres{
		CreateTableSqlTemplate: createTable,
		DropTableSqlTemplate:   `drop table "%s"`,
		UpsertQueryTemplate:    upsertQuery,
	}

	sqlUrl := "connect_timeout=30"
	if hostname != "" {
		sqlUrl += " host=" + hostname
	}
	if port != 0 {
		sqlUrl += " port=" + strconv.Itoa(port)
	}
	if sslmode != "" {
		sqlUrl += " sslmode=" + sslmode
	}
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
		return fmt.Errorf("connect to %s error:%v", adaptedSqlUrl, err)
	}

	if err = store.CreateTable(context.Background(), abstract_sql.DEFAULT_TABLE); err != nil {
		return fmt.Errorf("init table %s: %v", abstract_sql.DEFAULT_TABLE, err)
	}

	return nil
}
