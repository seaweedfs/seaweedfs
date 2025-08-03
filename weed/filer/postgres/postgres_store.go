// Package postgres provides PostgreSQL filer store implementation
// Migrated from github.com/lib/pq to github.com/jackc/pgx for:
// - Active development and support
// - Better performance and PostgreSQL-specific features
// - Improved error handling (no more panics)
// - Built-in logging capabilities
// - Superior SSL certificate support
package postgres

import (
	"database/sql"
	"fmt"
	"strconv"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/filer/abstract_sql"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

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
		configuration.GetString(prefix+"upsertQuery"),
		configuration.GetBool(prefix+"enableUpsert"),
		configuration.GetString(prefix+"username"),
		configuration.GetString(prefix+"password"),
		configuration.GetString(prefix+"hostname"),
		configuration.GetInt(prefix+"port"),
		configuration.GetString(prefix+"database"),
		configuration.GetString(prefix+"schema"),
		configuration.GetString(prefix+"sslmode"),
		configuration.GetString(prefix+"sslcert"),
		configuration.GetString(prefix+"sslkey"),
		configuration.GetString(prefix+"sslrootcert"),
		configuration.GetString(prefix+"sslcrl"),
		configuration.GetBool(prefix+"pgbouncer_compatible"),
		configuration.GetInt(prefix+"connection_max_idle"),
		configuration.GetInt(prefix+"connection_max_open"),
		configuration.GetInt(prefix+"connection_max_lifetime_seconds"),
	)
}

func (store *PostgresStore) initialize(upsertQuery string, enableUpsert bool, user, password, hostname string, port int, database, schema, sslmode, sslcert, sslkey, sslrootcert, sslcrl string, pgbouncerCompatible bool, maxIdle, maxOpen, maxLifetimeSeconds int) (err error) {

	store.SupportBucketTable = false
	if !enableUpsert {
		upsertQuery = ""
	}
	store.SqlGenerator = &SqlGenPostgres{
		CreateTableSqlTemplate: "",
		DropTableSqlTemplate:   `drop table "%s"`,
		UpsertQueryTemplate:    upsertQuery,
	}

	// pgx-optimized connection string with better timeouts and connection handling
	sqlUrl := "connect_timeout=30"

	// PgBouncer compatibility: add prefer_simple_protocol=true when needed
	// This avoids prepared statement issues with PgBouncer's transaction pooling mode
	if pgbouncerCompatible {
		sqlUrl += " prefer_simple_protocol=true"
	}

	if hostname != "" {
		sqlUrl += " host=" + hostname
	}
	if port != 0 {
		sqlUrl += " port=" + strconv.Itoa(port)
	}

	// SSL configuration - pgx provides better SSL support than lib/pq
	if sslmode != "" {
		sqlUrl += " sslmode=" + sslmode
	}
	if sslcert != "" {
		sqlUrl += " sslcert=" + sslcert
	}
	if sslkey != "" {
		sqlUrl += " sslkey=" + sslkey
	}
	if sslrootcert != "" {
		sqlUrl += " sslrootcert=" + sslrootcert
	}
	if sslcrl != "" {
		sqlUrl += " sslcrl=" + sslcrl
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
	if schema != "" && !pgbouncerCompatible {
		sqlUrl += " search_path=" + schema
		adaptedSqlUrl += " search_path=" + schema
	}
	var dbErr error
	store.DB, dbErr = sql.Open("pgx", sqlUrl)
	if dbErr != nil {
		if store.DB != nil {
			store.DB.Close()
		}
		store.DB = nil
		return fmt.Errorf("can not connect to %s error:%v", adaptedSqlUrl, dbErr)
	}

	store.DB.SetMaxIdleConns(maxIdle)
	store.DB.SetMaxOpenConns(maxOpen)
	store.DB.SetConnMaxLifetime(time.Duration(maxLifetimeSeconds) * time.Second)

	if err = store.DB.Ping(); err != nil {
		return fmt.Errorf("connect to %s error:%v", adaptedSqlUrl, err)
	}

	return nil
}
