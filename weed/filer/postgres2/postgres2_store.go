// Package postgres2 provides PostgreSQL filer store implementation with bucket support
// Migrated from github.com/lib/pq to github.com/jackc/pgx for:
// - Active development and support
// - Better performance and PostgreSQL-specific features
// - Improved error handling (no more panics)
// - Built-in logging capabilities
// - Superior SSL certificate support
package postgres2

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
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

func (store *PostgresStore2) initialize(createTable, upsertQuery string, enableUpsert bool, user, password, hostname string, port int, database, schema, sslmode, sslcert, sslkey, sslrootcert, sslcrl string, pgbouncerCompatible bool, maxIdle, maxOpen, maxLifetimeSeconds int) (err error) {

	store.SupportBucketTable = true
	if !enableUpsert {
		upsertQuery = ""
	}
	store.SqlGenerator = &postgres.SqlGenPostgres{
		CreateTableSqlTemplate: createTable,
		DropTableSqlTemplate:   `drop table "%s"`,
		UpsertQueryTemplate:    upsertQuery,
	}

	// pgx-optimized connection string with better timeouts and connection handling
	sqlUrl := "connect_timeout=30"

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
	// Parse the DSN into a pgx config so we can configure driver-level
	// options that are no longer accepted as DSN parameters in pgx/v5
	// (notably prefer_simple_protocol, which was removed).
	connConfig, parseErr := pgx.ParseConfig(sqlUrl)
	if parseErr != nil {
		return fmt.Errorf("can not parse connection config for %s error:%v", adaptedSqlUrl, parseErr)
	}

	// PgBouncer compatibility: use the simple query protocol and disable
	// statement caching. This avoids prepared statement issues with
	// PgBouncer's transaction pooling mode. In pgx/v5, prefer_simple_protocol
	// is no longer a DSN parameter; it must be set on the config directly.
	if pgbouncerCompatible {
		connConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
		connConfig.StatementCacheCapacity = 0
		connConfig.DescriptionCacheCapacity = 0
	}

	registeredConnStr := stdlib.RegisterConnConfig(connConfig)
	var dbErr error
	store.DB, dbErr = sql.Open("pgx", registeredConnStr)
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

	if err = store.CreateTable(context.Background(), abstract_sql.DEFAULT_TABLE); err != nil {
		return fmt.Errorf("init table %s: %v", abstract_sql.DEFAULT_TABLE, err)
	}

	return nil
}
