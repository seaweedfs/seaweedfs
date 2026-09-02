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
	"fmt"
	"strconv"

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
	// Absent keys keep pooled defaults; an explicit 0 still disables the idle
	// pool / caps. The defaults keep max_idle == max_open: with idle below open,
	// a concurrent burst (s3.lifecycle.run-shard walks 16 shards in parallel)
	// churns a fresh TCP connection per released operation -- open, one query,
	// close -- until the filer exhausts its ephemeral ports
	// ("dial tcp ...:5432/3306: connect: cannot assign requested address").
	// Idle connections only accumulate up to the actual peak concurrency and
	// are recycled by connection_max_lifetime_seconds, so the higher idle
	// default costs a quiet deployment nothing.
	configuration.SetDefault(prefix+"connection_max_idle", 50)
	configuration.SetDefault(prefix+"connection_max_open", 50)
	configuration.SetDefault(prefix+"connection_max_lifetime_seconds", 300)
	// Default on so minimal configs are not exposed to duplicate-key tx
	// poisoning on Postgres; an explicit false still disables it.
	configuration.SetDefault(prefix+"enableUpsert", true)
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
	if createTable == "" {
		createTable = postgres.DefaultCreateTableQuery
	}
	if !enableUpsert {
		upsertQuery = ""
	} else if upsertQuery == "" {
		upsertQuery = postgres.DefaultUpsertQuery
	}
	gen := &postgres.SqlGenPostgres{
		CreateTableSqlTemplate: createTable,
		DropTableSqlTemplate:   `drop table if exists "%s"`,
		UpsertQueryTemplate:    upsertQuery,
	}
	store.SqlGenerator = gen

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
	db, openErr := postgres.OpenPGXDB(sqlUrl, adaptedSqlUrl, pgbouncerCompatible, maxIdle, maxOpen, maxLifetimeSeconds)
	if openErr != nil {
		return openErr
	}
	store.DB = db

	if err = store.CreateTable(context.Background(), abstract_sql.DEFAULT_TABLE); err != nil {
		return fmt.Errorf("init table %s: %v", abstract_sql.DEFAULT_TABLE, err)
	}

	postgres.ConfigureListOrdering(store.DB, gen)

	return nil
}
