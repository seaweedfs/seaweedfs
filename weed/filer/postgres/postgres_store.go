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

	"strings"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/filer/abstract_sql"
	"github.com/seaweedfs/seaweedfs/weed/glog"
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
		CreateTableSqlTemplate: `CREATE TABLE IF NOT EXISTS "%s" (
			dirhash     BIGINT,
			name        VARCHAR(65535),
			directory   VARCHAR(65535),
			meta        bytea,
			PRIMARY KEY (dirhash, name)
		);`,
		DropTableSqlTemplate: `drop table "%s"`,
		UpsertQueryTemplate:  upsertQuery,
	}

	sqlUrl := store.buildUrl(user, password, hostname, port, database, schema, sslmode, sslcert, sslkey, sslrootcert, sslcrl, pgbouncerCompatible)

	var dbErr error
	store.DB, dbErr = sql.Open("pgx", sqlUrl)
	if dbErr != nil {
		if store.DB != nil {
			store.DB.Close()
		}
		store.DB = nil
		return fmt.Errorf("can not connect to %s error:%v", sqlUrl, dbErr)
	}

	store.DB.SetMaxIdleConns(maxIdle)
	store.DB.SetMaxOpenConns(maxOpen)
	store.DB.SetConnMaxLifetime(time.Duration(maxLifetimeSeconds) * time.Second)

	if err = store.DB.Ping(); err != nil {
		// check if database does not exist
		if strings.Contains(err.Error(), fmt.Sprintf("database \"%s\" does not exist", database)) {
			glog.V(0).Infof("Database %s does not exist, attempting to create...", database)

			// connect to postgres database to create the new database
			maintUrl := store.buildUrl(user, password, hostname, port, "postgres", "", sslmode, sslcert, sslkey, sslrootcert, sslcrl, pgbouncerCompatible)
			dbMaint, errMaint := sql.Open("pgx", maintUrl)
			if errMaint != nil {
				return fmt.Errorf("connect to maintenance db %s error:%v", maintUrl, errMaint)
			}
			defer dbMaint.Close()

			if _, errExec := dbMaint.Exec(fmt.Sprintf("CREATE DATABASE \"%s\"", database)); errExec != nil {
				return fmt.Errorf("create database %s error:%v", database, errExec)
			}
			glog.V(0).Infof("Created database %s", database)

			// reconnect
			if err = store.DB.Ping(); err != nil {
				return fmt.Errorf("connect to %s errorafter creation :%v", sqlUrl, err)
			}
		} else {
			return fmt.Errorf("connect to %s error:%v", sqlUrl, err)
		}
	}

	glog.V(0).Infof("Connected to %s", sqlUrl)

	if schema != "" {
		createStatement := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS \"%s\"", schema)
		glog.V(0).Infof("Creating schema if not exists: %s", createStatement)
		if _, err = store.DB.Exec(createStatement); err != nil {
			glog.Errorf("create schema %s: %v", schema, err)
			return fmt.Errorf("create schema %s: %v", schema, err)
		}
	}

	createTableSql := store.SqlGenerator.GetSqlCreateTable("filemeta")
	glog.V(0).Infof("Creating table filemeta if not exists: %s", createTableSql)
	if _, err = store.DB.Exec(createTableSql); err != nil {
		glog.Errorf("create table filemeta: %v", err)
		return fmt.Errorf("create table filemeta: %v", err)
	}

	return nil
}

func (store *PostgresStore) buildUrl(user, password, hostname string, port int, database, schema, sslmode, sslcert, sslkey, sslrootcert, sslcrl string, pgbouncerCompatible bool) string {
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
	if password != "" {
		sqlUrl += " password=" + password
	}
	if database != "" {
		sqlUrl += " dbname=" + database
	}
	if schema != "" && !pgbouncerCompatible {
		sqlUrl += " search_path=" + schema
	}
	return sqlUrl
}
