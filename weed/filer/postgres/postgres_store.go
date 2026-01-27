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

const MaxVarcharLength = 65535

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
		CreateTableSqlTemplate: fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%%s" (
			dirhash     BIGINT,
			name        VARCHAR(%d),
			directory   VARCHAR(%d),
			meta        bytea,
			PRIMARY KEY (dirhash, name)
		);`, MaxVarcharLength, MaxVarcharLength),
		DropTableSqlTemplate: `drop table "%s"`,
		UpsertQueryTemplate:  upsertQuery,
	}

	sqlUrl, maskedUrl := store.buildUrl(user, password, hostname, port, database, schema, sslmode, sslcert, sslkey, sslrootcert, sslcrl, pgbouncerCompatible)

	var dbErr error
	store.DB, dbErr = sql.Open("pgx", sqlUrl)
	if dbErr != nil {
		if store.DB != nil {
			store.DB.Close()
		}
		store.DB = nil
		return fmt.Errorf("can not connect to %s error:%v", maskedUrl, dbErr)
	}

	store.DB.SetMaxIdleConns(maxIdle)
	store.DB.SetMaxOpenConns(maxOpen)
	store.DB.SetConnMaxLifetime(time.Duration(maxLifetimeSeconds) * time.Second)

	if err = store.DB.Ping(); err != nil {
		// check if database does not exist
		if strings.Contains(err.Error(), fmt.Sprintf("database \"%s\" does not exist", database)) {
			glog.V(0).Infof("Database %s does not exist, attempting to create...", database)

			// connect to postgres database to create the new database
			maintUrl, maintMaskedUrl := store.buildUrl(user, password, hostname, port, "postgres", "", sslmode, sslcert, sslkey, sslrootcert, sslcrl, pgbouncerCompatible)
			dbMaint, errMaint := sql.Open("pgx", maintUrl)
			if errMaint != nil {
				return fmt.Errorf("connect to maintenance db %s error:%v", maintMaskedUrl, errMaint)
			}
			defer dbMaint.Close()

			if _, errExec := dbMaint.Exec(fmt.Sprintf("CREATE DATABASE \"%s\"", database)); errExec != nil {
				return fmt.Errorf("create database %s error:%v", database, errExec)
			}
			glog.V(0).Infof("Created database %s", database)

			// reconnect
			if err = store.DB.Ping(); err != nil {
				return fmt.Errorf("connect to %s errorafter creation :%v", maskedUrl, err)
			}
		} else {
			return fmt.Errorf("connect to %s error:%v", maskedUrl, err)
		}
	}

	glog.V(0).Infof("Connected to %s", maskedUrl)

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

	if err = store.checkSchema(); err != nil {
		glog.Errorf("check schema filemeta: %v", err)
		return fmt.Errorf("check schema filemeta: %v", err)
	}

	return nil
}

func (store *PostgresStore) checkSchema() error {
	// Define the expected schema
	// key: column name, value: expected SQL type (must be compatible with what Postgres reports)
	expectedColumns := map[string]string{
		"dirhash":   "bigint",
		"name":      "character varying", // VARCHAR maps to character varying in information_schema
		"directory": "character varying",
		"meta":      "bytea",
	}

	// Helper to check if type widening is safe
	isSafeWidening := func(current, expected string) bool {
		switch current {
		case "smallint":
			return expected == "integer" || expected == "bigint"
		case "integer":
			return expected == "bigint"
		case "real":
			return expected == "double precision"
		}
		return false
	}

	rows, err := store.DB.Query("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'filemeta'")
	if err != nil {
		return err
	}
	defer rows.Close()

	existingColumns := make(map[string]string)
	for rows.Next() {
		var columnName, dataType string
		if err := rows.Scan(&columnName, &dataType); err != nil {
			return err
		}
		existingColumns[columnName] = dataType
	}

	for colName, expectedType := range expectedColumns {
		currentType, exists := existingColumns[colName]

		if !exists {
			// Column missing, add it
			alterSql := fmt.Sprintf("ALTER TABLE filemeta ADD COLUMN %s %s", colName, expectedType)
			if colName == "name" || colName == "directory" {
				// Handle varchar length if needed
				alterSql = fmt.Sprintf("ALTER TABLE filemeta ADD COLUMN %s VARCHAR(%d)", colName, MaxVarcharLength)
			}
			glog.V(0).Infof("Adding missing column: %s", alterSql)
			if _, err := store.DB.Exec(alterSql); err != nil {
				return fmt.Errorf("failed to add column %s: %v", colName, err)
			}
		} else if currentType != expectedType {
			// Column exists but type mismatch
			if isSafeWidening(currentType, expectedType) {
				alterSql := fmt.Sprintf("ALTER TABLE filemeta ALTER COLUMN %s TYPE %s", colName, expectedType)
				if colName == "name" || colName == "directory" {
					alterSql = fmt.Sprintf("ALTER TABLE filemeta ALTER COLUMN %s TYPE VARCHAR(%d)", colName, MaxVarcharLength)
				}
				glog.V(0).Infof("Widening column type: %s", alterSql)
				if _, err := store.DB.Exec(alterSql); err != nil {
					return fmt.Errorf("failed to widen column %s: %v", colName, err)
				}
			} else {
				// Unsafe or unknown mismatch, log warning/error
				glog.Errorf("Type mismatch for column %s: expected %s, found %s. Automatic migration skipped to prevent data loss.", colName, expectedType, currentType)
				// Returning error here would prevent startup, which might be desired for data safety
				return fmt.Errorf("unsafe type mismatch for column %s: expected %s, found %s", colName, expectedType, currentType)
			}
		}
	}


	return nil
}

func (store *PostgresStore) buildUrl(user, password, hostname string, port int, database, schema, sslmode, sslcert, sslkey, sslrootcert, sslcrl string, pgbouncerCompatible bool) (url string, maskedUrl string) {
	// pgx-optimized connection string with better timeouts and connection handling
	url = "connect_timeout=30"
	maskedUrl = "connect_timeout=30"

	// PgBouncer compatibility: add prefer_simple_protocol=true when needed
	// This avoids prepared statement issues with PgBouncer's transaction pooling mode
	if pgbouncerCompatible {
		url += " prefer_simple_protocol=true"
		maskedUrl += " prefer_simple_protocol=true"
	}

	if hostname != "" {
		url += " host=" + hostname
		maskedUrl += " host=" + hostname
	}
	if port != 0 {
		url += " port=" + strconv.Itoa(port)
		maskedUrl += " port=" + strconv.Itoa(port)
	}

	// SSL configuration - pgx provides better SSL support than lib/pq
	if sslmode != "" {
		url += " sslmode=" + sslmode
		maskedUrl += " sslmode=" + sslmode
	}
	if sslcert != "" {
		url += " sslcert=" + sslcert
		maskedUrl += " sslcert=" + sslcert
	}
	if sslkey != "" {
		url += " sslkey=" + sslkey
		maskedUrl += " sslkey=" + sslkey
	}
	if sslrootcert != "" {
		url += " sslrootcert=" + sslrootcert
		maskedUrl += " sslrootcert=" + sslrootcert
	}
	if sslcrl != "" {
		url += " sslcrl=" + sslcrl
		maskedUrl += " sslcrl=" + sslcrl
	}
	if user != "" {
		url += " user=" + user
		maskedUrl += " user=" + user
	}
	if password != "" {
		url += " password=" + password
		maskedUrl += " password=*****"
	}
	if database != "" {
		url += " dbname=" + database
		maskedUrl += " dbname=" + database
	}
	if schema != "" && !pgbouncerCompatible {
		url += " search_path=" + schema
		maskedUrl += " search_path=" + schema
	}
	return url, maskedUrl
}
