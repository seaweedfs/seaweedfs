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
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
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

func isSqlState(err error, code string) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == code
	}
	return false
}

func (store *PostgresStore) initialize(upsertQuery string, enableUpsert bool, user, password, hostname string, port int, database, schema, sslmode, sslcert, sslkey, sslrootcert, sslcrl string, pgbouncerCompatible bool, maxIdle, maxOpen, maxLifetimeSeconds int) (err error) {

	store.SupportBucketTable = false
	if !enableUpsert {
		upsertQuery = ""
	}
	store.SqlGenerator = &SqlGenPostgres{
		Schema: schema,
		CreateTableSqlTemplate: fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %%s (
			dirhash     BIGINT,
			name        VARCHAR(%d),
			directory   VARCHAR(%d),
			meta        bytea,
			PRIMARY KEY (dirhash, name)
		);`, MaxVarcharLength, MaxVarcharLength),
		DropTableSqlTemplate: `drop table %s`,
		UpsertQueryTemplate:  upsertQuery,
	}

	sqlUrl, maskedUrl := store.buildUrl(user, password, hostname, port, database, schema, sslmode, sslcert, sslkey, sslrootcert, sslcrl, pgbouncerCompatible)

	var dbErr error
	store.DB, dbErr = sql.Open("pgx", sqlUrl)
	if dbErr != nil {
		store.DB = nil
		return fmt.Errorf("can not connect to %s error:%v", maskedUrl, dbErr)
	}

	defer func() {
		if err != nil && store.DB != nil {
			store.DB.Close()
			store.DB = nil
		}
	}()

	store.DB.SetMaxIdleConns(maxIdle)
	store.DB.SetMaxOpenConns(maxOpen)
	store.DB.SetConnMaxLifetime(time.Duration(maxLifetimeSeconds) * time.Second)

	if err = store.DB.Ping(); err != nil {
		// check if database does not exist: SQLSTATE 3D000 = invalid_catalog_name
		if isSqlState(err, "3D000") {
			glog.V(0).Infof("Database %s does not exist, attempting to create...", database)

			// connect to postgres database to create the new database
			maintUrl, maintMaskedUrl := store.buildUrl(user, password, hostname, port, "postgres", "", sslmode, sslcert, sslkey, sslrootcert, sslcrl, pgbouncerCompatible)
			dbMaint, errMaint := sql.Open("pgx", maintUrl)
			if errMaint != nil {
				return fmt.Errorf("connect to maintenance db %s error:%v", maintMaskedUrl, errMaint)
			}
			defer dbMaint.Close()

			if _, errExec := dbMaint.Exec(fmt.Sprintf("CREATE DATABASE \"%s\"", database)); errExec != nil {
				// SQLSTATE 42P04 = duplicate_database
				if isSqlState(errExec, "42P04") {
					glog.V(0).Infof("Database %s already exists (race condition ignored)", database)
				} else {
					return fmt.Errorf("create database %s error:%v", database, errExec)
				}
			} else {
				glog.V(0).Infof("Created database %s", database)
			}

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
			// SQLSTATE 42P06 = duplicate_schema
			if isSqlState(err, "42P06") {
				glog.V(0).Infof("Schema %s already exists (ignored)", schema)
			} else {
				glog.Errorf("create schema %s: %v", schema, err)
				return fmt.Errorf("create schema %s: %v", schema, err)
			}
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

type ColumnInfo struct {
	DataType  string
	MaxLength *int
}

func (store *PostgresStore) checkSchema() error {
	var rows *sql.Rows
	var err error
	if store.SqlGenerator.(*SqlGenPostgres).Schema != "" {
		rows, err = store.DB.Query("SELECT column_name, data_type, character_maximum_length FROM information_schema.columns WHERE table_name = 'filemeta' AND table_schema = $1", store.SqlGenerator.(*SqlGenPostgres).Schema)
	} else {
		rows, err = store.DB.Query("SELECT column_name, data_type, character_maximum_length FROM information_schema.columns WHERE table_name = 'filemeta'")
	}
	if err != nil {
		return err
	}
	defer rows.Close()

	existingColumns := make(map[string]ColumnInfo)
	for rows.Next() {
		var columnName, dataType string
		var maxLength *int
		if err := rows.Scan(&columnName, &dataType, &maxLength); err != nil {
			return err
		}
		existingColumns[columnName] = ColumnInfo{DataType: dataType, MaxLength: maxLength}
	}

	sqls, err := store.getSchemaChanges(existingColumns)
	if err != nil {
		return err
	}

	if len(sqls) == 0 {
		return nil
	}

	tx, err := store.DB.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for _, sql := range sqls {
		if _, err := tx.Exec(sql); err != nil {
			return fmt.Errorf("execute sql %s: %v", sql, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit changes: %v", err)
	}

	return nil
}

func (store *PostgresStore) getSchemaChanges(existingColumns map[string]ColumnInfo) ([]string, error) {
	type ExpectedColumn struct {
		Type   string
		Length int
	}

	// Define the expected schema
	expectedColumns := map[string]ExpectedColumn{
		"dirhash":   {"bigint", 0},
		"name":      {"character varying", 0}, // 0 means default to MaxVarcharLength
		"directory": {"character varying", 0},
		"meta":      {"bytea", 0},
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

	var sqls []string


	tableName := "filemeta"
	if gen, ok := store.SqlGenerator.(*SqlGenPostgres); ok {
		tableName = gen.getTableName("filemeta")
	}

	for colName, expected := range expectedColumns {
		current, exists := existingColumns[colName]

		targetLength := expected.Length
		if expected.Type == "character varying" && targetLength == 0 {
			targetLength = MaxVarcharLength
		}

		targetSqlType := expected.Type
		if expected.Type == "character varying" {
			targetSqlType = fmt.Sprintf("VARCHAR(%d)", targetLength)
		}

		if !exists {
			// Column missing, add it
			alterSql := fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s", tableName, colName, targetSqlType)
			glog.V(0).Infof("Adding missing column: %s", alterSql)
			sqls = append(sqls, alterSql)
		} else if current.DataType != expected.Type {
			// Column exists but type mismatch
			if isSafeWidening(current.DataType, expected.Type) {
				alterSql := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s", tableName, colName, targetSqlType)
				glog.V(0).Infof("Widening column type: %s", alterSql)
				sqls = append(sqls, alterSql)
			} else {
				glog.Errorf("Type mismatch for column %s: expected %s, found %s. Automatic migration skipped to prevent data loss.", colName, expected.Type, current.DataType)
				return nil, fmt.Errorf("unsafe type mismatch for column %s: expected %s, found %s", colName, expected.Type, current.DataType)
			}
		} else if expected.Type == "character varying" {
			// Type matches, but check length for VARCHAR
			if current.MaxLength != nil && *current.MaxLength < targetLength {
				alterSql := fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s", tableName, colName, targetSqlType)
				glog.V(0).Infof("Widening column length: %s", alterSql)
				sqls = append(sqls, alterSql)
			}
		}
	}

	return sqls, nil
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
