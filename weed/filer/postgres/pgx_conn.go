package postgres

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// OpenPGXDB parses the given DSN into a pgx ConnConfig, applies PgBouncer
// compatibility settings when requested, registers the config with the
// pgx/v5 stdlib driver, opens a *sql.DB, and verifies it with Ping.
//
// In pgx/v5 the prefer_simple_protocol DSN parameter was removed, so simple
// protocol mode must be configured on the ConnConfig via
// DefaultQueryExecMode. This helper centralizes that handling and, on any
// failure after RegisterConnConfig, unregisters the config so we do not leak
// entries in stdlib's global connection config map.
//
// adaptedSqlUrl is used only for error messages (the caller is expected to
// have redacted any password).
func OpenPGXDB(sqlUrl, adaptedSqlUrl string, pgbouncerCompatible bool, maxIdle, maxOpen, maxLifetimeSeconds int) (*sql.DB, error) {
	connConfig, parseErr := pgx.ParseConfig(sqlUrl)
	if parseErr != nil {
		return nil, fmt.Errorf("can not parse connection config for %s error:%v", adaptedSqlUrl, parseErr)
	}

	// PgBouncer compatibility: use the simple query protocol and disable
	// statement caching. This avoids prepared statement issues with
	// PgBouncer's transaction pooling mode.
	if pgbouncerCompatible {
		connConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
		connConfig.StatementCacheCapacity = 0
		connConfig.DescriptionCacheCapacity = 0
	}

	registeredConnStr := stdlib.RegisterConnConfig(connConfig)
	db, dbErr := sql.Open("pgx", registeredConnStr)
	if dbErr != nil {
		stdlib.UnregisterConnConfig(registeredConnStr)
		return nil, fmt.Errorf("can not connect to %s error:%v", adaptedSqlUrl, dbErr)
	}

	db.SetMaxIdleConns(maxIdle)
	db.SetMaxOpenConns(maxOpen)
	db.SetConnMaxLifetime(time.Duration(maxLifetimeSeconds) * time.Second)

	if err := db.Ping(); err != nil {
		db.Close()
		stdlib.UnregisterConnConfig(registeredConnStr)
		return nil, fmt.Errorf("connect to %s error:%v", adaptedSqlUrl, err)
	}

	return db, nil
}
