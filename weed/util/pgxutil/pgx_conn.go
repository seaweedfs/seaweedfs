// Package pgxutil holds shared helpers for opening *sql.DB handles backed
// by jackc/pgx, used by the postgres filer and credential stores so they
// stay consistent on connection setup, mTLS handling and PgBouncer
// compatibility.
package pgxutil

import (
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// DSNOptions describes the parameters used to assemble a libpq-style
// keyword=value connection string. Empty fields are omitted.
type DSNOptions struct {
	Hostname    string
	Port        int
	User        string
	Password    string
	Database    string
	Schema      string
	SSLMode     string
	SSLCert     string
	SSLKey      string
	SSLRootCert string
	SSLCRL      string
	// PgBouncerCompatible toggles two things at the caller's site: the
	// search_path is omitted from the DSN (PgBouncer rejects it under
	// transaction pooling) and OpenDB is told to use the simple query
	// protocol.
	PgBouncerCompatible bool
}

// BuildDSN assembles two libpq-style connection strings from opts: the
// real DSN passed to pgx, and an adapted copy with the password redacted
// for use in error messages and logs.
func BuildDSN(opts DSNOptions) (dsn, adaptedDSN string) {
	dsn = "connect_timeout=30"
	if opts.Hostname != "" {
		dsn += " host=" + opts.Hostname
	}
	if opts.Port != 0 {
		dsn += " port=" + strconv.Itoa(opts.Port)
	}
	if opts.SSLMode != "" {
		dsn += " sslmode=" + opts.SSLMode
	}
	if opts.SSLCert != "" {
		dsn += " sslcert=" + opts.SSLCert
	}
	if opts.SSLKey != "" {
		dsn += " sslkey=" + opts.SSLKey
	}
	if opts.SSLRootCert != "" {
		dsn += " sslrootcert=" + opts.SSLRootCert
	}
	if opts.SSLCRL != "" {
		dsn += " sslcrl=" + opts.SSLCRL
	}
	if opts.User != "" {
		dsn += " user=" + opts.User
	}
	adaptedDSN = dsn
	if opts.Password != "" {
		dsn += " password=" + opts.Password
		adaptedDSN += " password=ADAPTED"
	}
	if opts.Database != "" {
		dsn += " dbname=" + opts.Database
		adaptedDSN += " dbname=" + opts.Database
	}
	if opts.Schema != "" && !opts.PgBouncerCompatible {
		dsn += " search_path=" + opts.Schema
		adaptedDSN += " search_path=" + opts.Schema
	}
	return dsn, adaptedDSN
}

// OpenDB parses dsn into a pgx ConnConfig, applies PgBouncer compatibility
// settings when requested, opens a *sql.DB via stdlib.OpenDB, and verifies
// it with Ping.
//
// In pgx/v5 the prefer_simple_protocol DSN parameter was removed, so simple
// protocol mode must be configured on the ConnConfig via DefaultQueryExecMode.
// We use stdlib.OpenDB(config) rather than RegisterConnConfig + sql.Open so
// we don't leak entries in stdlib's global connection config map on either
// success or failure paths.
//
// adaptedDSN is used only for error messages (the caller is expected to
// have redacted any password).
func OpenDB(dsn, adaptedDSN string, pgbouncerCompatible bool, maxIdle, maxOpen, maxLifetimeSeconds int) (*sql.DB, error) {
	connConfig, parseErr := pgx.ParseConfig(dsn)
	if parseErr != nil {
		return nil, fmt.Errorf("can not parse connection config for %s error:%v", adaptedDSN, parseErr)
	}

	// PgBouncer compatibility: use the simple query protocol and disable
	// statement caching. This avoids prepared statement issues with
	// PgBouncer's transaction pooling mode.
	if pgbouncerCompatible {
		connConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
		connConfig.StatementCacheCapacity = 0
		connConfig.DescriptionCacheCapacity = 0
	}

	db := stdlib.OpenDB(*connConfig)
	db.SetMaxIdleConns(maxIdle)
	db.SetMaxOpenConns(maxOpen)
	db.SetConnMaxLifetime(time.Duration(maxLifetimeSeconds) * time.Second)

	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("connect to %s error:%v", adaptedDSN, err)
	}

	return db, nil
}
