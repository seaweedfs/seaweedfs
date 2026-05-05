package postgres

import (
	"database/sql"

	"github.com/seaweedfs/seaweedfs/weed/util/pgxutil"
)

// OpenPGXDB is a thin alias kept for callers (notably the postgres2 filer
// store) that already depend on this entry point. New code should use
// pgxutil.OpenDB directly.
func OpenPGXDB(sqlUrl, adaptedSqlUrl string, pgbouncerCompatible bool, maxIdle, maxOpen, maxLifetimeSeconds int) (*sql.DB, error) {
	return pgxutil.OpenDB(sqlUrl, adaptedSqlUrl, pgbouncerCompatible, maxIdle, maxOpen, maxLifetimeSeconds)
}
