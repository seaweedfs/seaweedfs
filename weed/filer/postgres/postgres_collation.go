package postgres

import (
	"database/sql"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/filer/abstract_sql"
	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// ConfigureListOrdering switches the list queries to COLLATE "C" when the live
// filemeta.name collation is locale-aware, so S3 ListObjectsV2 stays
// lexicographic. The fallback costs a sort, so warn the operator to fix it.
func ConfigureListOrdering(db *sql.DB, gen *SqlGenPostgres) {
	if isCockroachDB(db) {
		// CockroachDB string comparison is already byte-ordered, so the
		// fallback is redundant; older versions also reject COLLATE "C".
		return
	}
	collation, isBinary, err := nameColumnCollation(db)
	if err != nil {
		glog.V(1).Infof("postgres: skip filemeta.name collation check: %v", err)
		return
	}
	if isBinary {
		return
	}
	gen.ForceBinaryCollation = true
	glog.Warningf(`postgres: filemeta.name collation %q is not byte-ordered, so S3 list order is not byte-lexicographic and clients that merge sorted listings may report spurious diffs. Falling back to a slower COLLATE "C" sort; declare the name column COLLATE "C" (or use a C/C.UTF-8 database collation) for correct, indexed ordering.`, collation)
}

// isCockroachDB reports whether db is a CockroachDB backend, whose version()
// string carries "CockroachDB". On any error we assume real Postgres.
func isCockroachDB(db *sql.DB) bool {
	var version string
	if err := db.QueryRow("SELECT version()").Scan(&version); err != nil {
		glog.V(1).Infof("postgres: skip CockroachDB detection: %v", err)
		return false
	}
	return strings.Contains(version, "CockroachDB")
}

func nameColumnCollation(db *sql.DB) (collation string, isBinary bool, err error) {
	// information_schema reports NULL for a column on the database default, so
	// fall back to datcollate for the effective ordering.
	row := db.QueryRow(`SELECT
		(SELECT collation_name FROM information_schema.columns
			WHERE table_schema = current_schema() AND table_name = $1 AND column_name = 'name'),
		(SELECT datcollate FROM pg_database WHERE datname = current_database())`, abstract_sql.DEFAULT_TABLE)
	var columnCollation, dbCollate sql.NullString
	if err = row.Scan(&columnCollation, &dbCollate); err != nil {
		return "", false, err
	}
	effective := columnCollation.String
	if !columnCollation.Valid || effective == "" {
		effective = dbCollate.String
	}
	return effective, isByteOrderedCollation(effective), nil
}

// isByteOrderedCollation reports whether a Postgres locale sorts by byte value
// (C, POSIX, C.UTF-8) rather than locale order (en_US.UTF-8, ICU, ...).
func isByteOrderedCollation(name string) bool {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "c", "posix", "c.utf-8", "c.utf8":
		return true
	}
	return false
}
