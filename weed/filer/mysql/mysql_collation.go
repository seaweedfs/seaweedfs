package mysql

import (
	"database/sql"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/filer/abstract_sql"
	"github.com/seaweedfs/seaweedfs/weed/glog"
)

// ConfigureListOrdering switches the list queries to BINARY byte ordering when
// the live filemeta.name collation is not binary, so S3 ListObjectsV2 stays
// lexicographic. The fallback costs a filesort, so warn the operator to fix it.
func ConfigureListOrdering(db *sql.DB, gen *SqlGenMysql) {
	collation, isBinary, err := nameColumnCollation(db)
	if err != nil {
		glog.V(1).Infof("mysql: skip filemeta.name collation check: %v", err)
		return
	}
	if isBinary {
		return
	}
	gen.ForceBinaryCollation = true
	glog.Warningf("mysql: filemeta.name collation %q is not binary, so S3 list order is not byte-lexicographic and clients that merge sorted listings may report spurious diffs. Falling back to a slower BINARY sort; convert the name column to a *_bin collation (e.g. utf8mb4_bin) for correct, indexed ordering.", collation)
}

func nameColumnCollation(db *sql.DB) (collation string, isBinary bool, err error) {
	row := db.QueryRow("SELECT COLLATION_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = 'name'", abstract_sql.DEFAULT_TABLE)
	var c sql.NullString
	if err = row.Scan(&c); err != nil {
		return "", false, err
	}
	return c.String, isBinaryCollation(c.String), nil
}

// isBinaryCollation reports whether a collation orders by byte value: a NULL
// collation (BINARY column) or any *_bin collation.
func isBinaryCollation(collation string) bool {
	c := strings.ToLower(strings.TrimSpace(collation))
	return c == "" || c == "binary" || strings.HasSuffix(c, "_bin")
}
