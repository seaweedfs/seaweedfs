package actions

import (
	"context"
	"fmt"
	"strings"

	tr "github.com/seaweedfs/seaweedfs/weed/storage/blockvol/testrunner"
)

// RegisterDatabaseActions registers SQLite database actions.
func RegisterDatabaseActions(r *tr.Registry) {
	r.RegisterFunc("sqlite_create_db", tr.TierBlock, sqliteCreateDB)
	r.RegisterFunc("sqlite_insert_rows", tr.TierBlock, sqliteInsertRows)
	r.RegisterFunc("sqlite_count_rows", tr.TierBlock, sqliteCountRows)
	r.RegisterFunc("sqlite_integrity_check", tr.TierBlock, sqliteIntegrityCheck)
}

// sqliteCreateDB creates a SQLite database with WAL mode and a test table.
// Params: path (required), table (default: "rows")
func sqliteCreateDB(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	path := act.Params["path"]
	if path == "" {
		return nil, fmt.Errorf("sqlite_create_db: path param required")
	}
	table := act.Params["table"]
	if table == "" {
		table = "rows"
	}

	node, err := getNode(actx, act.Node)
	if err != nil {
		return nil, err
	}

	sql := fmt.Sprintf("PRAGMA journal_mode=WAL; CREATE TABLE IF NOT EXISTS %s (id INTEGER PRIMARY KEY, data TEXT, ts DATETIME DEFAULT CURRENT_TIMESTAMP);", table)
	cmd := fmt.Sprintf("sqlite3 %s %q", path, sql)
	_, stderr, code, err := node.RunRoot(ctx, cmd)
	if err != nil || code != 0 {
		return nil, fmt.Errorf("sqlite_create_db: code=%d stderr=%s err=%v", code, stderr, err)
	}

	return nil, nil
}

// sqliteInsertRows inserts rows into a SQLite database.
// Params: path (required), count (default: "100"), table (default: "rows")
func sqliteInsertRows(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	path := act.Params["path"]
	if path == "" {
		return nil, fmt.Errorf("sqlite_insert_rows: path param required")
	}
	count := act.Params["count"]
	if count == "" {
		count = "100"
	}
	table := act.Params["table"]
	if table == "" {
		table = "rows"
	}

	node, err := getNode(actx, act.Node)
	if err != nil {
		return nil, err
	}

	// Generate SQL in a temp file with BEGIN/COMMIT, then pipe to sqlite3.
	// Use bash -c with \x27 for single quotes to avoid quoting issues with sudo.
	tmpFile := "/tmp/sw_sqlite_insert.sql"
	cmd := fmt.Sprintf(
		`bash -c 'printf "BEGIN;\n" > %s; for i in $(seq 1 %s); do printf "INSERT INTO %s (data) VALUES (\x27row-%%d\x27);\n" $i; done >> %s; printf "COMMIT;\n" >> %s; sqlite3 %s < %s; rm -f %s'`,
		tmpFile, count, table, tmpFile, tmpFile, path, tmpFile, tmpFile)
	_, stderr, code, err := node.RunRoot(ctx, cmd)
	if err != nil || code != 0 {
		return nil, fmt.Errorf("sqlite_insert_rows: code=%d stderr=%s err=%v", code, stderr, err)
	}

	return nil, nil
}

// sqliteCountRows returns the row count from a SQLite table.
// Params: path (required), table (default: "rows")
func sqliteCountRows(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	path := act.Params["path"]
	if path == "" {
		return nil, fmt.Errorf("sqlite_count_rows: path param required")
	}
	table := act.Params["table"]
	if table == "" {
		table = "rows"
	}

	node, err := getNode(actx, act.Node)
	if err != nil {
		return nil, err
	}

	cmd := fmt.Sprintf("sqlite3 %s \"SELECT COUNT(*) FROM %s;\"", path, table)
	stdout, stderr, code, err := node.RunRoot(ctx, cmd)
	if err != nil || code != 0 {
		return nil, fmt.Errorf("sqlite_count_rows: code=%d stderr=%s err=%v", code, stderr, err)
	}

	return map[string]string{"value": strings.TrimSpace(stdout)}, nil
}

// sqliteIntegrityCheck runs PRAGMA integrity_check and fails if result != "ok".
// Params: path (required)
func sqliteIntegrityCheck(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	path := act.Params["path"]
	if path == "" {
		return nil, fmt.Errorf("sqlite_integrity_check: path param required")
	}

	node, err := getNode(actx, act.Node)
	if err != nil {
		return nil, err
	}

	cmd := fmt.Sprintf("sqlite3 %s \"PRAGMA integrity_check;\"", path)
	stdout, stderr, code, err := node.RunRoot(ctx, cmd)
	if err != nil || code != 0 {
		return nil, fmt.Errorf("sqlite_integrity_check: code=%d stderr=%s err=%v", code, stderr, err)
	}

	result := strings.TrimSpace(stdout)
	if result != "ok" {
		return nil, fmt.Errorf("sqlite_integrity_check: result=%q (expected 'ok')", result)
	}

	return nil, nil
}
