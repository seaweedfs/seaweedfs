package actions

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	tr "github.com/seaweedfs/seaweedfs/weed/storage/blockvol/testrunner"
)

// RegisterDatabaseActions registers SQLite and PostgreSQL database actions.
func RegisterDatabaseActions(r *tr.Registry) {
	r.RegisterFunc("sqlite_create_db", tr.TierBlock, sqliteCreateDB)
	r.RegisterFunc("sqlite_insert_rows", tr.TierBlock, sqliteInsertRows)
	r.RegisterFunc("sqlite_count_rows", tr.TierBlock, sqliteCountRows)
	r.RegisterFunc("sqlite_integrity_check", tr.TierBlock, sqliteIntegrityCheck)
	r.RegisterFunc("pgbench_init", tr.TierBlock, pgbenchInit)
	r.RegisterFunc("pgbench_run", tr.TierBlock, pgbenchRun)
	r.RegisterFunc("pgbench_cleanup", tr.TierBlock, pgbenchCleanup)
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
	tmpFile := tempPath(actx, "sqlite_insert.sql")
	cmd := fmt.Sprintf(
		`bash -c 'mkdir -p %s; printf "BEGIN;\n" > %s; for i in $(seq 1 %s); do printf "INSERT INTO %s (data) VALUES (\x27row-%%d\x27);\n" $i; done >> %s; printf "COMMIT;\n" >> %s; sqlite3 %s < %s; rm -f %s'`,
		actx.TempRoot, tmpFile, count, table, tmpFile, tmpFile, path, tmpFile, tmpFile)
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

// pgbenchInit initializes a PostgreSQL instance on a block device for benchmarking.
// Params:
//   - device (required): block device to format and mount
//   - mount (default: "/mnt/pgbench"): mount point
//   - port (default: "5434"): PostgreSQL port
//   - scale (default: "10"): pgbench scale factor
//   - fstype (default: "ext4"): filesystem type
//   - pg_bin (default: "/usr/lib/postgresql/16/bin"): PostgreSQL binary directory
//
// Returns: value = "ready"
func pgbenchInit(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	device := act.Params["device"]
	if device == "" {
		return nil, fmt.Errorf("pgbench_init: device param required")
	}

	mount := paramDefault(act.Params, "mount", "/mnt/pgbench")
	port := paramDefault(act.Params, "port", "5434")
	scale := paramDefault(act.Params, "scale", "10")
	fstype := paramDefault(act.Params, "fstype", "ext4")
	pgBin := paramDefault(act.Params, "pg_bin", "/usr/lib/postgresql/16/bin")

	node, err := getNode(actx, act.Node)
	if err != nil {
		return nil, err
	}

	pgdata := mount + "/pgdata"

	// Format, mount, init PostgreSQL, start, create bench DB, run pgbench -i.
	script := fmt.Sprintf(`set -e
# Stop any previous instance
sudo -u postgres %s/pg_ctl -D %s stop 2>/dev/null || true
sleep 1
# Format and mount
mkfs.%s -F %s > /dev/null 2>&1
mkdir -p %s
mount %s %s
# Init PostgreSQL
mkdir -p %s
chown postgres:postgres %s
sudo -u postgres %s/initdb -D %s > /dev/null 2>&1
echo "listen_addresses = '127.0.0.1'" >> %s/postgresql.conf
echo "port = %s" >> %s/postgresql.conf
echo "unix_socket_directories = '/tmp'" >> %s/postgresql.conf
echo "shared_buffers = 256MB" >> %s/postgresql.conf
echo "effective_cache_size = 512MB" >> %s/postgresql.conf
echo "work_mem = 4MB" >> %s/postgresql.conf
echo "wal_buffers = 16MB" >> %s/postgresql.conf
echo "max_connections = 200" >> %s/postgresql.conf
chown -R postgres:postgres %s
# Start
sudo -u postgres %s/pg_ctl -D %s -l %s/logfile start
sleep 3
# Create DB and init pgbench
sudo -u postgres %s/createdb -h /tmp -p %s benchdb 2>/dev/null || true
sudo -u postgres pgbench -h /tmp -i -s %s -p %s benchdb 2>&1 | tail -3
echo PGBENCH_INIT_OK`,
		pgBin, pgdata,
		fstype, device,
		mount,
		device, mount,
		pgdata,
		pgdata,
		pgBin, pgdata,
		pgdata, port, pgdata, pgdata,
		pgdata, pgdata, pgdata, pgdata, pgdata,
		pgdata,
		pgBin, pgdata, pgdata,
		pgBin, port,
		scale, port,
	)

	actx.Log("  pgbench_init: %s on %s port=%s scale=%s", fstype, device, port, scale)
	stdout, stderr, code, err := node.RunRoot(ctx, fmt.Sprintf("bash -c '%s'", strings.ReplaceAll(script, "'", "'\\''")))
	if err != nil || code != 0 {
		return nil, fmt.Errorf("pgbench_init: code=%d stderr=%s err=%v stdout=%s", code, stderr, err, stdout)
	}
	if !strings.Contains(stdout, "PGBENCH_INIT_OK") {
		return nil, fmt.Errorf("pgbench_init: init did not complete: %s", stdout)
	}

	// Save state for pgbench_run and pgbench_cleanup.
	actx.Vars["__pgbench_mount"] = mount
	actx.Vars["__pgbench_port"] = port
	actx.Vars["__pgbench_pgbin"] = pgBin
	actx.Vars["__pgbench_pgdata"] = pgdata

	return map[string]string{"value": "ready"}, nil
}

// pgbenchRun executes a pgbench workload and returns the TPS.
// Params:
//   - clients (default: "1"): number of concurrent clients
//   - duration (default: "30"): run time in seconds
//   - select_only (default: "false"): if "true", run SELECT-only workload (-S)
//   - port: override port (default: uses __pgbench_port from pgbench_init)
//
// Returns: value = TPS (numeric string, e.g. "1234.56")
func pgbenchRun(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	port := act.Params["port"]
	if port == "" {
		port = actx.Vars["__pgbench_port"]
	}
	if port == "" {
		port = "5434"
	}

	clients := paramDefault(act.Params, "clients", "1")
	duration := paramDefault(act.Params, "duration", "30")
	selectOnly := act.Params["select_only"] == "true"

	node, err := getNode(actx, act.Node)
	if err != nil {
		return nil, err
	}

	cmd := fmt.Sprintf("sudo -u postgres pgbench -h /tmp -c %s -j %s -T %s -p %s",
		clients, clients, duration, port)
	if selectOnly {
		cmd += " -S"
	}
	cmd += " benchdb"

	mode := "TPC-B"
	if selectOnly {
		mode = "SELECT-only"
	}
	actx.Log("  pgbench %s c=%s %ss", mode, clients, duration)
	stdout, stderr, code, err := node.RunRoot(ctx, cmd)
	if err != nil || code != 0 {
		return nil, fmt.Errorf("pgbench_run: code=%d stderr=%s stdout=%s err=%v", code, stderr, stdout, err)
	}

	// Parse TPS from pgbench output. Look for "tps = NNNN.NN" (excluding initial connection).
	tps := parsePgbenchTPS(stdout)
	if tps == "" {
		return nil, fmt.Errorf("pgbench_run: could not parse TPS from output: %s", stdout)
	}

	actx.Log("  pgbench %s c=%s: %s TPS", mode, clients, tps)
	return map[string]string{"value": tps}, nil
}

// pgbenchCleanup stops PostgreSQL and unmounts the device.
// Uses state saved by pgbench_init (__pgbench_mount, __pgbench_pgbin, __pgbench_pgdata).
func pgbenchCleanup(ctx context.Context, actx *tr.ActionContext, act tr.Action) (map[string]string, error) {
	mount := actx.Vars["__pgbench_mount"]
	pgBin := actx.Vars["__pgbench_pgbin"]
	pgdata := actx.Vars["__pgbench_pgdata"]

	if mount == "" {
		mount = "/mnt/pgbench"
	}
	if pgBin == "" {
		pgBin = "/usr/lib/postgresql/16/bin"
	}
	if pgdata == "" {
		pgdata = mount + "/pgdata"
	}

	node, err := getNode(actx, act.Node)
	if err != nil {
		return nil, err
	}

	cmd := fmt.Sprintf("sudo -u postgres %s/pg_ctl -D %s stop 2>/dev/null; sleep 1; umount %s 2>/dev/null; true",
		pgBin, pgdata, mount)
	node.RunRoot(ctx, cmd)
	return nil, nil
}

// parsePgbenchTPS extracts TPS from pgbench output.
// Matches "tps = 1234.567890" (excluding "initial connection time" lines).
var pgbenchTPSPattern = regexp.MustCompile(`tps = ([\d.]+)\s+\(`)

func parsePgbenchTPS(output string) string {
	lines := strings.Split(output, "\n")
	for _, line := range lines {
		// Skip "initial connection time = X.XX ms" lines (no TPS).
		if strings.Contains(line, "initial connection time") && !strings.Contains(line, "tps") {
			continue
		}
		if m := pgbenchTPSPattern.FindStringSubmatch(line); len(m) > 1 {
			return m[1]
		}
	}
	return ""
}
