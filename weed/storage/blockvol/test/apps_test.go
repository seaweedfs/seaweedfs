//go:build integration && apps

package test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestApps(t *testing.T) {
	t.Run("Postgres", testAppsPostgres)
	t.Run("MySQL", testAppsMySQL)
	t.Run("SQLiteWAL", testAppsSQLiteWAL)
	t.Run("QemuBoot", testAppsQemuBoot)
	t.Run("QemuFio", testAppsQemuFio)
	t.Run("DockerOverlay", testAppsDockerOverlay)
	t.Run("LVMStripe", testAppsLVMStripe)
	t.Run("MdRaid1", testAppsMdRaid1)
}

func testAppsPostgres(t *testing.T) {
	requireCmd(t, "pg_isready")
	requireCmd(t, "pgbench")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	tgt, iscsi, host := newTestTarget(t, "500M", "")
	dev := startAndLogin(t, ctx, tgt, iscsi, host)
	mnt := "/tmp/blockvol-pg"
	pgdata := mnt + "/pgdata"

	t.Cleanup(func() {
		cleanCtx, c := context.WithTimeout(context.Background(), 15*time.Second)
		defer c()
		clientNode.RunRoot(cleanCtx, fmt.Sprintf("sudo -u postgres pg_ctl -D %s stop -m fast 2>/dev/null || true", pgdata))
		clientNode.RunRoot(cleanCtx, fmt.Sprintf("umount -f %s 2>/dev/null", mnt))
		clientNode.RunRoot(cleanCtx, fmt.Sprintf("rm -rf %s", mnt))
	})

	// mkfs + mount
	clientNode.RunRoot(ctx, fmt.Sprintf("mkfs.ext4 -F %s", dev))
	clientNode.RunRoot(ctx, fmt.Sprintf("mkdir -p %s", mnt))
	clientNode.RunRoot(ctx, fmt.Sprintf("mount %s %s", dev, mnt))

	// initdb -- use full path since sudo doesn't inherit PG bin dir
	// chown the entire mount point so postgres can write pg.log there
	clientNode.RunRoot(ctx, fmt.Sprintf("chown postgres:postgres %s", mnt))
	clientNode.RunRoot(ctx, fmt.Sprintf("mkdir -p %s", pgdata))
	clientNode.RunRoot(ctx, fmt.Sprintf("chown postgres:postgres %s", pgdata))
	clientNode.RunRoot(ctx, fmt.Sprintf("chmod 700 %s", pgdata))
	_, stderr, code, _ := clientNode.RunRoot(ctx,
		fmt.Sprintf("sudo -u postgres /usr/lib/postgresql/*/bin/initdb -D %s", pgdata))
	if code != 0 {
		t.Fatalf("initdb: code=%d stderr=%s", code, stderr)
	}

	// Start postgres with custom port to avoid conflict with system instance
	_, stderr, code, _ = clientNode.RunRoot(ctx,
		fmt.Sprintf("sudo -u postgres /usr/lib/postgresql/*/bin/pg_ctl -D %s -l %s/pg.log -o '-p 15432' start", pgdata, mnt))
	if code != 0 {
		t.Fatalf("pg_ctl start: code=%d stderr=%s", code, stderr)
	}

	// pgbench init + run
	clientNode.RunRoot(ctx, "sudo -u postgres /usr/lib/postgresql/*/bin/createdb -p 15432 pgbench 2>/dev/null")
	_, stderr, code, _ = clientNode.RunRoot(ctx, "sudo -u postgres pgbench -p 15432 -i pgbench")
	if code != 0 {
		t.Fatalf("pgbench init: code=%d stderr=%s", code, stderr)
	}
	stdout, stderr, code, _ := clientNode.RunRoot(ctx, "sudo -u postgres pgbench -p 15432 -T 30 pgbench")
	if code != 0 {
		t.Fatalf("pgbench run: code=%d stderr=%s", code, stderr)
	}
	// Extract TPS from pgbench output
	for _, line := range strings.Split(stdout, "\n") {
		if strings.Contains(line, "tps") {
			t.Logf("pgbench: %s", strings.TrimSpace(line))
		}
	}

	// Kill9 target
	clientNode.RunRoot(ctx, fmt.Sprintf("sudo -u postgres /usr/lib/postgresql/*/bin/pg_ctl -D %s stop -m fast 2>/dev/null || true", pgdata))
	clientNode.RunRoot(ctx, fmt.Sprintf("umount -f %s 2>/dev/null", mnt))
	iscsi.Logout(ctx, tgt.config.IQN)
	iscsi.CleanupAll(ctx, tgt.config.IQN)
	tgt.Kill9()

	// Restart and verify recovery
	if err := tgt.Start(ctx, false); err != nil {
		t.Fatalf("restart: %v", err)
	}
	dev, err := iscsi.Login(ctx, tgt.config.IQN)
	if err != nil {
		t.Fatalf("re-login: %v", err)
	}
	clientNode.RunRoot(ctx, fmt.Sprintf("mount %s %s", dev, mnt))

	_, stderr, code, _ = clientNode.RunRoot(ctx,
		fmt.Sprintf("sudo -u postgres /usr/lib/postgresql/*/bin/pg_ctl -D %s -l %s/pg.log -o '-p 15432' start", pgdata, mnt))
	if code != 0 {
		t.Fatalf("pg recovery start: code=%d stderr=%s", code, stderr)
	}
	// Verify recovery -- pg_isready should succeed
	_, _, code, _ = clientNode.RunRoot(ctx, "pg_isready -p 15432")
	if code != 0 {
		t.Fatalf("pg_isready failed after recovery")
	}
	t.Log("postgres recovery after Kill9 succeeded")
}

func testAppsMySQL(t *testing.T) {
	requireCmd(t, "mysqld")
	requireCmd(t, "sysbench")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	tgt, iscsi, host := newTestTarget(t, "500M", "")
	dev := startAndLogin(t, ctx, tgt, iscsi, host)
	mnt := "/tmp/blockvol-mysql"
	mysqlData := mnt + "/mysql"
	sock := "/tmp/mysql-blockvol-test.sock"

	t.Cleanup(func() {
		cleanCtx, c := context.WithTimeout(context.Background(), 15*time.Second)
		defer c()
		clientNode.RunRoot(cleanCtx, fmt.Sprintf("mysqladmin -u root -S %s shutdown 2>/dev/null || true", sock))
		time.Sleep(2 * time.Second)
		clientNode.RunRoot(cleanCtx, fmt.Sprintf("umount -f %s 2>/dev/null", mnt))
		clientNode.RunRoot(cleanCtx, fmt.Sprintf("rm -rf %s %s", mnt, sock))
	})

	clientNode.RunRoot(ctx, fmt.Sprintf("mkfs.ext4 -F %s", dev))
	clientNode.RunRoot(ctx, fmt.Sprintf("mkdir -p %s", mnt))
	clientNode.RunRoot(ctx, fmt.Sprintf("mount %s %s", dev, mnt))

	// Stop any system mysqld to avoid port/socket conflicts
	clientNode.RunRoot(ctx, "systemctl stop mysql 2>/dev/null || true")
	clientNode.RunRoot(ctx, fmt.Sprintf("rm -f %s", sock))

	// Initialize MySQL with custom datadir
	// Run as root to avoid AppArmor ownership issues on iSCSI-backed ext4
	clientNode.RunRoot(ctx, fmt.Sprintf("chown -R mysql:mysql %s", mnt))
	_, stderr, code, _ := clientNode.RunRoot(ctx,
		fmt.Sprintf("mysqld --initialize-insecure --datadir=%s --user=root 2>&1", mysqlData))
	if code != 0 {
		t.Fatalf("mysqld init: code=%d stderr=%s", code, stderr)
	}

	// Start mysqld with custom socket and port
	clientNode.RunRoot(ctx, fmt.Sprintf(
		"bash -c 'mysqld --datadir=%s --socket=%s --port=13306 --user=root --skip-grant-tables &'",
		mysqlData, sock))
	// Wait for mysqld to be ready
	for i := 0; i < 30; i++ {
		_, _, code, _ = clientNode.RunRoot(ctx, fmt.Sprintf("mysqladmin -u root -S %s ping 2>/dev/null", sock))
		if code == 0 {
			break
		}
		time.Sleep(time.Second)
	}
	if code != 0 {
		t.Fatalf("mysqld did not start")
	}

	// Sysbench
	clientNode.RunRoot(ctx, fmt.Sprintf("mysql -u root -S %s -e 'CREATE DATABASE IF NOT EXISTS sbtest'", sock))
	_, stderr, code, _ = clientNode.RunRoot(ctx, fmt.Sprintf(
		"sysbench oltp_read_write --mysql-socket=%s --mysql-user=root --db-driver=mysql --tables=4 --table-size=1000 prepare", sock))
	if code != 0 {
		t.Fatalf("sysbench prepare: code=%d stderr=%s", code, stderr)
	}
	stdout, stderr, code, _ := clientNode.RunRoot(ctx, fmt.Sprintf(
		"sysbench oltp_read_write --mysql-socket=%s --mysql-user=root --db-driver=mysql --tables=4 --table-size=1000 --time=30 run", sock))
	if code != 0 {
		t.Fatalf("sysbench run: code=%d stderr=%s", code, stderr)
	}
	for _, line := range strings.Split(stdout, "\n") {
		if strings.Contains(line, "transactions:") || strings.Contains(line, "queries:") {
			t.Logf("sysbench: %s", strings.TrimSpace(line))
		}
	}

	// Clean shutdown
	clientNode.RunRoot(ctx, fmt.Sprintf("mysqladmin -u root -S %s shutdown", sock))
	t.Log("MySQL + sysbench test passed")
}

func testAppsSQLiteWAL(t *testing.T) {
	requireCmd(t, "sqlite3")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	tgt, iscsi, host := newTestTarget(t, "100M", "")
	dev := startAndLogin(t, ctx, tgt, iscsi, host)
	mnt := "/tmp/blockvol-sqlite"

	t.Cleanup(func() {
		cleanCtx, c := context.WithTimeout(context.Background(), 10*time.Second)
		defer c()
		clientNode.RunRoot(cleanCtx, fmt.Sprintf("umount -f %s 2>/dev/null", mnt))
		clientNode.RunRoot(cleanCtx, fmt.Sprintf("rm -rf %s", mnt))
	})

	clientNode.RunRoot(ctx, fmt.Sprintf("mkfs.ext4 -F %s", dev))
	clientNode.RunRoot(ctx, fmt.Sprintf("mkdir -p %s && mount %s %s", mnt, dev, mnt))

	// Create DB in WAL mode, insert 10K rows via batched inserts
	// Use a script file to avoid shell quoting issues over SSH
	script := fmt.Sprintf(`bash -c '
set -e
DB="%s/test.db"
rm -f "$DB" "$DB-wal" "$DB-shm"
sqlite3 "$DB" "PRAGMA journal_mode=WAL; CREATE TABLE t(id INTEGER PRIMARY KEY, val TEXT);"
for i in $(seq 1 100); do
  SQL="BEGIN;"
  for j in $(seq 1 100); do
    n=$(( (i-1)*100 + j ))
    SQL="${SQL} INSERT INTO t(val) VALUES('"'"'row_${n}'"'"');"
  done
  SQL="${SQL} COMMIT;"
  sqlite3 "$DB" "$SQL"
done
sqlite3 "$DB" "SELECT count(*) FROM t;"
'`, mnt)

	stdout, stderr, code, _ := clientNode.RunRoot(ctx, script)
	if code != 0 {
		t.Fatalf("sqlite3 failed: code=%d stderr=%s", code, stderr)
	}
	// Last line of stdout should be the count
	lines := strings.Split(strings.TrimSpace(stdout), "\n")
	lastLine := lines[len(lines)-1]
	if lastLine != "10000" {
		t.Fatalf("expected 10000 rows, got last line: %q (full output: %s)", lastLine, stdout)
	}
	t.Log("SQLite WAL: 10K rows inserted and verified")
}

func testAppsQemuBoot(t *testing.T) {
	requireCmd(t, "qemu-system-x86_64")
	t.Skip("QEMU boot test requires Alpine ISO setup")
}

func testAppsQemuFio(t *testing.T) {
	requireCmd(t, "qemu-system-x86_64")
	t.Skip("QEMU fio test requires VM image setup")
}

func testAppsDockerOverlay(t *testing.T) {
	requireCmd(t, "docker")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	tgt, iscsi, host := newTestTarget(t, "500M", "")
	dev := startAndLogin(t, ctx, tgt, iscsi, host)
	mnt := "/tmp/blockvol-docker"

	t.Cleanup(func() {
		cleanCtx, c := context.WithTimeout(context.Background(), 15*time.Second)
		defer c()
		clientNode.RunRoot(cleanCtx, fmt.Sprintf("umount -f %s 2>/dev/null", mnt))
		clientNode.RunRoot(cleanCtx, fmt.Sprintf("rm -rf %s", mnt))
	})

	clientNode.RunRoot(ctx, fmt.Sprintf("mkfs.ext4 -F %s", dev))
	clientNode.RunRoot(ctx, fmt.Sprintf("mkdir -p %s && mount %s %s", mnt, dev, mnt))

	// Write a file via Docker bind-mount to the iSCSI-backed filesystem
	clientNode.RunRoot(ctx, "docker pull alpine:latest 2>/dev/null")
	testContent := "blockvol-docker-integration-test"
	stdout, stderr, code, _ := clientNode.RunRoot(ctx,
		fmt.Sprintf("docker run --rm -v %s:/data alpine:latest sh -c 'echo %s > /data/docker-test.txt && cat /data/docker-test.txt'",
			mnt, testContent))
	if code != 0 {
		t.Fatalf("docker run failed: code=%d stderr=%s stdout=%s", code, stderr, stdout)
	}
	if !strings.Contains(stdout, testContent) {
		t.Fatalf("expected %q in output, got: %s", testContent, stdout)
	}

	// Verify file persists on host
	stdout2, _, _, _ := clientNode.RunRoot(ctx, fmt.Sprintf("cat %s/docker-test.txt", mnt))
	if !strings.Contains(stdout2, testContent) {
		t.Fatalf("file not persisted: %s", stdout2)
	}
	t.Log("Docker on iSCSI-backed ext4 passed")
}

func testAppsLVMStripe(t *testing.T) {
	requireCmd(t, "pvcreate")
	t.Skip("LVM stripe test requires 2 iSCSI volumes")
}

func testAppsMdRaid1(t *testing.T) {
	requireCmd(t, "mdadm")
	t.Skip("MD RAID-1 test requires 2 iSCSI volumes")
}

// requireCmd is defined in fault_helpers.go (integration tag).
