//go:build integration

package test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// pgHelper manages a Postgres instance lifecycle on a remote/WSL2 node.
type pgHelper struct {
	node   *Node
	dev    string // iSCSI block device (e.g. /dev/sdb)
	mnt    string // mount point
	pgdata string // PGDATA directory
	pgPort int    // Postgres port (avoid conflicts)
}

// newPgHelper creates a pgHelper. dev must be a valid block device path.
func newPgHelper(node *Node, dev string, pgPort int) *pgHelper {
	mnt := "/tmp/blockvol-pgcrash"
	return &pgHelper{
		node:   node,
		dev:    dev,
		mnt:    mnt,
		pgdata: mnt + "/pgdata",
		pgPort: pgPort,
	}
}

// InitFS formats the device and initializes Postgres.
func (p *pgHelper) InitFS(ctx context.Context) error {
	// mkfs
	_, stderr, code, _ := p.node.RunRoot(ctx, fmt.Sprintf("mkfs.ext4 -F %s", p.dev))
	if code != 0 {
		return fmt.Errorf("mkfs: code=%d stderr=%s", code, stderr)
	}

	// mount
	if err := p.Mount(ctx); err != nil {
		return err
	}

	// Prepare pgdata
	p.node.RunRoot(ctx, fmt.Sprintf("chown postgres:postgres %s", p.mnt))
	p.node.RunRoot(ctx, fmt.Sprintf("mkdir -p %s", p.pgdata))
	p.node.RunRoot(ctx, fmt.Sprintf("chown postgres:postgres %s", p.pgdata))
	p.node.RunRoot(ctx, fmt.Sprintf("chmod 700 %s", p.pgdata))

	return p.InitDB(ctx)
}

// InitDB runs initdb in pgdata.
func (p *pgHelper) InitDB(ctx context.Context) error {
	_, stderr, code, _ := p.node.RunRoot(ctx,
		fmt.Sprintf("sudo -u postgres /usr/lib/postgresql/*/bin/initdb -D %s", p.pgdata))
	if code != 0 {
		return fmt.Errorf("initdb: code=%d stderr=%s", code, stderr)
	}
	return nil
}

// Start starts Postgres.
func (p *pgHelper) Start(ctx context.Context) error {
	_, stderr, code, _ := p.node.RunRoot(ctx,
		fmt.Sprintf("sudo -u postgres /usr/lib/postgresql/*/bin/pg_ctl -D %s -l %s/pg.log -o '-p %d' start",
			p.pgdata, p.mnt, p.pgPort))
	if code != 0 {
		return fmt.Errorf("pg_ctl start: code=%d stderr=%s", code, stderr)
	}
	return nil
}

// Stop stops Postgres with fast shutdown.
func (p *pgHelper) Stop(ctx context.Context) error {
	_, _, code, _ := p.node.RunRoot(ctx,
		fmt.Sprintf("sudo -u postgres /usr/lib/postgresql/*/bin/pg_ctl -D %s stop -m fast 2>/dev/null", p.pgdata))
	if code != 0 {
		return fmt.Errorf("pg_ctl stop: code=%d", code)
	}
	return nil
}

// IsReady waits up to timeout for pg_isready to succeed.
func (p *pgHelper) IsReady(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		_, _, code, _ := p.node.RunRoot(ctx, fmt.Sprintf("pg_isready -p %d", p.pgPort))
		if code == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			time.Sleep(1 * time.Second)
		}
	}
	return fmt.Errorf("pg_isready timeout after %v", timeout)
}

// PgBench runs pgbench for the given duration. Returns transaction count.
func (p *pgHelper) PgBench(ctx context.Context, seconds int) (int, error) {
	stdout, stderr, code, _ := p.node.RunRoot(ctx,
		fmt.Sprintf("sudo -u postgres pgbench -p %d -T %d pgbench", p.pgPort, seconds))
	if code != 0 {
		return 0, fmt.Errorf("pgbench: code=%d stderr=%s", code, stderr)
	}
	// Parse TPS from output
	for _, line := range strings.Split(stdout, "\n") {
		if strings.Contains(line, "number of transactions actually processed:") {
			parts := strings.Split(line, ":")
			if len(parts) >= 2 {
				nStr := strings.TrimSpace(parts[1])
				// Remove any non-numeric suffix
				nStr = strings.Split(nStr, "/")[0]
				nStr = strings.TrimSpace(nStr)
				n, err := strconv.Atoi(nStr)
				if err == nil {
					return n, nil
				}
			}
		}
	}
	return 0, nil // couldn't parse but pgbench succeeded
}

// PgBenchInit initializes pgbench tables.
func (p *pgHelper) PgBenchInit(ctx context.Context) error {
	p.node.RunRoot(ctx, fmt.Sprintf(
		"sudo -u postgres /usr/lib/postgresql/*/bin/createdb -p %d pgbench 2>/dev/null", p.pgPort))
	_, stderr, code, _ := p.node.RunRoot(ctx,
		fmt.Sprintf("sudo -u postgres pgbench -p %d -i pgbench", p.pgPort))
	if code != 0 {
		return fmt.Errorf("pgbench init: code=%d stderr=%s", code, stderr)
	}
	return nil
}

// CountHistory returns SELECT count(*) FROM pgbench_history.
func (p *pgHelper) CountHistory(ctx context.Context) (int, error) {
	stdout, stderr, code, _ := p.node.RunRoot(ctx,
		fmt.Sprintf("sudo -u postgres psql -p %d -t -c 'SELECT count(*) FROM pgbench_history' pgbench", p.pgPort))
	if code != 0 {
		return 0, fmt.Errorf("count history: code=%d stderr=%s", code, stderr)
	}
	nStr := strings.TrimSpace(stdout)
	n, err := strconv.Atoi(nStr)
	if err != nil {
		return 0, fmt.Errorf("parse count: %q: %w", nStr, err)
	}
	return n, nil
}

// Mount mounts the device at mnt. Runs e2fsck -y first to repair any
// filesystem inconsistencies from incomplete replication.
func (p *pgHelper) Mount(ctx context.Context) error {
	p.node.RunRoot(ctx, fmt.Sprintf("mkdir -p %s", p.mnt))
	// e2fsck -y auto-fixes errors (returns 0=clean, 1=corrected, 2=corrected+reboot).
	// Only fail on exit code >= 4 (uncorrectable).
	_, stderr, code, _ := p.node.RunRoot(ctx, fmt.Sprintf("e2fsck -y %s 2>/dev/null", p.dev))
	if code >= 4 {
		return fmt.Errorf("e2fsck: code=%d stderr=%s", code, stderr)
	}
	_, stderr, code, _ = p.node.RunRoot(ctx, fmt.Sprintf("mount %s %s", p.dev, p.mnt))
	if code != 0 {
		return fmt.Errorf("mount: code=%d stderr=%s", code, stderr)
	}
	return nil
}

// Unmount force-unmounts the mount point.
func (p *pgHelper) Unmount(ctx context.Context) {
	p.node.RunRoot(ctx, fmt.Sprintf("umount -f %s 2>/dev/null", p.mnt))
}

// Cleanup stops postgres, unmounts, and removes mount point.
func (p *pgHelper) Cleanup(ctx context.Context) {
	p.Stop(ctx)
	p.Unmount(ctx)
	p.node.RunRoot(ctx, fmt.Sprintf("rm -rf %s", p.mnt))
}
