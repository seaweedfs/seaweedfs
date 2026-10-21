package abstract_sql

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var _ filer.EntryScanner = (*AbstractSqlStore)(nil)

// scanTable is one table to read during a scan together with the path its
// directory column is relative to: "" for the default table, whose rows hold
// full directories, and "/buckets/<bucket>" for a per-bucket table.
type scanTable struct {
	table string
	base  string
	// cachedOnly marks a table that only a stale bucket cache remembers: its
	// bucket directory was already gone from the snapshot, so a missing table
	// means it was dropped meanwhile, not that rows are absent.
	cachedOnly bool
}

// scanPageSize is how many rows one keyset page of a scan fetches.
var scanPageSize = 1000

// SetScanPageSizeForTest lets tests drive the keyset cursor across pages. It
// returns the previous size.
func SetScanPageSizeForTest(n int) int {
	prev := scanPageSize
	scanPageSize = n
	return prev
}

// ScanEntries implements filer.EntryScanner: every table that can hold an
// entry below dirPath is read inside one REPEATABLE READ, read-only
// transaction, in primary-key pages. All rows are seen as of one instant; a
// rename committed meanwhile shows up under exactly one of its names. The
// transaction takes no row locks and does not block writers; it does hold the
// tables it has read against DDL, so a bucket deleted while the scan runs
// drops its table once the scan ends. A page's cursor is closed before its
// entries go to fn, so fn may read the store through the context it receives
// (which carries the transaction) on the same connection.
func (store *AbstractSqlStore) ScanEntries(ctx context.Context, dirPath util.FullPath, fn func(ctx context.Context, entry *filer.Entry) error) error {
	// Read before the transaction holds a connection: OnBucketCreation keeps
	// dbsLock while it waits for a connection of its own, so taking the lock
	// while holding a connection could deadlock on an exhausted pool.
	cachedBuckets := store.cachedBucketNames()

	tx, err := store.beginSnapshotTx(ctx)
	if err != nil {
		return err
	}
	// Read-only: nothing to commit, and a rollback releases the snapshot.
	defer tx.Rollback()

	tables, err := store.scanTables(ctx, tx, dirPath, cachedBuckets)
	if err != nil {
		return err
	}
	// Reads made through this context (getTxOrDB, kvDB) use the transaction
	// and therefore the snapshot.
	txCtx := context.WithValue(ctx, "tx", tx)
	// The root's own row may live in a table the scan does not read (a
	// bucket directory is in the default table, its contents in the bucket's).
	if root, err := store.readRowInSnapshot(txCtx, tx, dirPath); err != nil {
		return err
	} else if root != nil {
		if err := fn(txCtx, root); err != nil {
			return err
		}
	}
	for _, t := range tables {
		if err := store.scanOneTable(txCtx, tx, t, dirPath, fn); err != nil {
			return err
		}
	}
	return nil
}

// readRowInSnapshot reads one entry's row inside the transaction, routing to
// the table getTxOrDB would use but without its bucket-table creation. A
// missing row is nil.
func (store *AbstractSqlStore) readRowInSnapshot(ctx context.Context, tx *sql.Tx, p util.FullPath) (*filer.Entry, error) {
	if p == "/" {
		return nil, nil
	}
	parent, name := p.DirAndName()
	table, shortDir := DEFAULT_TABLE, parent
	if store.SupportBucketTable {
		if bucket := bucketTableFor(util.FullPath(parent)); bucket != "" {
			table = bucket
			if shortDir = strings.TrimPrefix(parent, bucketBase(bucket)); shortDir == "" {
				shortDir = "/"
			}
		}
	}
	var data []byte
	err := tx.QueryRowContext(ctx, store.GetSqlFind(table), util.HashStringToLong(shortDir), name, shortDir).Scan(&data)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, missingTableOr(err, table, fmt.Sprintf("read %s", p))
	}
	entry := &filer.Entry{FullPath: p}
	if err := entry.DecodeAttributesAndChunks(util.MaybeDecompressData(data)); err != nil {
		return nil, fmt.Errorf("decode %s: %v", p, err)
	}
	return entry, nil
}

// beginSnapshotTx starts the read-only REPEATABLE READ transaction that gives
// one snapshot across statements. Nothing weaker is tried: under READ
// COMMITTED each page would see a different snapshot, and the stream would
// still be advertised as one.
func (store *AbstractSqlStore) beginSnapshotTx(ctx context.Context) (*sql.Tx, error) {
	db := store.DB
	if store.ScanDB != nil {
		db = store.ScanDB
	} else if db.Stats().MaxOpenConnections == 1 {
		// The scan would hold the filer's only connection for its whole run.
		return nil, fmt.Errorf("%w: the store's single connection cannot be held for a scan", filer.ErrUnsupportedEntryScan)
	}
	// A BeginTx failure (a down database) must fail the traversal, not route it
	// to the walk: a recovered database lets the walk read renamed-away entries
	// as absent, the very loss this scan exists to prevent.
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("begin snapshot: %v", err)
	}
	return tx, nil
}

func (store *AbstractSqlStore) cachedBucketNames() []string {
	store.dbsLock.Lock()
	defer store.dbsLock.Unlock()
	names := make([]string, 0, len(store.dbs))
	for name := range store.dbs {
		names = append(names, name)
	}
	return names
}

// scanTables picks the tables to read for dirPath: the default table alone,
// one bucket's table for a path inside that bucket, or the default table plus
// every bucket table (enumerated from /buckets inside the snapshot).
func (store *AbstractSqlStore) scanTables(ctx context.Context, tx *sql.Tx, dirPath util.FullPath, cachedBuckets []string) ([]scanTable, error) {
	if !store.SupportBucketTable {
		return []scanTable{{table: DEFAULT_TABLE}}, nil
	}
	if bucket := bucketTableFor(dirPath); bucket != "" {
		return []scanTable{{table: bucket, base: bucketBase(bucket)}}, nil
	}
	tables := []scanTable{{table: DEFAULT_TABLE}}
	if dirPath != "/" && string(dirPath) != bucketsRoot {
		// Outside every bucket (e.g. /topics, or an internal /buckets/.system
		// folder): everything below dirPath is in the default table.
		return tables, nil
	}
	buckets, err := store.listBucketNames(ctx, tx, cachedBuckets)
	if err != nil {
		return nil, err
	}
	for _, bucket := range buckets.names {
		tables = append(tables, scanTable{table: bucket, base: bucketBase(bucket), cachedOnly: !buckets.seen[bucket]})
	}
	return tables, nil
}

const bucketsRoot = "/buckets"

func bucketBase(bucket string) string {
	return bucketsRoot + "/" + bucket
}

// bucketTableFor returns the bucket whose table alone holds everything below
// dirPath, or "" when dirPath is not inside a real bucket (as getTxOrDB routes).
func bucketTableFor(dirPath util.FullPath) string {
	p := string(dirPath)
	if !strings.HasPrefix(p, bucketsRoot+"/") {
		return ""
	}
	rest := p[len(bucketsRoot)+1:]
	bucket := rest
	if i := strings.Index(rest, "/"); i >= 0 {
		bucket = rest[:i]
	}
	if bucket == "" || strings.HasPrefix(bucket, ".") || !isValidBucket(bucket) {
		return ""
	}
	return bucket
}

// bucketNames is the snapshot-listed and cached-only buckets whose tables a
// scan may read.
type bucketNames struct {
	names []string
	seen  map[string]bool // listed under /buckets inside the snapshot
}

// listBucketNames reads the directories under /buckets within the snapshot
// and keeps those that route to their own table, plus the buckets this
// process has already seen (their directory entry may be gone, their rows not).
func (store *AbstractSqlStore) listBucketNames(ctx context.Context, tx *sql.Tx, cachedBuckets []string) (bucketNames, error) {
	var names bucketNames
	names.seen = make(map[string]bool)
	added := make(map[string]bool)
	add := func(name string) {
		if added[name] || strings.HasPrefix(name, ".") || !isValidBucket(name) {
			return
		}
		added[name] = true
		names.names = append(names.names, name)
	}

	const page = 1000
	startFrom := ""
	for {
		type row struct {
			name string
			data []byte
		}
		var rows []row
		if err := func() error {
			res, err := tx.QueryContext(ctx, store.GetSqlListExclusive(DEFAULT_TABLE), util.HashStringToLong(bucketsRoot), startFrom, bucketsRoot, "%", page)
			if err != nil {
				return fmt.Errorf("list %s: %v", bucketsRoot, err)
			}
			defer res.Close()
			for res.Next() {
				var r row
				if err := res.Scan(&r.name, &r.data); err != nil {
					return fmt.Errorf("scan %s: %v", bucketsRoot, err)
				}
				rows = append(rows, r)
			}
			return res.Err()
		}(); err != nil {
			return bucketNames{}, err
		}
		for _, r := range rows {
			startFrom = r.name
			entry := &filer.Entry{FullPath: util.NewFullPath(bucketsRoot, r.name)}
			if err := entry.DecodeAttributesAndChunks(util.MaybeDecompressData(r.data)); err != nil {
				return bucketNames{}, fmt.Errorf("scan decode %s: %v", entry.FullPath, err)
			}
			if entry.IsDirectory() {
				names.seen[r.name] = true
				add(r.name)
			}
		}
		if len(rows) < page {
			break
		}
	}

	for _, name := range cachedBuckets {
		add(name)
	}
	return names, nil
}

// scanOneTable reads one table in primary-key pages and hands the entries
// strictly below dirPath to fn (the root itself is read separately). Key-value
// records share the default table; GenDirAndName stores the raw key bytes as
// their dirhash while entry rows hash the directory, so the mismatch
// identifies them exactly.
func (store *AbstractSqlStore) scanOneTable(ctx context.Context, tx *sql.Tx, t scanTable, dirPath util.FullPath, fn func(ctx context.Context, entry *filer.Entry) error) error {
	type row struct {
		dirHash   int64
		name, dir string
		data      []byte
	}
	cursorHash, cursorName := int64(math.MinInt64), ""
	firstPage := true
	for {
		var page []row
		if t.cachedOnly && firstPage {
			// A missing table can only be met by the first page read: once a
			// row came back the tx holds the table against a drop. On
			// PostgreSQL the failed query aborts the transaction, so the skip
			// needs a savepoint to roll back to before the next table's read.
			if _, err := tx.ExecContext(ctx, "SAVEPOINT seaweed_scan_bucket"); err != nil {
				return fmt.Errorf("scan table %s: %v", t.table, err)
			}
		}
		sqlText, args := store.GetSqlScanPage(t.table, cursorHash, cursorName, scanPageSize)
		rows, err := tx.QueryContext(ctx, sqlText, args...)
		if err != nil {
			if t.cachedOnly && isMissingTableError(err) {
				tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT seaweed_scan_bucket")
				return nil // a cached-only name whose table was dropped meanwhile
			}
			return missingTableOr(err, t.table, fmt.Sprintf("scan table %s", t.table))
		}
		firstPage = false
		if err := func() error {
			defer rows.Close()
			for rows.Next() {
				var r row
				if err := rows.Scan(&r.dirHash, &r.name, &r.dir, &r.data); err != nil {
					return fmt.Errorf("scan table %s: %v", t.table, err)
				}
				page = append(page, r)
			}
			return rows.Err()
		}(); err != nil {
			return err
		}

		for _, r := range page {
			cursorHash, cursorName = r.dirHash, r.name
			if r.dirHash != util.HashStringToLong(r.dir) {
				continue // a key-value record, see GenDirAndName
			}
			fullPath := util.NewFullPath(t.base+r.dir, r.name)
			if !isStrictlyBelow(fullPath, dirPath) {
				continue
			}
			entry := &filer.Entry{FullPath: fullPath}
			if err := entry.DecodeAttributesAndChunks(util.MaybeDecompressData(r.data)); err != nil {
				return fmt.Errorf("scan decode %s: %v", fullPath, err)
			}
			if err := fn(ctx, entry); err != nil {
				return err
			}
		}
		if len(page) < scanPageSize {
			return nil
		}
	}
}

// missingTableOr names the condition when a bucket's table is not there. The
// scan fails closed: from inside the snapshot a table that was never created
// cannot be told from one dropped a moment ago, whose rows the snapshot still
// counts, and treating either as empty would let fsck purge referenced chunks.
func missingTableOr(err error, table, what string) error {
	if table != DEFAULT_TABLE && isMissingTableError(err) {
		return fmt.Errorf("bucket %s has a directory entry but no table (never created, or dropped during the scan): %v", table, err)
	}
	return fmt.Errorf("%s: %v", what, err)
}

// isMissingTableError recognizes a missing table by message, as InsertEntry
// recognizes duplicates: PostgreSQL `relation "x" does not exist` (SQLSTATE
// 42P01), MySQL error 1146 (SQLSTATE 42S02), SQLite "no such table".
func isMissingTableError(err error) bool {
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "42p01") ||
		(strings.Contains(msg, "relation \"") && strings.Contains(msg, "does not exist")) ||
		strings.Contains(msg, "42s02") ||
		strings.Contains(msg, "error 1146") ||
		strings.Contains(msg, "no such table")
}

// isStrictlyBelow reports whether p is a descendant of dir (not dir itself).
func isStrictlyBelow(p, dir util.FullPath) bool {
	if dir == "/" {
		return p != "/"
	}
	return strings.HasPrefix(string(p), string(dir)+"/")
}
