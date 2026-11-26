package abstract_sql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// The bucket owner index lives at /buckets/.system/owners/<owner>/<bucket>.
// .system is an internal folder, not an S3 bucket, so getTxOrDB must route it
// to the default table rather than reject it as an invalid bucket name.
func TestGetTxOrDBInternalSystemFolder(t *testing.T) {
	store := &AbstractSqlStore{SupportBucketTable: true}
	ctx := context.Background()

	cases := []struct {
		path          string
		isForChildren bool
	}{
		{"/buckets/.system/owners", false},
		{"/buckets/.system/owners/foo/bucket1", false},
		{"/buckets/.system/owners/foo", true},
	}
	for _, c := range cases {
		_, bucket, shortPath, err := store.getTxOrDB(ctx, util.FullPath(c.path), c.isForChildren)
		if err != nil {
			t.Errorf("getTxOrDB(%s): unexpected error: %v", c.path, err)
		}
		if bucket != DEFAULT_TABLE {
			t.Errorf("getTxOrDB(%s): bucket = %q, want %q", c.path, bucket, DEFAULT_TABLE)
		}
		if string(shortPath) != c.path {
			t.Errorf("getTxOrDB(%s): shortPath = %q, want %q", c.path, shortPath, c.path)
		}
	}
}

func TestGetTxOrDBRealBucket(t *testing.T) {
	store := &AbstractSqlStore{SupportBucketTable: true, dbs: map[string]bool{"mybucket": true}}
	ctx := context.Background()

	_, bucket, shortPath, err := store.getTxOrDB(ctx, util.FullPath("/buckets/mybucket/dir/file.txt"), false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if bucket != "mybucket" {
		t.Errorf("bucket = %q, want mybucket", bucket)
	}
	if string(shortPath) != "/dir/file.txt" {
		t.Errorf("shortPath = %q, want /dir/file.txt", shortPath)
	}
}

func TestLikePattern(t *testing.T) {
	cases := []struct {
		prefix, want string
	}{
		{"", "%"},
		{"plain", "plain%"},
		{"call_03_KNl", "call!_03!_KNl%"},
		{"100%", "100!%%"},
		{"a!b", "a!!b%"},
		{`a\b`, `a\b%`},
	}
	for _, c := range cases {
		if got := LikePattern(c.prefix); got != c.want {
			t.Errorf("LikePattern(%q) = %q, want %q", c.prefix, got, c.want)
		}
	}
}

func TestSplitPoolForKv(t *testing.T) {
	cases := []struct {
		maxOpen, mainOpen, kvOpen int
	}{
		{0, 0, 0},   // unbounded: nothing waits, one pool is enough
		{-1, -1, 0}, // same, however the operator spelled it
		{50, 42, 8},
		{16, 12, 4},
		{4, 3, 1},
		{2, 1, 1},
		{1, 1, 1}, // a listing needs two, so a cap of 1 has to become 2
	}
	for _, c := range cases {
		mainOpen, kvOpen := splitPoolForKv(c.maxOpen)
		if mainOpen != c.mainOpen || kvOpen != c.kvOpen {
			t.Errorf("splitPoolForKv(%d) = %d, %d, want %d, %d", c.maxOpen, mainOpen, kvOpen, c.mainOpen, c.kvOpen)
		}
	}
}

type recordingDDLGen struct {
	SqlGenerator
	calls []string
}

func (g *recordingDDLGen) GetSqlCreateTable(tableName string) string {
	g.calls = append(g.calls, "create "+tableName)
	return "CREATE TABLE " + tableName
}

func (g *recordingDDLGen) GetSqlDropTable(tableName string) string {
	g.calls = append(g.calls, "drop "+tableName)
	return "DROP"
}

func TestBucketHooksSkipInvalidBucketNames(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectExec("CREATE TABLE mybucket").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("DROP").WillReturnResult(sqlmock.NewResult(0, 0))

	gen := &recordingDDLGen{}
	store := &AbstractSqlStore{SqlGenerator: gen, DB: db, SupportBucketTable: true}

	for _, name := range []string{
		`.x01" (a int);CREATE TABLE pwned(flag text);--`,
		`.z01","marker`,
		`b01" (a int); DROP TABLE filemeta; --`,
		DEFAULT_TABLE,
		".system",
		"",
	} {
		store.OnBucketCreation(name)
		store.OnBucketDeletion(name)
	}
	store.OnBucketCreation("mybucket")
	store.OnBucketDeletion("mybucket")

	if want := []string{"create mybucket", "drop mybucket"}; !reflect.DeepEqual(gen.calls, want) {
		t.Errorf("DDL calls = %q, want %q", gen.calls, want)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

type listGen struct{ SqlGenerator }

func (listGen) GetSqlListExclusive(string) string { return "SELECT name, meta" }
func (listGen) GetSqlListInclusive(string) string { return "SELECT name, meta" }

// A listing that loses its connection partway through must report the error,
// not return the rows read so far as if the directory ended there.
func TestListDirectoryEntriesReportsRowIterationError(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	meta, err := (&filer.Entry{}).EncodeAttributesAndChunks()
	if err != nil {
		t.Fatal(err)
	}
	connLost := errors.New("connection reset by peer")
	mock.ExpectQuery("SELECT name, meta").WillReturnRows(
		sqlmock.NewRows([]string{"name", "meta"}).
			AddRow("a.txt", meta).
			AddRow("b.txt", meta).
			RowError(1, connLost))

	store := &AbstractSqlStore{SqlGenerator: listGen{}, DB: db}
	var listed []string
	_, err = store.ListDirectoryEntries(context.Background(), "/dir", "", true, 100, func(entry *filer.Entry) (bool, error) {
		listed = append(listed, entry.Name())
		return true, nil
	})
	if !errors.Is(err, connLost) {
		t.Fatalf("ListDirectoryEntries error = %v, want %v (listed %v)", err, connLost, listed)
	}
}

// A listing the callback stops on purpose is not a failure, even if the
// request context is canceled before the rows are closed.
func TestListDirectoryEntriesStoppedByCallbackWithCanceledContext(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	meta, err := (&filer.Entry{}).EncodeAttributesAndChunks()
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectQuery("SELECT name, meta").WillReturnRows(
		sqlmock.NewRows([]string{"name", "meta"}).AddRow("a.txt", meta).AddRow("b.txt", meta))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := &AbstractSqlStore{SqlGenerator: listGen{}, DB: db}
	_, err = store.ListDirectoryEntries(ctx, "/dir", "", true, 100, func(entry *filer.Entry) (bool, error) {
		cancel()
		// database/sql closes the rows on a background goroutine; once the
		// connection is back in the pool, rows.Err reports the cancellation.
		for deadline := time.Now().Add(5 * time.Second); db.Stats().InUse > 0; time.Sleep(time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatal("the canceled listing still holds its connection")
			}
		}
		return false, nil
	})
	if err != nil {
		t.Fatalf("ListDirectoryEntries error = %v, want nil", err)
	}
}

type writeGen struct{ SqlGenerator }

func (writeGen) GetSqlInsert(string) string { return "INSERT entry" }
func (writeGen) GetSqlDelete(string) string { return "DELETE entry" }

// What IsolatesTransactions declares: an entry write, an entry delete and a KV
// write under a transaction's context all go through that transaction, the KV
// write too when KV has a pool of its own, and a rollback ends it.
func TestWritesUnderATransactionGoThroughIt(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// nothing is expected of the KV pool: a call to it fails the write
	kvDB, kvMock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer kvDB.Close()

	mock.ExpectBegin()
	mock.ExpectExec("INSERT entry").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("DELETE entry").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT entry").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectRollback()

	store := &AbstractSqlStore{SqlGenerator: writeGen{}, DB: db, KvDB: kvDB}
	if !store.IsolatesTransactions() {
		t.Fatal("IsolatesTransactions = false")
	}
	txCtx, err := store.BeginTransaction(context.Background())
	if err != nil {
		t.Fatalf("BeginTransaction: %v", err)
	}
	// one mock serves the pool and its transactions alike, so the expectations
	// alone would also pass for writes through store.DB
	tx, _ := txCtx.Value("tx").(*sql.Tx)
	if tx == nil {
		t.Fatal("BeginTransaction context carries no *sql.Tx")
	}
	if db, _, _, _ := store.getTxOrDB(txCtx, "/dir/new.txt", false); db != TxOrDB(tx) {
		t.Fatalf("getTxOrDB under the transaction = %T, want its *sql.Tx", db)
	}
	if db := store.kvDB(txCtx); db != TxOrDB(tx) {
		t.Fatalf("kvDB under the transaction = %T, want its *sql.Tx", db)
	}
	if err := store.InsertEntry(txCtx, &filer.Entry{FullPath: "/dir/new.txt"}); err != nil {
		t.Fatalf("InsertEntry: %v", err)
	}
	if err := store.DeleteEntry(txCtx, "/dir/old.txt"); err != nil {
		t.Fatalf("DeleteEntry: %v", err)
	}
	if err := store.KvPut(txCtx, []byte("hard-link-id"), []byte("record")); err != nil {
		t.Fatalf("KvPut: %v", err)
	}
	if err := store.RollbackTransaction(txCtx); err != nil {
		t.Fatalf("RollbackTransaction: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
	if err := kvMock.ExpectationsWereMet(); err != nil {
		t.Error(err)
	}
}

// busyDriver serves canned rows per query on connections that, like pgx's,
// refuse a query while the rows of another are still open.
type busyDriver map[string][][]driver.Value

func (d busyDriver) Connect(context.Context) (driver.Conn, error) { return &busyConn{rows: d}, nil }
func (d busyDriver) Driver() driver.Driver                        { return nil }

type busyConn struct {
	rows busyDriver
	busy bool
}

func (c *busyConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (c *busyConn) Close() error                        { return nil }
func (c *busyConn) Begin() (driver.Tx, error)           { return c, nil }
func (c *busyConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return c, nil
}
func (c *busyConn) Commit() error   { return nil }
func (c *busyConn) Rollback() error { return nil }
func (c *busyConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if c.busy {
		return nil, errors.New("conn busy")
	}
	c.busy = true
	return &busyRows{conn: c, rows: c.rows[query]}, nil
}

type busyRows struct {
	conn *busyConn
	rows [][]driver.Value
}

func (r *busyRows) Columns() []string { return make([]string, len(r.rows[0])) }
func (r *busyRows) Close() error      { r.conn.busy = false; return nil }
func (r *busyRows) Next(dest []driver.Value) error {
	if len(r.rows) == 0 {
		return io.EOF
	}
	copy(dest, r.rows[0])
	r.rows = r.rows[1:]
	return nil
}

type findGen struct{ listGen }

func (findGen) GetSqlFind(string) string { return "SELECT meta" }

// A transaction has one connection, so a listing in one (a directory rename)
// must close its rows before the callback reads a hard link on it.
func TestListDirectoryEntriesInTransactionClosesRowsBeforeCallback(t *testing.T) {
	meta, err := (&filer.Entry{}).EncodeAttributesAndChunks()
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(busyDriver{
		"SELECT name, meta": {{"a.txt", meta}, {"b.txt", meta}},
		"SELECT meta":       {{[]byte("record")}},
	})
	defer db.Close()

	store := &AbstractSqlStore{SqlGenerator: findGen{}, DB: db}
	ctx, err := store.BeginTransaction(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer store.RollbackTransaction(ctx)

	var listed []string
	_, err = store.ListDirectoryEntries(ctx, "/dir", "", true, 100, func(entry *filer.Entry) (bool, error) {
		listed = append(listed, entry.Name())
		_, err := store.KvGet(ctx, []byte("hard-link-id"))
		return true, err
	})
	if err != nil {
		t.Fatalf("ListDirectoryEntries: %v", err)
	}
	if want := []string{"a.txt", "b.txt"}; !reflect.DeepEqual(listed, want) {
		t.Errorf("listed %v, want %v", listed, want)
	}
}
