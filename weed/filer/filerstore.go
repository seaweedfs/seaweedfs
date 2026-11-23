package filer

import (
	"context"
	"errors"
	"io"

	"github.com/seaweedfs/seaweedfs/weed/util"
)

const CountEntryChunksForGzip = 50

var (
	ErrUnsupportedListDirectoryPrefixed      = errors.New("unsupported directory prefix listing")
	ErrUnsupportedSuperLargeDirectoryListing = errors.New("unsupported super large directory listing")
	ErrKvNotImplemented                      = errors.New("kv not implemented yet")
	ErrKvNotFound                            = errors.New("kv: not found")
	// ErrTransactionConflict marks a commit the store refused because another
	// writer changed a key the transaction read or wrote. No writes from the
	// refused transaction were committed; an unknown commit result must not
	// carry this sentinel. Running the transaction again reads the other write.
	ErrTransactionConflict = errors.New("transaction conflict")
)

type ListEachEntryFunc func(entry *Entry) (bool, error)

type FilerStore interface {
	// GetName gets the name to locate the configuration in filer.toml file
	GetName() string
	// Initialize initializes the file store
	Initialize(configuration util.Configuration, prefix string) error
	InsertEntry(context.Context, *Entry) error
	UpdateEntry(context.Context, *Entry) (err error)
	// err == filer_pb.ErrNotFound if not found
	FindEntry(context.Context, util.FullPath) (entry *Entry, err error)
	DeleteEntry(context.Context, util.FullPath) (err error)
	DeleteFolderChildren(context.Context, util.FullPath) (err error)
	ListDirectoryEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, eachEntryFunc ListEachEntryFunc) (lastFileName string, err error)
	ListDirectoryPrefixedEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int64, prefix string, eachEntryFunc ListEachEntryFunc) (lastFileName string, err error)

	BeginTransaction(ctx context.Context) (context.Context, error)
	CommitTransaction(ctx context.Context) error
	RollbackTransaction(ctx context.Context) error

	KvPut(ctx context.Context, key []byte, value []byte) (err error)
	KvGet(ctx context.Context, key []byte) (value []byte, err error)
	KvDelete(ctx context.Context, key []byte) (err error)

	Shutdown()
}

// ErrUnsupportedEntryScan is returned by EntryScanner.ScanEntries, before
// any entry is visited, when the store cannot serve the scan.
var ErrUnsupportedEntryScan = errors.New("entry scan not supported by this filer store")

// UnorderedListingStore is implemented by a store whose directory listings do
// not come in file name order (Elasticsearch pages by a hash of the entry
// path), so a listing there cannot start at a name the store did not return.
type UnorderedListingStore interface {
	ListsUnordered() bool
}

// EntryScanner is an optional FilerStore capability: visit every entry at and
// below dirPath, in any order, from one consistent snapshot of the store.
//
// A directory walk sees each directory at a different time, so an entry
// renamed from a directory not yet listed into one already listed is never
// seen; volume.fsck then purges its chunks as orphans. A snapshot sees it
// exactly once. The context handed to fn carries the snapshot, so store reads
// made with it (hard-link records) see the same instant.
type EntryScanner interface {
	ScanEntries(ctx context.Context, dirPath util.FullPath, fn func(ctx context.Context, entry *Entry) error) error
}

type BucketAware interface {
	OnBucketCreation(bucket string)
	OnBucketDeletion(bucket string)
	CanDropWholeBucket() bool
}

type Debuggable interface {
	Debug(writer io.Writer)
}

// IsolatingTransactionStore is implemented by a store whose transactions are
// real: the entry and KV writes made under the context BeginTransaction
// returns go through that transaction, stay invisible to other readers until
// CommitTransaction, and are undone by RollbackTransaction. A store says so
// itself, once that has been checked for it: the SQL stores and TiKV do. It
// cannot be told from the context a store hands back, which may carry a
// transaction that its writes do not use.
type IsolatingTransactionStore interface {
	IsolatesTransactions() bool
}

// ConflictRefusingStore protects a read-modify-write of one entry and its
// shared KV records. Entry and KV operations must use the context's transaction,
// keep its writes invisible until commit, and leave none behind on rollback.
// If another writer changes a key this transaction both reads and writes after
// its read version, CommitTransaction must refuse with ErrTransactionConflict.
// Read/write conflicts suffice; blind writes need not conflict with each other.
//
// TiKV and FoundationDB implement this capability. It does not promise that
// bulk operations such as DeleteFolderChildren stay inside the transaction.
// Other stores must opt in only after their entry and KV paths, isolation and
// conflict errors have been checked; having BeginTransaction alone is not enough.
type ConflictRefusingStore interface {
	RefusesConflictingCommits() bool
}

// OverwritingStore is implemented by a store without transactions whose
// InsertEntry over an existing entry replaces it at once and whole: the stored
// entry, its expiry, and its listing in the directory. A rename can then write
// a file over its target instead of deleting the target first. A store says so
// itself, once its insert has been checked: an upsert may leave part of the
// old row in place. The Redis stores do.
type OverwritingStore interface {
	OverwritesAtOnce() bool
}
