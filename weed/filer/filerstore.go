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
