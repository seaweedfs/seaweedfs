package filer

import (
	"context"
	"errors"
	"github.com/chrislusf/seaweedfs/weed/util"
)

var (
	ErrUnsupportedListDirectoryPrefixed = errors.New("unsupported directory prefix listing")
	ErrKvNotImplemented                 = errors.New("kv not implemented yet")
	ErrKvNotFound                       = errors.New("kv: not found")

)

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
	ListDirectoryEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int) ([]*Entry, error)
	ListDirectoryPrefixedEntries(ctx context.Context, dirPath util.FullPath, startFileName string, includeStartFile bool, limit int, prefix string) ([]*Entry, error)

	BeginTransaction(ctx context.Context) (context.Context, error)
	CommitTransaction(ctx context.Context) error
	RollbackTransaction(ctx context.Context) error

	KvPut(ctx context.Context, key []byte, value []byte) (err error)
	KvGet(ctx context.Context, key []byte) (value []byte, err error)
	KvDelete(ctx context.Context, key []byte) (err error)

	Shutdown()
}
