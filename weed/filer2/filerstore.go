package filer2

import (
	"context"
	"errors"
	"github.com/chrislusf/seaweedfs/weed/util"
)

type FilerStore interface {
	// GetName gets the name to locate the configuration in filer.toml file
	GetName() string
	// Initialize initializes the file store
	Initialize(configuration util.Configuration) error
	InsertEntry(context.Context, *Entry) error
	UpdateEntry(context.Context, *Entry) (err error)
	// err == filer2.ErrNotFound if not found
	FindEntry(context.Context, FullPath) (entry *Entry, err error)
	DeleteEntry(context.Context, FullPath) (err error)
	ListDirectoryEntries(ctx context.Context, dirPath FullPath, startFileName string, includeStartFile bool, limit int) ([]*Entry, error)

	BeginTransaction(ctx context.Context) (context.Context, error)
	CommitTransaction(ctx context.Context) error
	RollbackTransaction(ctx context.Context) error
}

var ErrNotFound = errors.New("filer: no entry is found in filer store")
