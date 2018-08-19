package filer2

import (
	"errors"
	"github.com/chrislusf/seaweedfs/weed/util"
)

type FilerStore interface {
	// GetName gets the name to locate the configuration in filer.toml file
	GetName() string
	// Initialize initializes the file store
	Initialize(configuration util.Configuration) error
	InsertEntry(*Entry) error
	UpdateEntry(*Entry) (err error)
	// err == filer2.ErrNotFound if not found
	FindEntry(FullPath) (entry *Entry, err error)
	DeleteEntry(FullPath) (err error)
	ListDirectoryEntries(dirPath FullPath, startFileName string, includeStartFile bool, limit int) ([]*Entry, error)
}

var ErrNotFound = errors.New("filer: no entry is found in filer store")
