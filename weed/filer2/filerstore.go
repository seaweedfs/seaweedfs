package filer2

import (
	"errors"
)

type FilerStore interface {
	GetName() string
	// Initialize initializes the file store
	Initialize(configuration Configuration) error
	InsertEntry(*Entry) error
	UpdateEntry(*Entry) (err error)
	FindEntry(FullPath) (entry *Entry, err error)
	DeleteEntry(FullPath) (err error)
	ListDirectoryEntries(dirPath FullPath, startFileName string, inclusive bool, limit int) ([]*Entry, error)
}

var ErrNotFound = errors.New("filer: no entry is found in filer store")
