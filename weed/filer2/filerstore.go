package filer2

import "errors"

type FilerStore interface {
	InsertEntry(*Entry) (error)
	UpdateEntry(*Entry) (err error)
	FindEntry(FullPath) (found bool, entry *Entry, err error)
	DeleteEntry(FullPath) (fileEntry *Entry, err error)
	ListDirectoryEntries(dirPath FullPath, startFileName string, inclusive bool, limit int) ([]*Entry, error)
}

var ErrNotFound = errors.New("filer: no entry is found in filer store")
