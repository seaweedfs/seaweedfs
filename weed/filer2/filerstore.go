package filer2

import (
	"errors"
	"github.com/spf13/viper"
)

type FilerStore interface {
	GetName() string
	Initialize(viper *viper.Viper) (error)
	InsertEntry(*Entry) (error)
	UpdateEntry(*Entry) (err error)
	FindEntry(FullPath) (entry *Entry, err error)
	DeleteEntry(FullPath) (fileEntry *Entry, err error)
	ListDirectoryEntries(dirPath FullPath, startFileName string, inclusive bool, limit int) ([]*Entry, error)
}

var ErrNotFound = errors.New("filer: no entry is found in filer store")
