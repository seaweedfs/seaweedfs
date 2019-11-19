package backend

import (
	"io"
	"time"
)

type DataStorageBackend interface {
	io.ReaderAt
	io.WriterAt
	Truncate(off int64) error
	io.Closer
	GetStat() (datSize int64, modTime time.Time, err error)
	String() string
}

var (
	StorageBackends []DataStorageBackend
)
