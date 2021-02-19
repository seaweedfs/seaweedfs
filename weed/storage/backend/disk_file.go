package backend

import (
	"os"
	"time"
)

var (
	_ BackendStorageFile = &DiskFile{}
)

type DiskFile struct {
	File         *os.File
	fullFilePath string
}

func NewDiskFile(f *os.File) *DiskFile {
	return &DiskFile{
		fullFilePath: f.Name(),
		File:         f,
	}
}

func (df *DiskFile) ReadAt(p []byte, off int64) (n int, err error) {
	return df.File.ReadAt(p, off)
}

func (df *DiskFile) WriteAt(p []byte, off int64) (n int, err error) {
	return df.File.WriteAt(p, off)
}

func (df *DiskFile) Truncate(off int64) error {
	return df.File.Truncate(off)
}

func (df *DiskFile) Close() error {
	return df.File.Close()
}

func (df *DiskFile) GetStat() (datSize int64, modTime time.Time, err error) {
	stat, e := df.File.Stat()
	if e == nil {
		return stat.Size(), stat.ModTime(), nil
	}
	return 0, time.Time{}, err
}

func (df *DiskFile) Name() string {
	return df.fullFilePath
}

func (df *DiskFile) Sync() error {
	return df.File.Sync()
}
