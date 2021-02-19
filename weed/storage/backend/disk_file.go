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
	fileSize     int64
	modTime      time.Time
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
	n, err = df.File.WriteAt(p, off)
	if err == nil {
		waterMark := off + int64(n)
		if waterMark > df.fileSize {
			df.fileSize = waterMark
			df.modTime = time.Now()
		}
	}
	return
}

func (df *DiskFile) Truncate(off int64) error {
	err := df.File.Truncate(off)
	if err == nil {
		df.fileSize = off
		df.modTime = time.Now()
	}
	return err
}

func (df *DiskFile) Close() error {
	return df.File.Close()
}

func (df *DiskFile) GetStat() (datSize int64, modTime time.Time, err error) {
	if df.fileSize != 0 {
		return df.fileSize, df.modTime, nil
	}
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
	df.fileSize = 0
	return df.File.Sync()
}
