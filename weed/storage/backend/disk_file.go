package backend

import (
	"github.com/chrislusf/seaweedfs/weed/glog"
	. "github.com/chrislusf/seaweedfs/weed/storage/types"
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
	stat, err := f.Stat()
	if err != nil {
		glog.Fatalf("stat file %s: %v", f.Name(), err)
	}
	offset := stat.Size()
	if offset%NeedlePaddingSize != 0 {
		offset = offset + (NeedlePaddingSize - offset%NeedlePaddingSize)
	}

	return &DiskFile{
		fullFilePath: f.Name(),
		File:         f,
		fileSize:     offset,
		modTime:      stat.ModTime(),
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

func (df *DiskFile) Write(p []byte) (n int, err error) {
	return df.WriteAt(p, df.fileSize)
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
	if df.File == nil {
		err = os.ErrInvalid
	}
	return df.fileSize, df.modTime, err
}

func (df *DiskFile) Name() string {
	return df.fullFilePath
}

func (df *DiskFile) Sync() error {
	return df.File.Sync()
}
