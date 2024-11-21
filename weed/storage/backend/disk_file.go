package backend

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	. "github.com/seaweedfs/seaweedfs/weed/storage/types"
	"io"
	"os"
	"runtime"
	"time"
)

var (
	_ BackendStorageFile = &DiskFile{}
)

const isMac = runtime.GOOS == "darwin"

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
	if df.File == nil {
		return 0, os.ErrClosed
	}
	n, err = df.File.ReadAt(p, off)
	if err == io.EOF && n == len(p) {
		err = nil
	}
	return
}

func (df *DiskFile) WriteAt(p []byte, off int64) (n int, err error) {
	if df.File == nil {
		return 0, os.ErrClosed
	}
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
	if df.File == nil {
		return os.ErrClosed
	}
	err := df.File.Truncate(off)
	if err == nil {
		df.fileSize = off
		df.modTime = time.Now()
	}
	return err
}

func (df *DiskFile) Close() error {
	if df.File == nil {
		return nil
	}
	err := df.Sync()
	var err1 error
	if df.File != nil {
		// always try to close
		err1 = df.File.Close()
	}
	// assume closed
	df.File = nil
	if err != nil {
		return err
	}
	if err1 != nil {
		return err1
	}
	return nil
}

func (df *DiskFile) GetStat() (datSize int64, modTime time.Time, err error) {
	if df.File == nil {
		err = os.ErrClosed
	}
	return df.fileSize, df.modTime, err
}

func (df *DiskFile) Name() string {
	return df.fullFilePath
}

func (df *DiskFile) Sync() error {
	if df.File == nil {
		return os.ErrClosed
	}
	if isMac {
		return nil
	}
	return df.File.Sync()
}
