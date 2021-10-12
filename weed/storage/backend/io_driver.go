package backend

import (
	"os"

	"github.com/chrislusf/seaweedfs/weed/glog"
)

var (
	_              IODriver = &SyscallIODriver{}
	supportIOUring          = false
)

type IODriver interface {
	ReadAt(p []byte, off int64) (n int, err error)
	WriteAt(p []byte, off int64) (n int, err error)
	Sync() error
	Truncate(off int64) error
	Close() error
}

func NewIODriver(f *os.File) IODriver {
	if !supportIOUring {
		glog.V(0).Infof("Using syscall driver for file %s", f.Name())
		return NewSyscallIODriver(f)
	}
	if EnableIOUring {
		driver, err := NewIOUringDriver(f)
		if err == nil {
			glog.V(0).Infof("Using iouring driver for file %s", f.Name())
			return driver
		}
		glog.Warningf("iouring driver got error: %v", err)
	}
	glog.V(0).Infof("Using syscall driver for file %s", f.Name())
	return NewSyscallIODriver(f)
}

type SyscallIODriver struct {
	File *os.File
}

func NewSyscallIODriver(file *os.File) *SyscallIODriver {
	return &SyscallIODriver{
		File: file,
	}
}

func (d *SyscallIODriver) ReadAt(p []byte, off int64) (int, error) {
	return d.File.ReadAt(p, off)
}

func (d *SyscallIODriver) WriteAt(p []byte, off int64) (int, error) {
	return d.File.WriteAt(p, off)
}

func (d *SyscallIODriver) Sync() error {
	return d.File.Sync()
}

func (d *SyscallIODriver) Truncate(off int64) error {
	return d.File.Truncate(off)
}

func (d *SyscallIODriver) Close() error {
	return d.File.Close()
}
