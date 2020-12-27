package ftpd

import (
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/spf13/afero"
	"net"
	"os"
	"time"
)

type FtpFileSystem struct {
	option      *FtpServerOption
	ftpListener net.Listener
}

var (
	_ = afero.Fs(&FtpFileSystem{})
)

// NewServer returns a new FTP server driver
func NewFtpFileSystem(option *FtpServerOption) (*FtpFileSystem, error) {
	return &FtpFileSystem{
		option: option,
	}, nil
}

func (fs *FtpFileSystem) Create(name string) (afero.File, error) {

}
func (fs *FtpFileSystem) Mkdir(name string, perm os.FileMode) error {

}
func (fs *FtpFileSystem) MkdirAll(path string, perm os.FileMode) error {

}
func (fs *FtpFileSystem) Open(name string) (afero.File, error) {

}
func (fs *FtpFileSystem) OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {

}
func (fs *FtpFileSystem) Remove(name string) error {

}
func (fs *FtpFileSystem) RemoveAll(path string) error {

}
func (fs *FtpFileSystem) Rename(oldname, newname string) error {

}
func (fs *FtpFileSystem) Stat(name string) (os.FileInfo, error) {

}
func (fs *FtpFileSystem) Name() string {
	return "SeaweedFS FTP Server " + util.Version()
}
func (fs *FtpFileSystem) Chmod(name string, mode os.FileMode) error {
}
func (fs *FtpFileSystem) Chtimes(name string, atime time.Time, mtime time.Time) error {

}
