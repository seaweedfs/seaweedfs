// sftp_server.go
package sftpd

import (
	"fmt"
	"io"

	"github.com/pkg/sftp"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/sftpd/user"
	"google.golang.org/grpc"
)

type SftpServer struct {
	filerAddr      pb.ServerAddress
	grpcDialOption grpc.DialOption
	dataCenter     string
	filerGroup     string
	user           *user.User
}

// NewSftpServer constructs the server.
func NewSftpServer(filerAddr pb.ServerAddress, grpcDialOption grpc.DialOption, dataCenter, filerGroup string, user *user.User) SftpServer {

	return SftpServer{
		filerAddr:      filerAddr,
		grpcDialOption: grpcDialOption,
		dataCenter:     dataCenter,
		filerGroup:     filerGroup,
		user:           user,
	}
}

// Fileread is invoked for “get” requests.
func (fs *SftpServer) Fileread(req *sftp.Request) (io.ReaderAt, error) {
	return fs.readFile(req)
}

// Filewrite is invoked for “put” requests.
func (fs *SftpServer) Filewrite(req *sftp.Request) (io.WriterAt, error) {
	return fs.newFileWriter(req)
}

// Filecmd handles Remove, Rename, Mkdir, Rmdir, etc.
func (fs *SftpServer) Filecmd(req *sftp.Request) error {
	return fs.dispatchCmd(req)
}

// Filelist handles directory listings.
func (fs *SftpServer) Filelist(req *sftp.Request) (sftp.ListerAt, error) {
	return fs.listDir(req)
}

// EnsureHomeDirectory creates the user's home directory if it doesn't exist
func (fs *SftpServer) EnsureHomeDirectory() error {
	if fs.user.HomeDir == "" {
		return fmt.Errorf("user has no home directory configured")
	}

	glog.V(0).Infof("Ensuring home directory exists for user %s: %s", fs.user.Username, fs.user.HomeDir)

	err := fs.makeDir(&sftp.Request{
		Filepath: fs.user.HomeDir,
	})
	if err != nil {
		return fmt.Errorf("failed to ensure home directory: %v", err)
	}
	return nil
}
