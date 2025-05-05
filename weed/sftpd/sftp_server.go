// sftp_server.go
package sftpd

import (
	"io"

	"github.com/pkg/sftp"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/sftpd/auth"
	"github.com/seaweedfs/seaweedfs/weed/sftpd/user"
	"google.golang.org/grpc"
)

type SftpServer struct {
	filerAddr      pb.ServerAddress
	grpcDialOption grpc.DialOption
	dataCenter     string
	filerGroup     string
	user           *user.User
	authManager    *auth.Manager
}

// NewSftpServer constructs the server.
func NewSftpServer(filerAddr pb.ServerAddress, grpcDialOption grpc.DialOption, dataCenter, filerGroup string, user *user.User) SftpServer {
	// Create a file system helper for the auth manager
	fsHelper := NewFileSystemHelper(filerAddr, grpcDialOption, dataCenter, filerGroup)

	// Create an auth manager for permission checking
	authManager := auth.NewManager(nil, fsHelper, []string{})

	return SftpServer{
		filerAddr:      filerAddr,
		grpcDialOption: grpcDialOption,
		dataCenter:     dataCenter,
		filerGroup:     filerGroup,
		user:           user,
		authManager:    authManager,
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
