// sftp_server.go
package sftpd

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/pkg/sftp"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	filer_pb "github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/sftpd/user"
	"github.com/seaweedfs/seaweedfs/weed/util"
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

	// Check if home directory already exists
	entry, err := fs.getEntry(fs.user.HomeDir)
	if err == nil && entry != nil {
		// Directory exists, just ensure proper ownership
		if entry.Attributes.Uid != fs.user.Uid || entry.Attributes.Gid != fs.user.Gid {
			dir, _ := util.FullPath(fs.user.HomeDir).DirAndName()
			entry.Attributes.Uid = fs.user.Uid
			entry.Attributes.Gid = fs.user.Gid
			return fs.updateEntry(dir, entry)
		}
		return nil
	}

	// Skip permission check for home directory creation
	// This is a special case where we want to create the directory regardless
	dir, name := util.FullPath(fs.user.HomeDir).DirAndName()

	// Create the directory with proper permissions using filer_pb.Mkdir
	err = filer_pb.Mkdir(context.Background(), fs, dir, name, func(entry *filer_pb.Entry) {
		mode := uint32(0700 | os.ModeDir) // Default to private permissions for home dirs
		entry.Attributes.FileMode = mode
		entry.Attributes.Uid = fs.user.Uid
		entry.Attributes.Gid = fs.user.Gid
		now := time.Now().Unix()
		entry.Attributes.Crtime = now
		entry.Attributes.Mtime = now
		if entry.Extended == nil {
			entry.Extended = make(map[string][]byte)
		}
		entry.Extended["creator"] = []byte(fs.user.Username)
	})

	if err != nil {
		return fmt.Errorf("failed to create home directory: %v", err)
	}

	glog.V(0).Infof("Successfully created home directory for user %s: %s", fs.user.Username, fs.user.HomeDir)
	return nil
}
