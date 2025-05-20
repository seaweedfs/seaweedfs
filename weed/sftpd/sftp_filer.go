// sftp_filer_refactored.go
package sftpd

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strings"
	"syscall"
	"time"

	"github.com/pkg/sftp"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	filer_pb "github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	weed_server "github.com/seaweedfs/seaweedfs/weed/server"
	"github.com/seaweedfs/seaweedfs/weed/sftpd/user"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
)

const (
	defaultTimeout   = 30 * time.Second
	defaultListLimit = 1000
)

// ==================== Filer RPC Helpers ====================

// callWithClient wraps a gRPC client call with timeout and client creation.
func (fs *SftpServer) callWithClient(streaming bool, fn func(ctx context.Context, client filer_pb.SeaweedFilerClient) error) error {
	return fs.withTimeoutContext(func(ctx context.Context) error {
		return fs.WithFilerClient(streaming, func(client filer_pb.SeaweedFilerClient) error {
			return fn(ctx, client)
		})
	})
}

// getEntry retrieves a single directory entry by path.
func (fs *SftpServer) getEntry(p string) (*filer_pb.Entry, error) {
	dir, name := util.FullPath(p).DirAndName()
	var entry *filer_pb.Entry
	err := fs.callWithClient(false, func(ctx context.Context, client filer_pb.SeaweedFilerClient) error {
		r, err := client.LookupDirectoryEntry(ctx, &filer_pb.LookupDirectoryEntryRequest{Directory: dir, Name: name})
		if err != nil {
			return err
		}
		if r.Entry == nil {
			return fmt.Errorf("%s not found in %s", name, dir)
		}
		entry = r.Entry
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("lookup %s: %w", p, err)
	}
	return entry, nil
}

// updateEntry sends an UpdateEntryRequest for the given entry.
func (fs *SftpServer) updateEntry(dir string, entry *filer_pb.Entry) error {
	return fs.callWithClient(false, func(ctx context.Context, client filer_pb.SeaweedFilerClient) error {
		_, err := client.UpdateEntry(ctx, &filer_pb.UpdateEntryRequest{Directory: dir, Entry: entry})
		return err
	})
}

// ==================== FilerClient Interface ====================

func (fs *SftpServer) AdjustedUrl(location *filer_pb.Location) string { return location.Url }
func (fs *SftpServer) GetDataCenter() string                          { return fs.dataCenter }
func (fs *SftpServer) WithFilerClient(streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) error {
	addr := fs.filerAddr.ToGrpcAddress()
	return pb.WithGrpcClient(streamingMode, util.RandomInt32(), func(conn *grpc.ClientConn) error {
		return fn(filer_pb.NewSeaweedFilerClient(conn))
	}, addr, false, fs.grpcDialOption)
}
func (fs *SftpServer) withTimeoutContext(fn func(ctx context.Context) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	return fn(ctx)
}

// ==================== Command Dispatcher ====================

func (fs *SftpServer) dispatchCmd(r *sftp.Request) error {
	glog.V(0).Infof("Dispatch: %s %s", r.Method, r.Filepath)
	switch r.Method {
	case "Remove":
		return fs.removeEntry(r)
	case "Rename":
		return fs.renameEntry(r)
	case "Mkdir":
		return fs.makeDir(r)
	case "Rmdir":
		return fs.removeDir(r)
	case "Setstat":
		return fs.setFileStat(r)
	default:
		return fmt.Errorf("unsupported: %s", r.Method)
	}
}

// ==================== File Operations ====================

func (fs *SftpServer) readFile(r *sftp.Request) (io.ReaderAt, error) {
	if err := fs.checkFilePermission(r.Filepath, "read"); err != nil {
		return nil, err
	}
	entry, err := fs.getEntry(r.Filepath)
	if err != nil {
		return nil, err
	}
	return &SeaweedFileReaderAt{fs: fs, entry: entry}, nil
}

// putFile uploads a file to the filer and sets ownership metadata.
func (fs *SftpServer) putFile(filepath string, data []byte, user *user.User) error {
	dir, filename := util.FullPath(filepath).DirAndName()
	uploadUrl := fmt.Sprintf("http://%s%s", fs.filerAddr, filepath)

	// Create a reader from our buffered data and calculate MD5 hash
	hash := md5.New()
	reader := bytes.NewReader(data)
	body := io.TeeReader(reader, hash)
	fileSize := int64(len(data))

	// Create and execute HTTP request
	proxyReq, err := http.NewRequest(http.MethodPut, uploadUrl, body)
	if err != nil {
		return fmt.Errorf("create request: %v", err)
	}
	proxyReq.ContentLength = fileSize
	proxyReq.Header.Set("Content-Type", "application/octet-stream")

	client := &http.Client{}
	resp, err := client.Do(proxyReq)
	if err != nil {
		return fmt.Errorf("upload to filer: %v", err)
	}
	defer resp.Body.Close()

	// Process response
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %v", err)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("upload failed with status %d: %s", resp.StatusCode, string(respBody))
	}

	var result weed_server.FilerPostResult
	if err := json.Unmarshal(respBody, &result); err != nil {
		return fmt.Errorf("parse response: %v", err)
	}

	if result.Error != "" {
		return fmt.Errorf("filer error: %s", result.Error)
	}

	// Update file ownership using the same pattern as other functions
	if user != nil {
		err := fs.callWithClient(false, func(ctx context.Context, client filer_pb.SeaweedFilerClient) error {
			// Look up the file to get its current entry
			lookupResp, err := client.LookupDirectoryEntry(ctx, &filer_pb.LookupDirectoryEntryRequest{
				Directory: dir,
				Name:      filename,
			})
			if err != nil {
				return fmt.Errorf("lookup file for attribute update: %v", err)
			}

			if lookupResp.Entry == nil {
				return fmt.Errorf("file not found after upload: %s/%s", dir, filename)
			}

			// Update the entry with new uid/gid
			entry := lookupResp.Entry
			entry.Attributes.Uid = user.Uid
			entry.Attributes.Gid = user.Gid

			// Update the entry in the filer
			_, err = client.UpdateEntry(ctx, &filer_pb.UpdateEntryRequest{
				Directory: dir,
				Entry:     entry,
			})
			return err
		})

		if err != nil {
			// Log the error but don't fail the whole operation
			glog.Errorf("Failed to update file ownership for %s: %v", filepath, err)
		}
	}

	return nil
}

func (fs *SftpServer) newFileWriter(r *sftp.Request) (io.WriterAt, error) {
	return &filerFileWriter{fs: *fs, req: r, permissions: 0644, uid: fs.user.Uid, gid: fs.user.Gid}, nil
}

func (fs *SftpServer) removeEntry(r *sftp.Request) error {
	return fs.deleteEntry(r.Filepath, false)
}

func (fs *SftpServer) renameEntry(r *sftp.Request) error {
	if err := fs.checkFilePermission(r.Filepath, "rename"); err != nil {
		return err
	}
	oldDir, oldName := util.FullPath(r.Filepath).DirAndName()
	newDir, newName := util.FullPath(r.Target).DirAndName()
	return fs.callWithClient(false, func(ctx context.Context, client filer_pb.SeaweedFilerClient) error {
		_, err := client.AtomicRenameEntry(ctx, &filer_pb.AtomicRenameEntryRequest{
			OldDirectory: oldDir, OldName: oldName,
			NewDirectory: newDir, NewName: newName,
		})
		return err
	})
}

func (fs *SftpServer) setFileStat(r *sftp.Request) error {
	if err := fs.checkFilePermission(r.Filepath, "write"); err != nil {
		return err
	}
	entry, err := fs.getEntry(r.Filepath)
	if err != nil {
		return err
	}
	dir, _ := util.FullPath(r.Filepath).DirAndName()
	// apply attrs
	if r.AttrFlags().Permissions {
		entry.Attributes.FileMode = uint32(r.Attributes().FileMode())
	}
	if r.AttrFlags().UidGid {
		entry.Attributes.Uid = uint32(r.Attributes().UID)
		entry.Attributes.Gid = uint32(r.Attributes().GID)
	}
	if r.AttrFlags().Acmodtime {
		entry.Attributes.Mtime = int64(r.Attributes().Mtime)
	}
	if r.AttrFlags().Size {
		entry.Attributes.FileSize = uint64(r.Attributes().Size)
	}
	return fs.updateEntry(dir, entry)
}

// ==================== Directory Operations ====================

func (fs *SftpServer) listDir(r *sftp.Request) (sftp.ListerAt, error) {
	if err := fs.checkFilePermission(r.Filepath, "list"); err != nil {
		return nil, err
	}
	if r.Method == "Stat" || r.Method == "Lstat" {
		entry, err := fs.getEntry(r.Filepath)
		if err != nil {
			return nil, err
		}
		fi := &EnhancedFileInfo{FileInfo: FileInfoFromEntry(entry), uid: entry.Attributes.Uid, gid: entry.Attributes.Gid}
		return listerat([]os.FileInfo{fi}), nil
	}
	return fs.listAllPages(r.Filepath)
}

func (fs *SftpServer) listAllPages(dirPath string) (sftp.ListerAt, error) {
	var all []os.FileInfo
	last := ""
	for {
		page, err := fs.fetchDirectoryPage(dirPath, last)
		if err != nil {
			return nil, err
		}
		all = append(all, page...)
		if len(page) < defaultListLimit {
			break
		}
		last = page[len(page)-1].Name()
	}
	return listerat(all), nil
}

func (fs *SftpServer) fetchDirectoryPage(dirPath, start string) ([]os.FileInfo, error) {
	var list []os.FileInfo
	err := fs.callWithClient(true, func(ctx context.Context, client filer_pb.SeaweedFilerClient) error {
		stream, err := client.ListEntries(ctx, &filer_pb.ListEntriesRequest{Directory: dirPath, StartFromFileName: start, Limit: defaultListLimit})
		if err != nil {
			return err
		}
		for {
			r, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil || r.Entry == nil {
				continue
			}
			p := path.Join(dirPath, r.Entry.Name)
			if err := fs.checkFilePermission(p, "list"); err != nil {
				continue
			}
			list = append(list, &EnhancedFileInfo{FileInfo: FileInfoFromEntry(r.Entry), uid: r.Entry.Attributes.Uid, gid: r.Entry.Attributes.Gid})
		}
		return nil
	})
	return list, err
}

// makeDir creates a new directory with proper permissions.
func (fs *SftpServer) makeDir(r *sftp.Request) error {
	if fs.user == nil {
		return fmt.Errorf("cannot create directory: no user info")
	}
	dir, name := util.FullPath(r.Filepath).DirAndName()
	if err := fs.checkFilePermission(dir, "mkdir"); err != nil {
		return err
	}
	// default mode and ownership
	err := filer_pb.Mkdir(context.Background(), fs, string(dir), name, func(entry *filer_pb.Entry) {
		mode := uint32(0755 | os.ModeDir)
		if strings.HasPrefix(r.Filepath, fs.user.HomeDir) {
			mode = uint32(0700 | os.ModeDir)
		}
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
	return err
}

// removeDir deletes a directory.
func (fs *SftpServer) removeDir(r *sftp.Request) error {
	return fs.deleteEntry(r.Filepath, false)
}

// ==================== Common Arguments Helpers ====================

func FileInfoFromEntry(e *filer_pb.Entry) FileInfo {
	return FileInfo{name: e.Name, size: int64(e.Attributes.FileSize), mode: os.FileMode(e.Attributes.FileMode), modTime: time.Unix(e.Attributes.Mtime, 0), isDir: e.IsDirectory}
}

func (fs *SftpServer) deleteEntry(p string, recursive bool) error {
	if err := fs.checkFilePermission(p, "delete"); err != nil {
		return err
	}
	dir, name := util.FullPath(p).DirAndName()
	return fs.callWithClient(false, func(ctx context.Context, client filer_pb.SeaweedFilerClient) error {
		r, err := client.DeleteEntry(ctx, &filer_pb.DeleteEntryRequest{Directory: dir, Name: name, IsDeleteData: true, IsRecursive: recursive})
		if err != nil {
			return err
		}
		if r.Error != "" {
			return fmt.Errorf("%s", r.Error)
		}
		return nil
	})
}

// ==================== Custom Types ====================

type EnhancedFileInfo struct {
	FileInfo
	uid uint32
	gid uint32
}

func (fi *EnhancedFileInfo) Sys() interface{} {
	return &syscall.Stat_t{Uid: fi.uid, Gid: fi.gid}
}

func (fi *EnhancedFileInfo) Owner() (uid, gid int) {
	return int(fi.uid), int(fi.gid)
}

// SeaweedFileReaderAt implements io.ReaderAt for SeaweedFS files

type SeaweedFileReaderAt struct {
	fs    *SftpServer
	entry *filer_pb.Entry
}

func (ra *SeaweedFileReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	// Create a new reader for each ReadAt call
	reader := filer.NewFileReader(context.Background(), ra.fs, ra.entry)
	if reader == nil {
		return 0, fmt.Errorf("failed to create file reader")
	}

	// Check if we're reading past the end of the file
	fileSize := int64(ra.entry.Attributes.FileSize)
	if off >= fileSize {
		return 0, io.EOF
	}

	// Seek to the offset
	if seeker, ok := reader.(io.Seeker); ok {
		_, err = seeker.Seek(off, io.SeekStart)
		if err != nil {
			return 0, fmt.Errorf("seek error: %v", err)
		}
	} else {
		// If the reader doesn't implement Seek, we need to read and discard bytes
		toSkip := off
		skipBuf := make([]byte, 8192)
		for toSkip > 0 {
			skipSize := int64(len(skipBuf))
			if skipSize > toSkip {
				skipSize = toSkip
			}
			read, err := reader.Read(skipBuf[:skipSize])
			if err != nil {
				return 0, fmt.Errorf("skip error: %v", err)
			}
			if read == 0 {
				return 0, fmt.Errorf("unable to skip to offset %d", off)
			}
			toSkip -= int64(read)
		}
	}

	// Adjust read length if it would go past EOF
	readLen := len(p)
	remaining := fileSize - off
	if int64(readLen) > remaining {
		readLen = int(remaining)
		if readLen == 0 {
			return 0, io.EOF
		}
	}

	// Read the data
	n, err = io.ReadFull(reader, p[:readLen])

	// Handle EOF correctly
	if err == io.ErrUnexpectedEOF || (err == nil && n < len(p)) {
		err = io.EOF
	}

	return n, err
}

func (fs *SftpServer) checkFilePermission(filepath string, permissions string) error {
	return fs.authManager.CheckPermission(fs.user, filepath, permissions)
}
