// sftp_filer_refactored.go
package sftpd

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/pkg/sftp"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	filer_pb "github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	weed_server "github.com/seaweedfs/seaweedfs/weed/server"
	"github.com/seaweedfs/seaweedfs/weed/sftpd/user"
	"github.com/seaweedfs/seaweedfs/weed/util"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
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
			if isNotExistError(err) {
				return os.ErrNotExist
			}
			return err
		}
		if r.Entry == nil {
			return fmt.Errorf("%s not found in %s", name, dir)
		}
		entry = r.Entry
		return nil
	})
	if err != nil {
		if isNotExistError(err) {
			return nil, os.ErrNotExist
		}
		return nil, fmt.Errorf("lookup %s: %w", p, err)
	}
	return entry, nil
}

func isNotExistError(err error) bool {
	return strings.Contains(err.Error(), "not found") ||
		strings.Contains(err.Error(), "no entry is found") ||
		strings.Contains(err.Error(), "file does not exist") ||
		err == os.ErrNotExist
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
	return pb.WithGrpcClient(context.Background(), streamingMode, util.RandomInt32(), func(conn *grpc.ClientConn) error {
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
	absPath, err := fs.toAbsolutePath(r.Filepath)
	if err != nil {
		return err
	}
	glog.V(1).Infof("Dispatch: %s %s (absolute: %s)", r.Method, r.Filepath, absPath)
	switch r.Method {
	case "Remove":
		return fs.removeEntry(absPath)
	case "Rename":
		absTarget, err := fs.toAbsolutePath(r.Target)
		if err != nil {
			return err
		}
		return fs.renameEntry(absPath, absTarget)
	case "Mkdir":
		return fs.makeDir(absPath)
	case "Rmdir":
		return fs.removeDir(absPath)
	case "Setstat":
		return fs.setFileStatWithRequest(absPath, r)
	default:
		return fmt.Errorf("unsupported: %s", r.Method)
	}
}

// ==================== File Operations ====================

func (fs *SftpServer) readFile(r *sftp.Request) (io.ReaderAt, error) {
	absPath, err := fs.toAbsolutePath(r.Filepath)
	if err != nil {
		return nil, err
	}
	if err := fs.checkFilePermission(absPath, "read"); err != nil {
		return nil, err
	}
	entry, err := fs.getEntry(absPath)
	if err != nil {
		return nil, err
	}
	return NewSeaweedFileReaderAt(fs, entry), nil
}

func (fs *SftpServer) newFileWriter(r *sftp.Request) (io.WriterAt, error) {
	absPath, err := fs.toAbsolutePath(r.Filepath)
	if err != nil {
		return nil, err
	}
	if err := fs.checkFilePermission(absPath, "write"); err != nil {
		glog.Errorf("Permission denied for %s", absPath)
		return nil, err
	}
	// Create a temporary file to buffer writes
	tmpFile, err := os.CreateTemp("", "sftp-upload-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %w", err)
	}
	// Without O_TRUNC the client writes into the existing file, so start from its current bytes.
	if !r.Pflags().Trunc {
		if err := fs.copyExistingContent(absPath, tmpFile); err != nil {
			tmpFile.Close()
			os.Remove(tmpFile.Name())
			return nil, err
		}
	}

	return &SeaweedSftpFileWriter{
		fs:          *fs,
		req:         r,
		absPath:     absPath,
		tmpFile:     tmpFile,
		permissions: 0644,
		uid:         fs.user.Uid,
		gid:         fs.user.Gid,
		offset:      0,
	}, nil
}

func (fs *SftpServer) copyExistingContent(absPath string, dst io.Writer) error {
	entry, err := fs.getEntry(absPath)
	if err == os.ErrNotExist {
		return nil
	}
	if err != nil {
		return err
	}
	if entry.IsDirectory {
		return nil
	}
	size := int64(filer.FileSize(entry))
	remote := entry.GetRemoteEntry()
	if len(entry.Content) == 0 && len(entry.GetChunks()) == 0 {
		if remote != nil && remote.RemoteSize > 0 {
			return fmt.Errorf("existing content of %s is not readable", absPath)
		}
		if size == 0 {
			return nil
		}
		return writeZeros(dst, size)
	}
	if size == 0 {
		return nil
	}
	if len(entry.Content) > 0 {
		if _, err := dst.Write(entry.Content); err != nil {
			return err
		}
		if remaining := size - int64(len(entry.Content)); remaining > 0 {
			return writeZeros(dst, remaining)
		}
		return nil
	}
	streamFn, err := filer.PrepareStreamContent(lookupFileIdFn(filer.LookupFn(fs)), filer.JwtForVolumeServer, entry.GetChunks(), 0, size)
	if err != nil {
		return fmt.Errorf("read existing content of %s: %w", absPath, err)
	}
	if err := streamFn(dst); err != nil {
		return fmt.Errorf("read existing content of %s: %w", absPath, err)
	}
	return nil
}

// lookupFileIdFn adapts a filer LookupFn to the chunk streamer's provider
// interface. PrepareStreamContent fills unstored gaps with zeros, so a sparse
// file's tail survives the copy.
type lookupFileIdFn wdclient.LookupFileIdFunctionType

func (fn lookupFileIdFn) GetLookupFileIdFunction() wdclient.LookupFileIdFunctionType {
	return wdclient.LookupFileIdFunctionType(fn)
}

func writeZeros(w io.Writer, size int64) error {
	zeros := make([]byte, 8192)
	for size > 0 {
		n := int64(len(zeros))
		if size < n {
			n = size
		}
		written, err := w.Write(zeros[:n])
		size -= int64(written)
		if err != nil {
			return err
		}
	}
	return nil
}

func (fs *SftpServer) removeEntry(absPath string) error {
	return fs.deleteEntry(absPath, false)
}

func (fs *SftpServer) renameEntry(absPath, absTarget string) error {
	if err := fs.checkFilePermission(absPath, "rename"); err != nil {
		return err
	}
	targetDir, _ := util.FullPath(absTarget).DirAndName()
	if err := fs.checkFilePermission(targetDir, "write"); err != nil {
		return err
	}
	requireAbsentTarget := false
	if path.Clean(absPath) != path.Clean(absTarget) {
		var err error
		requireAbsentTarget, err = fs.checkRenameTargetDelete(absTarget, targetDir)
		if err != nil {
			return err
		}
	}
	if err := fs.checkRenameKeepsAcl(absPath, absTarget); err != nil {
		return err
	}
	oldDir, oldName := util.FullPath(absPath).DirAndName()
	newDir, newName := util.FullPath(absTarget).DirAndName()
	return fs.callWithClient(false, func(ctx context.Context, client filer_pb.SeaweedFilerClient) error {
		_, err := client.AtomicRenameEntry(ctx, &filer_pb.AtomicRenameEntryRequest{
			OldDirectory: oldDir, OldName: oldName,
			NewDirectory: newDir, NewName: newName,
			// A target the user may not delete must stay absent: the filer
			// checks it again under the rename locks, where a create cannot
			// slip in.
			FailIfTargetExists: requireAbsentTarget,
		})
		return err
	})
}

// checkRenameTargetDelete enforces delete access where a rename can destroy an
// entry. Overwriting an existing target deletes it, so it needs delete access
// on the target itself. A name that does not exist yet needs no delete check
// unless a configured ACL restricts that name itself: enforcing such an ACL
// even while the entry is absent keeps a concurrent create from slipping a
// protected target under the rename. When the target name simply inherits its
// directory's ACL and the user could not delete an entry there, the rename is
// still allowed for a new name but the caller is told to require the target to
// stay absent, so the filer rejects the overwrite of one created in the gap.
// Lookup failures other than not-found fail closed.
func (fs *SftpServer) checkRenameTargetDelete(absTarget, targetDir string) (requireAbsentTarget bool, err error) {
	if _, err := fs.getEntry(absTarget); err == nil {
		return false, fs.checkFilePermission(absTarget, "delete")
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	if fs.user == nil || fs.user.Username == "root" || fs.user.Uid == 0 {
		return false, nil
	}
	if matchingAclPrefix(fs.user, absTarget) != matchingAclPrefix(fs.user, targetDir) {
		if !HasExplicitPermission(fs.user, absTarget, PermDelete, false) {
			glog.V(0).Infof("permission denied for user %s: rename target %s carries a delete-protected ACL", fs.user.Username, absTarget)
			return false, os.ErrPermission
		}
		return false, nil
	}
	if err := fs.checkFilePermission(absTarget, "delete"); err != nil {
		if errors.Is(err, os.ErrPermission) {
			return true, nil
		}
		return false, err
	}
	return false, nil
}

// matchingAclPrefix returns the longest configured ACL path that applies to p,
// or "" when no configured path covers it.
func matchingAclPrefix(user *user.User, p string) string {
	best := ""
	for aclPath := range user.Permissions {
		cleaned := path.Clean(aclPath)
		if pathWithin(cleaned, p) && len(cleaned) > len(best) {
			best = cleaned
		}
	}
	return best
}

// checkRenameKeepsAcl denies a rename that would move entries to locations
// where ACLs grant the user read, list, write, or delete access the source
// side lacked. It compares the moved root and every configured ACL path under
// either the source or the target (mapped back to the source) so both
// inherited restrictions and deeper destination grants are caught.
func (fs *SftpServer) checkRenameKeepsAcl(absPath, absTarget string) error {
	if fs.user == nil || fs.user.Username == "root" || fs.user.Uid == 0 {
		return nil
	}
	type aclPair struct {
		src, dst string
		isDir    bool
	}
	srcEntry, err := fs.getEntry(absPath)
	if err != nil {
		return err
	}
	pairs := []aclPair{{absPath, absTarget, srcEntry.IsDirectory}}
	seen := map[string]bool{absPath: true}
	for p := range fs.user.Permissions {
		cleaned := path.Clean(p)
		var src string
		switch {
		case pathWithin(absPath, cleaned):
			src = cleaned
		case pathWithin(absTarget, cleaned):
			src = path.Join(absPath, strings.TrimPrefix(cleaned, absTarget))
		default:
			continue
		}
		if seen[src] {
			continue
		}
		seen[src] = true
		// A rule only protects an entry that actually moves; a path reserved
		// for a missing child must not block the rename. A lookup error is not
		// proof that the child is missing, so only os.ErrNotExist may skip.
		entry, err := fs.getEntry(src)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return err
		}
		relocated := path.Join(absTarget, strings.TrimPrefix(src, absPath))
		pairs = append(pairs, aclPair{src, relocated, entry.IsDirectory})
	}
	for _, pair := range pairs {
		// A moved directory carries file children whose ACLs resolve with
		// isDirectory=false, where list no longer counts as read. Comparing
		// only the directory shape would miss children gaining read, so a
		// directory is checked in the file shape too.
		shapes := []bool{pair.isDir}
		if pair.isDir {
			shapes = append(shapes, false)
		}
		for _, isDir := range shapes {
			for _, perm := range []string{PermRead, PermList, PermWrite, PermDelete} {
				if !HasExplicitPermission(fs.user, pair.src, perm, isDir) && HasExplicitPermission(fs.user, pair.dst, perm, isDir) {
					glog.V(0).Infof("permission denied for user %s: rename of %s to %s would drop the ACL on %s", fs.user.Username, absPath, absTarget, pair.src)
					return os.ErrPermission
				}
			}
		}
	}
	return nil
}

func (fs *SftpServer) setFileStatWithRequest(absPath string, r *sftp.Request) error {
	if err := fs.checkFilePermission(absPath, "write"); err != nil {
		return err
	}
	entry, err := fs.getEntry(absPath)
	if err != nil {
		return err
	}
	dir, _ := util.FullPath(absPath).DirAndName()
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
	absPath, err := fs.toAbsolutePath(r.Filepath)
	if err != nil {
		return nil, err
	}
	if err := fs.checkFilePermission(absPath, "list"); err != nil {
		return nil, err
	}
	if r.Method == "Stat" || r.Method == "Lstat" {
		entry, err := fs.getEntry(absPath)
		if err != nil {
			return nil, err
		}
		fi := &EnhancedFileInfo{FileInfo: FileInfoFromEntry(entry), uid: entry.Attributes.Uid, gid: entry.Attributes.Gid}
		return listerat([]os.FileInfo{fi}), nil
	}
	return fs.listAllPages(absPath)
}

func (fs *SftpServer) listAllPages(dirPath string) (sftp.ListerAt, error) {
	var all []os.FileInfo
	last := ""
	for {
		page, lastName, received, err := fs.fetchDirectoryPage(dirPath, last)
		if err != nil {
			return nil, err
		}
		all = append(all, page...)
		if received < defaultListLimit {
			break
		}
		last = lastName
	}
	return listerat(all), nil
}

// fetchDirectoryPage returns the entries the user may list, plus the last name and count the filer sent.
func (fs *SftpServer) fetchDirectoryPage(dirPath, start string) ([]os.FileInfo, string, int, error) {
	var list []os.FileInfo
	lastName := ""
	received := 0
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
			if err != nil {
				return err
			}
			if r.Entry == nil {
				continue
			}
			received++
			lastName = r.Entry.Name
			p := path.Join(dirPath, r.Entry.Name)
			if err := fs.checkFilePermission(p, "list"); err != nil {
				continue
			}
			list = append(list, &EnhancedFileInfo{FileInfo: FileInfoFromEntry(r.Entry), uid: r.Entry.Attributes.Uid, gid: r.Entry.Attributes.Gid})
		}
		return nil
	})
	return list, lastName, received, err
}

// makeDir creates a new directory with proper permissions.
func (fs *SftpServer) makeDir(absPath string) error {
	if fs.user == nil {
		return fmt.Errorf("cannot create directory: no user info")
	}
	dir, name := util.FullPath(absPath).DirAndName()
	if err := fs.checkFilePermission(dir, "write"); err != nil {
		return err
	}
	// default mode and ownership
	err := filer_pb.Mkdir(context.Background(), fs, string(dir), name, func(entry *filer_pb.Entry) {
		mode := uint32(0755 | os.ModeDir)
		// Defensive check: all paths should be under HomeDir after toAbsolutePath translation
		if absPath == fs.user.HomeDir || strings.HasPrefix(absPath, fs.user.HomeDir+"/") {
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
func (fs *SftpServer) removeDir(absPath string) error {
	return fs.deleteEntry(absPath, false)
}

func (fs *SftpServer) putFile(filepath string, reader io.Reader, user *user.User) error {
	dir, filename := util.FullPath(filepath).DirAndName()
	// Escape the path so a "?" in the filename cannot inject filer query commands like cp.from/mv.from.
	uploadUrl := (&url.URL{Scheme: "http", Host: fs.filerAddr.ToHttpAddress(), Path: filepath}).String()
	// Let the global HTTP client normalize the scheme to https:// when TLS is configured
	normalizedUrl, err := util_http.NormalizeUrl(uploadUrl)
	if err != nil {
		return fmt.Errorf("normalize upload url %q: %w", uploadUrl, err)
	}
	uploadUrl = normalizedUrl

	// Compute MD5 while uploading
	hash := md5.New()
	body := io.TeeReader(reader, hash)

	// We can skip ContentLength if unknown (chunked transfer encoding)
	req, err := http.NewRequest(http.MethodPut, uploadUrl, body)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	// Add JWT authorization if filer signing key is configured
	if len(fs.filerSigningKey) > 0 {
		jwt := security.GenJwtForFilerServer(security.SigningKey(fs.filerSigningKey), fs.filerSigningExpiresAfter)
		if jwt != "" {
			req.Header.Set("Authorization", security.BearerPrefix+string(jwt))
		}
	}

	resp, err := util_http.Do(req)
	if err != nil {
		return fmt.Errorf("upload to filer: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("upload failed with status %d: %s", resp.StatusCode, string(respBody))
	}

	var result weed_server.FilerPostResult
	if err := json.Unmarshal(respBody, &result); err != nil {
		return fmt.Errorf("parse response: %w", err)
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
				return fmt.Errorf("lookup file for attribute update: %w", err)
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

// FileStat represents file statistics in a platform-independent way
type FileStat struct {
	Uid uint32
	Gid uint32
}

func (fi *EnhancedFileInfo) Sys() interface{} {
	return &FileStat{Uid: fi.uid, Gid: fi.gid}
}

func (fi *EnhancedFileInfo) Owner() (uid, gid int) {
	return int(fi.uid), int(fi.gid)
}

func (fs *SftpServer) checkFilePermission(filepath string, permissions string) error {
	return fs.CheckFilePermission(filepath, permissions)
}
