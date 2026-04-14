package nfs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	billy "github.com/go-git/go-billy/v5"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/chunk_cache"
	gonfsfile "github.com/willscott/go-nfs/file"
	"google.golang.org/protobuf/proto"
)

const (
	maxInlineWriteSize   = 4 << 20
	maxBufferedWriteSize = 64 << 20
)

type noopChunkCache struct{}

func (noopChunkCache) ReadChunkAt(_ []byte, _ string, _ uint64) (int, error) { return 0, nil }
func (noopChunkCache) SetChunk(_ string, _ []byte)                           {}
func (noopChunkCache) IsInCache(_ string, _ bool) bool                       { return false }
func (noopChunkCache) GetMaxFilePartSizeInCache() uint64                     { return 0 }

type seaweedFileSystem struct {
	server      *Server
	actualRoot  util.FullPath
	readerCache *filer.ReaderCache
}

type seaweedFileInfo struct {
	name        string
	virtualPath string
	size        int64
	mode        os.FileMode
	modTime     time.Time
	actualPath  util.FullPath
	entry       *filer_pb.Entry
	generation  uint64
	fileID      uint64
	nlink       uint32
}

type seaweedFile struct {
	fs          *seaweedFileSystem
	virtualPath string
	info        *seaweedFileInfo
	reader      io.ReaderAt
	offset      int64
	writable    bool
	content     []byte
	dirty       bool
	closed      bool
}

var _ billy.Filesystem = (*seaweedFileSystem)(nil)
var _ billy.Capable = (*seaweedFileSystem)(nil)
var _ billy.Change = (*seaweedFileSystem)(nil)
var _ filer_pb.FilerClient = (*seaweedFileSystem)(nil)

func newSeaweedFileSystem(server *Server, actualRoot util.FullPath, sharedReaderCache *filer.ReaderCache) *seaweedFileSystem {
	fs := &seaweedFileSystem{
		server:     server,
		actualRoot: normalizeExportRoot(actualRoot),
	}
	if sharedReaderCache != nil {
		fs.readerCache = sharedReaderCache
	} else {
		fs.readerCache = filer.NewReaderCache(32, chunk_cache.ChunkCache(noopChunkCache{}), filer.LookupFn(fs))
	}
	return fs
}

func (fs *seaweedFileSystem) Capabilities() billy.Capability {
	return billy.WriteCapability | billy.ReadCapability | billy.ReadAndWriteCapability |
		billy.SeekCapability | billy.TruncateCapability | billy.LockCapability
}

func (fs *seaweedFileSystem) Create(filename string) (billy.File, error) {
	return fs.OpenFile(filename, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o666)
}

func (fs *seaweedFileSystem) Open(filename string) (billy.File, error) {
	return fs.openFile(context.Background(), filename, os.O_RDONLY, 0)
}

func (fs *seaweedFileSystem) OpenFile(filename string, flag int, perm os.FileMode) (billy.File, error) {
	return fs.openFile(context.Background(), filename, flag, perm)
}

func (fs *seaweedFileSystem) openFile(ctx context.Context, filename string, flag int, perm os.FileMode) (billy.File, error) {
	virtualPath := cleanBillyPath(filename)
	writable := flag&(os.O_WRONLY|os.O_RDWR|os.O_APPEND|os.O_CREATE|os.O_TRUNC|os.O_EXCL) != 0

	info, err := fs.ensureOpenEntry(ctx, virtualPath, flag, perm)
	if err != nil {
		return nil, err
	}
	if info.entry.IsDirectory {
		return nil, fmt.Errorf("%s: is a directory", filename)
	}
	file := &seaweedFile{
		fs:          fs,
		virtualPath: virtualPath,
		info:        info,
		writable:    writable,
	}
	if writable {
		content, contentErr := fs.loadWritableContent(ctx, info)
		if contentErr != nil {
			return nil, contentErr
		}
		file.content = content
		if flag&os.O_TRUNC != 0 {
			file.content = nil
			file.dirty = true
		}
		if flag&os.O_APPEND != 0 {
			file.offset = int64(len(file.content))
		}
	}
	return file, nil
}

func (fs *seaweedFileSystem) Stat(filename string) (os.FileInfo, error) {
	return fs.fileInfoForVirtualPath(context.Background(), filename)
}

func (fs *seaweedFileSystem) Lstat(filename string) (os.FileInfo, error) {
	return fs.fileInfoForVirtualPath(context.Background(), filename)
}

func (fs *seaweedFileSystem) Rename(oldpath, newpath string) error {
	oldVirtualPath, oldActualPath := fs.resolvePath(oldpath)
	_, newActualPath := fs.resolvePath(newpath)

	if oldVirtualPath == "/" || cleanBillyPath(newpath) == "/" {
		return os.ErrPermission
	}
	if _, err := fs.fileInfoForVirtualPath(context.Background(), oldVirtualPath); err != nil {
		return err
	}

	oldDir, oldName := oldActualPath.DirAndName()
	newDir, newName := newActualPath.DirAndName()
	return fs.server.withInternalClient(false, func(client nfsFilerClient) error {
		_, err := client.AtomicRenameEntry(context.Background(), &filer_pb.AtomicRenameEntryRequest{
			OldDirectory: oldDir,
			OldName:      oldName,
			NewDirectory: newDir,
			NewName:      newName,
		})
		if err != nil {
			if isLookupNotFound(err) {
				return os.ErrNotExist
			}
			return err
		}
		return nil
	})
}

func (fs *seaweedFileSystem) Remove(filename string) error {
	virtualPath, actualPath := fs.resolvePath(filename)
	if virtualPath == "/" {
		return os.ErrPermission
	}
	if _, err := fs.fileInfoForVirtualPath(context.Background(), virtualPath); err != nil {
		return err
	}

	dir, name := actualPath.DirAndName()
	return fs.server.withInternalClient(false, func(client nfsFilerClient) error {
		resp, err := client.DeleteEntry(context.Background(), &filer_pb.DeleteEntryRequest{
			Directory:    dir,
			Name:         name,
			IsDeleteData: false,
			IsRecursive:  false,
		})
		if err != nil {
			if isLookupNotFound(err) {
				return os.ErrNotExist
			}
			return err
		}
		if resp != nil && resp.Error != "" {
			if strings.Contains(resp.Error, filer_pb.ErrNotFound.Error()) {
				return os.ErrNotExist
			}
			return errors.New(resp.Error)
		}
		return nil
	})
}

func (fs *seaweedFileSystem) Join(elem ...string) string {
	if len(elem) == 0 {
		return "/"
	}
	joined := path.Join(elem...)
	if joined == "." || joined == "" {
		return "/"
	}
	if !strings.HasPrefix(joined, "/") {
		joined = "/" + joined
	}
	return path.Clean(joined)
}

func (fs *seaweedFileSystem) TempFile(string, string) (billy.File, error) {
	return nil, billy.ErrReadOnly
}

func (fs *seaweedFileSystem) ReadDir(dirname string) ([]os.FileInfo, error) {
	ctx := context.Background()
	virtualPath, actualPath := fs.resolvePath(dirname)

	var infos []os.FileInfo
	err := fs.server.withInternalClient(false, func(client nfsFilerClient) error {
		stream, err := client.ListEntries(ctx, &filer_pb.ListEntriesRequest{
			Directory: string(actualPath),
			Limit:     math.MaxInt32,
		})
		if err != nil {
			if isLookupNotFound(err) {
				return os.ErrNotExist
			}
			return err
		}

		for {
			resp, recvErr := stream.Recv()
			if recvErr == io.EOF {
				break
			}
			if recvErr != nil {
				return recvErr
			}
			if resp == nil || resp.Entry == nil {
				continue
			}

			childVirtualPath := path.Join(virtualPath, resp.Entry.Name)
			childActualPath := util.NewFullPath(string(actualPath), resp.Entry.Name)
			info, infoErr := fs.materializeFileInfo(ctx, childVirtualPath, childActualPath, resp.Entry)
			if infoErr != nil {
				return infoErr
			}
			infos = append(infos, info)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	sort.Slice(infos, func(i, j int) bool {
		return infos[i].Name() < infos[j].Name()
	})
	return infos, nil
}

func (fs *seaweedFileSystem) MkdirAll(filename string, perm os.FileMode) error {
	virtualPath := cleanBillyPath(filename)
	if virtualPath == "/" {
		return nil
	}

	info, err := fs.fileInfoForVirtualPath(context.Background(), virtualPath)
	if err == nil {
		if info.IsDir() {
			return nil
		}
		return os.ErrExist
	}
	if !os.IsNotExist(err) {
		return err
	}

	_, actualPath := fs.resolvePath(virtualPath)
	_, err = fs.createEntry(context.Background(), actualPath, true, perm|os.ModeDir, "")
	return err
}

func (fs *seaweedFileSystem) Symlink(target, link string) error {
	virtualPath, actualPath := fs.resolvePath(link)
	if virtualPath == "/" {
		return os.ErrPermission
	}
	if _, err := fs.fileInfoForVirtualPath(context.Background(), virtualPath); err == nil {
		return os.ErrExist
	} else if !os.IsNotExist(err) {
		return err
	}

	_, err := fs.createEntry(context.Background(), actualPath, false, 0o777, target)
	return err
}

func (fs *seaweedFileSystem) Readlink(link string) (string, error) {
	info, err := fs.fileInfoForVirtualPath(context.Background(), link)
	if err != nil {
		return "", err
	}
	if info.entry.Attributes == nil || info.entry.Attributes.SymlinkTarget == "" {
		return "", billy.ErrNotSupported
	}
	return info.entry.Attributes.SymlinkTarget, nil
}

func (fs *seaweedFileSystem) Chroot(p string) (billy.Filesystem, error) {
	info, err := fs.fileInfoForVirtualPath(context.Background(), p)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("%s: not a directory", p)
	}
	return newSeaweedFileSystem(fs.server, info.actualPath, fs.readerCache), nil
}

func (fs *seaweedFileSystem) Chmod(name string, mode os.FileMode) error {
	_, actualPath := fs.resolvePath(name)
	_, err := fs.mutateEntry(context.Background(), actualPath, func(entry *filer_pb.Entry) {
		entry.Attributes.FileMode = uint32(mode)
		touchEntryTimes(entry, false)
	})
	return err
}

func (fs *seaweedFileSystem) Lchown(name string, uid, gid int) error {
	_, actualPath := fs.resolvePath(name)
	_, err := fs.mutateEntry(context.Background(), actualPath, func(entry *filer_pb.Entry) {
		entry.Attributes.Uid = uint32(uid)
		entry.Attributes.Gid = uint32(gid)
		touchEntryTimes(entry, false)
	})
	return err
}

func (fs *seaweedFileSystem) Chown(name string, uid, gid int) error {
	return fs.Lchown(name, uid, gid)
}

func (fs *seaweedFileSystem) Chtimes(name string, _ time.Time, mtime time.Time) error {
	_, actualPath := fs.resolvePath(name)
	_, err := fs.mutateEntry(context.Background(), actualPath, func(entry *filer_pb.Entry) {
		entry.Attributes.Mtime = mtime.Unix()
		entry.Attributes.MtimeNs = int32(mtime.Nanosecond())
		entry.Attributes.Ctime = mtime.Unix()
		entry.Attributes.CtimeNs = int32(mtime.Nanosecond())
	})
	return err
}

func (fs *seaweedFileSystem) Root() string {
	return "/"
}

func (fs *seaweedFileSystem) WithFilerClient(streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) error {
	return fs.server.WithFilerClient(streamingMode, fn)
}

func (fs *seaweedFileSystem) AdjustedUrl(location *filer_pb.Location) string {
	if location == nil {
		return ""
	}
	if fs.server.option.VolumeServerAccess == "publicUrl" && location.PublicUrl != "" {
		return location.PublicUrl
	}
	return location.Url
}

func (fs *seaweedFileSystem) GetDataCenter() string {
	return ""
}

func (fs *seaweedFileSystem) resolvePath(name string) (string, util.FullPath) {
	virtualPath := cleanBillyPath(name)
	if virtualPath == "/" {
		return virtualPath, fs.actualRoot
	}
	return virtualPath, fs.actualRoot.Child(strings.TrimPrefix(virtualPath, "/"))
}

func (fs *seaweedFileSystem) ensureOpenEntry(ctx context.Context, virtualPath string, flag int, perm os.FileMode) (*seaweedFileInfo, error) {
	info, err := fs.fileInfoForVirtualPath(ctx, virtualPath)
	if err == nil {
		if flag&os.O_CREATE != 0 && flag&os.O_EXCL != 0 {
			return nil, os.ErrExist
		}
		return info, nil
	}
	if !os.IsNotExist(err) {
		return nil, err
	}
	if flag&os.O_CREATE == 0 {
		return nil, err
	}

	_, actualPath := fs.resolvePath(virtualPath)
	if perm == 0 {
		perm = 0o666
	}
	entry, createErr := fs.createEntry(ctx, actualPath, false, perm, "")
	if createErr != nil {
		return nil, createErr
	}
	return fs.materializeFileInfo(ctx, virtualPath, actualPath, entry)
}

func (fs *seaweedFileSystem) createEntry(ctx context.Context, actualPath util.FullPath, isDirectory bool, mode os.FileMode, symlinkTarget string) (*filer_pb.Entry, error) {
	dir, name := actualPath.DirAndName()
	now := time.Now()
	entry := &filer_pb.Entry{
		Name:        name,
		IsDirectory: isDirectory,
		Attributes: &filer_pb.FuseAttributes{
			Mtime:    now.Unix(),
			MtimeNs:  int32(now.Nanosecond()),
			Ctime:    now.Unix(),
			CtimeNs:  int32(now.Nanosecond()),
			Crtime:   now.Unix(),
			FileMode: uint32(mode),
			Uid:      filer_pb.OS_UID,
			Gid:      filer_pb.OS_GID,
		},
	}
	if isDirectory {
		entry.Attributes.FileMode = uint32(mode | os.ModeDir)
	}
	if symlinkTarget != "" {
		entry.Attributes.SymlinkTarget = symlinkTarget
	}

	var createdEntry *filer_pb.Entry
	err := fs.server.withInternalClient(false, func(client nfsFilerClient) error {
		resp, err := client.CreateEntry(ctx, &filer_pb.CreateEntryRequest{
			Directory: dir,
			Entry:     entry,
			OExcl:     false,
		})
		if err != nil {
			if errors.Is(err, filer_pb.ErrEntryAlreadyExists) {
				return os.ErrExist
			}
			return err
		}
		if resp != nil {
			if resp.ErrorCode != filer_pb.FilerError_OK {
				if sentinel := filer_pb.FilerErrorToSentinel(resp.ErrorCode); sentinel != nil {
					if errors.Is(sentinel, filer_pb.ErrEntryAlreadyExists) {
						return os.ErrExist
					}
					return sentinel
				}
				if resp.Error != "" {
					return errors.New(resp.Error)
				}
			}
			if resp.MetadataEvent != nil && resp.MetadataEvent.EventNotification != nil && resp.MetadataEvent.EventNotification.NewEntry != nil {
				createdEntry = resp.MetadataEvent.EventNotification.NewEntry
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if createdEntry != nil {
		return createdEntry, nil
	}
	return fs.lookupEntry(ctx, actualPath)
}

func (fs *seaweedFileSystem) mutateEntry(ctx context.Context, actualPath util.FullPath, mutate func(*filer_pb.Entry)) (*filer_pb.Entry, error) {
	currentEntry, err := fs.lookupEntry(ctx, actualPath)
	if err != nil {
		return nil, err
	}

	clonedEntry, ok := proto.Clone(currentEntry).(*filer_pb.Entry)
	if !ok {
		return nil, errors.New("clone filer entry")
	}
	if clonedEntry.Attributes == nil {
		clonedEntry.Attributes = &filer_pb.FuseAttributes{}
	}

	mutate(clonedEntry)

	dir, _ := actualPath.DirAndName()
	var updatedEntry *filer_pb.Entry
	err = fs.server.withInternalClient(false, func(client nfsFilerClient) error {
		resp, err := client.UpdateEntry(ctx, &filer_pb.UpdateEntryRequest{
			Directory: dir,
			Entry:     clonedEntry,
		})
		if err != nil {
			return err
		}
		if resp != nil && resp.MetadataEvent != nil && resp.MetadataEvent.EventNotification != nil && resp.MetadataEvent.EventNotification.NewEntry != nil {
			updatedEntry = resp.MetadataEvent.EventNotification.NewEntry
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if updatedEntry != nil {
		return updatedEntry, nil
	}
	return fs.lookupEntry(ctx, actualPath)
}

func (fs *seaweedFileSystem) loadWritableContent(ctx context.Context, info *seaweedFileInfo) ([]byte, error) {
	if info == nil || info.entry == nil {
		return nil, os.ErrNotExist
	}
	if len(info.entry.Content) > 0 {
		return bytes.Clone(info.entry.Content), nil
	}

	fileSize := info.size
	if fileSize == 0 {
		return nil, nil
	}
	if fileSize > maxBufferedWriteSize {
		return nil, billy.ErrNotSupported
	}

	readFile := &seaweedFile{
		fs:          fs,
		virtualPath: info.virtualPath,
		info:        info,
	}
	content := make([]byte, fileSize)
	_, err := readFile.ReadAt(content, 0)
	if err != nil && err != io.EOF {
		return nil, err
	}
	return content, nil
}

func (fs *seaweedFileSystem) saveDataAsChunk(actualPath util.FullPath, content []byte) (*filer_pb.FileChunk, error) {
	uploader, err := fs.server.newUploader()
	if err != nil {
		return nil, fmt.Errorf("upload data: %w", err)
	}

	filename := actualPath.Name()
	fileID, uploadResult, uploadErr, _ := uploader.UploadWithRetry(
		fs,
		&filer_pb.AssignVolumeRequest{
			Count:      1,
			DataCenter: fs.GetDataCenter(),
			Path:       string(actualPath),
		},
		&operation.UploadOption{
			Filename:          filename,
			Cipher:            false,
			IsInputCompressed: false,
			MimeType:          "",
			PairMap:           nil,
		},
		func(host, fileID string) string {
			fileURL := fmt.Sprintf("http://%s/%s", host, fileID)
			if fs.server.option.VolumeServerAccess == "filerProxy" {
				fileURL = fmt.Sprintf("http://%s/?proxyChunkId=%s", fs.server.option.Filer.ToHttpAddress(), fileID)
			}
			return fileURL
		},
		util.NewBytesReader(content),
	)
	if uploadErr != nil {
		return nil, fmt.Errorf("upload data: %w", uploadErr)
	}
	if uploadResult == nil {
		return nil, errors.New("upload data: missing upload result")
	}
	if uploadResult.Error != "" {
		return nil, fmt.Errorf("upload result: %s", uploadResult.Error)
	}
	return uploadResult.ToPbFileChunk(fileID, 0, time.Now().UnixNano()), nil
}

func (fs *seaweedFileSystem) persistContent(ctx context.Context, actualPath util.FullPath, content []byte) (*filer_pb.Entry, error) {
	if len(content) > maxInlineWriteSize {
		chunk, err := fs.saveDataAsChunk(actualPath, content)
		if err != nil {
			return nil, err
		}
		return fs.mutateEntry(ctx, actualPath, func(entry *filer_pb.Entry) {
			entry.Content = nil
			entry.Chunks = []*filer_pb.FileChunk{chunk}
			entry.RemoteEntry = nil
			entry.Attributes.FileSize = uint64(len(content))
			touchEntryTimes(entry, true)
		})
	}

	return fs.mutateEntry(ctx, actualPath, func(entry *filer_pb.Entry) {
		entry.Content = bytes.Clone(content)
		entry.Chunks = nil
		entry.RemoteEntry = nil
		entry.Attributes.FileSize = uint64(len(content))
		touchEntryTimes(entry, true)
	})
}

func (fs *seaweedFileSystem) fileInfoForVirtualPath(ctx context.Context, name string) (*seaweedFileInfo, error) {
	virtualPath, actualPath := fs.resolvePath(name)

	entry, err := fs.lookupEntry(ctx, actualPath)
	if err != nil {
		return nil, err
	}
	return fs.materializeFileInfo(ctx, virtualPath, actualPath, entry)
}

func (fs *seaweedFileSystem) materializeFileInfo(ctx context.Context, virtualPath string, actualPath util.FullPath, entry *filer_pb.Entry) (*seaweedFileInfo, error) {
	entry, generation, err := fs.ensureIndexedEntry(ctx, actualPath, entry)
	if err != nil {
		return nil, err
	}

	fileID := entry.Attributes.GetInode()
	if fileID == 0 && actualPath == fs.server.exportRoot && entry.IsDirectory {
		fileID = uint64(fs.server.exportID)
	}

	return &seaweedFileInfo{
		name:        fileInfoName(virtualPath, entry),
		virtualPath: virtualPath,
		size:        int64(filer.FileSize(entry)),
		mode:        fileModeForEntry(entry),
		modTime:     entryModTime(entry),
		actualPath:  actualPath,
		entry:       entry,
		generation:  generation,
		fileID:      fileID,
		nlink:       entryLinkCount(entry),
	}, nil
}

func (fs *seaweedFileSystem) lookupEntry(ctx context.Context, actualPath util.FullPath) (*filer_pb.Entry, error) {
	var entry *filer_pb.Entry
	err := fs.server.withInternalClient(false, func(client nfsFilerClient) error {
		dir, name := actualPath.DirAndName()
		resp, err := client.LookupDirectoryEntry(ctx, &filer_pb.LookupDirectoryEntryRequest{
			Directory: dir,
			Name:      name,
		})
		if err != nil {
			return err
		}
		if resp == nil || resp.Entry == nil {
			return filer_pb.ErrNotFound
		}
		entry = resp.Entry
		return nil
	})
	if err == nil {
		return entry, nil
	}
	if isLookupNotFound(err) {
		if actualPath == "/" {
			return syntheticRootEntry(), nil
		}
		return nil, os.ErrNotExist
	}
	return nil, err
}

func (fs *seaweedFileSystem) ensureIndexedEntry(ctx context.Context, actualPath util.FullPath, entry *filer_pb.Entry) (*filer_pb.Entry, uint64, error) {
	if entry == nil {
		return nil, 0, os.ErrNotExist
	}
	if entry.Attributes == nil {
		entry.Attributes = &filer_pb.FuseAttributes{}
	}

	if entry.Attributes.Inode == 0 && !(actualPath == "/" && entry.Name == "/" && entry.IsDirectory) {
		updatedEntry, err := fs.backfillLegacyInode(ctx, actualPath, entry)
		if err != nil {
			return nil, 0, err
		}
		entry = updatedEntry
	}

	if entry.Attributes.GetInode() == 0 {
		if actualPath == "/" && entry.Name == "/" && entry.IsDirectory {
			return entry, filer.InodeIndexInitialGeneration, nil
		}
		return nil, 0, fmt.Errorf("nfs requires inode-backed entry for %s", actualPath)
	}

	generation, err := fs.lookupGeneration(ctx, entry.Attributes.GetInode())
	if err != nil {
		return nil, 0, err
	}
	return entry, generation, nil
}

func (fs *seaweedFileSystem) backfillLegacyInode(ctx context.Context, actualPath util.FullPath, entry *filer_pb.Entry) (*filer_pb.Entry, error) {
	dir, _ := actualPath.DirAndName()
	clonedEntry, ok := proto.Clone(entry).(*filer_pb.Entry)
	if !ok {
		return nil, errors.New("clone filer entry")
	}

	var updatedEntry *filer_pb.Entry
	err := fs.server.withInternalClient(false, func(client nfsFilerClient) error {
		resp, err := client.UpdateEntry(ctx, &filer_pb.UpdateEntryRequest{
			Directory: dir,
			Entry:     clonedEntry,
		})
		if err != nil {
			return err
		}
		if resp != nil && resp.MetadataEvent != nil && resp.MetadataEvent.EventNotification != nil && resp.MetadataEvent.EventNotification.NewEntry != nil {
			updatedEntry = resp.MetadataEvent.EventNotification.NewEntry
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if updatedEntry != nil {
		return updatedEntry, nil
	}
	return fs.lookupEntry(ctx, actualPath)
}

func (fs *seaweedFileSystem) lookupGeneration(ctx context.Context, inode uint64) (uint64, error) {
	var resp *filer_pb.KvGetResponse
	err := fs.server.withInternalClient(false, func(client nfsFilerClient) error {
		var kvErr error
		resp, kvErr = client.KvGet(ctx, &filer_pb.KvGetRequest{Key: filer.InodeIndexKey(inode)})
		return kvErr
	})
	if err != nil {
		return 0, err
	}
	if resp == nil {
		return 0, ErrStaleHandle
	}
	if resp.GetError() != "" {
		return 0, errors.New(resp.GetError())
	}
	if len(resp.GetValue()) == 0 {
		return 0, ErrStaleHandle
	}

	record, err := filer.DecodeInodeIndexRecord(resp.GetValue())
	if err != nil {
		return 0, err
	}
	if record.Generation == 0 {
		return filer.InodeIndexInitialGeneration, nil
	}
	return record.Generation, nil
}

func fileInfoName(virtualPath string, entry *filer_pb.Entry) string {
	if entry != nil && entry.Name != "" {
		return entry.Name
	}
	if virtualPath == "/" {
		return "/"
	}
	return path.Base(virtualPath)
}

func fileModeForEntry(entry *filer_pb.Entry) os.FileMode {
	mode := os.FileMode(0)
	if entry != nil && entry.Attributes != nil {
		mode = os.FileMode(entry.Attributes.FileMode)
	}
	if entry != nil && entry.IsDirectory {
		mode |= os.ModeDir
	}
	if entry != nil && entry.Attributes != nil && entry.Attributes.SymlinkTarget != "" {
		mode |= os.ModeSymlink
	}
	return mode
}

func entryModTime(entry *filer_pb.Entry) time.Time {
	if entry == nil || entry.Attributes == nil {
		return time.Unix(0, 0)
	}
	seconds := entry.Attributes.Mtime
	nanos := int64(entry.Attributes.MtimeNs)
	if seconds == 0 && nanos == 0 {
		seconds = entry.Attributes.Crtime
	}
	return time.Unix(seconds, nanos)
}

func entryLinkCount(entry *filer_pb.Entry) uint32 {
	if entry == nil {
		return 1
	}
	if entry.HardLinkCounter > 0 {
		return uint32(entry.HardLinkCounter)
	}
	return 1
}

func touchEntryTimes(entry *filer_pb.Entry, updateMtime bool) {
	if entry == nil {
		return
	}
	if entry.Attributes == nil {
		entry.Attributes = &filer_pb.FuseAttributes{}
	}
	now := time.Now()
	if updateMtime {
		entry.Attributes.Mtime = now.Unix()
		entry.Attributes.MtimeNs = int32(now.Nanosecond())
	}
	entry.Attributes.Ctime = now.Unix()
	entry.Attributes.CtimeNs = int32(now.Nanosecond())
	if entry.Attributes.Crtime == 0 {
		entry.Attributes.Crtime = now.Unix()
	}
}

func cleanBillyPath(name string) string {
	if name == "" || name == "." {
		return "/"
	}
	cleaned := path.Clean(name)
	if cleaned == "." {
		return "/"
	}
	if !strings.HasPrefix(cleaned, "/") {
		cleaned = "/" + cleaned
	}
	return cleaned
}

func syntheticRootEntry() *filer_pb.Entry {
	return &filer_pb.Entry{
		Name:        "/",
		IsDirectory: true,
		Attributes: &filer_pb.FuseAttributes{
			FileMode: uint32(os.ModeDir | 0755),
		},
	}
}

func (fi *seaweedFileInfo) Name() string       { return fi.name }
func (fi *seaweedFileInfo) Size() int64        { return fi.size }
func (fi *seaweedFileInfo) Mode() os.FileMode  { return fi.mode }
func (fi *seaweedFileInfo) ModTime() time.Time { return fi.modTime }
func (fi *seaweedFileInfo) IsDir() bool        { return fi.mode.IsDir() }
func (fi *seaweedFileInfo) Sys() interface{} {
	return &gonfsfile.FileInfo{
		Nlink:  fi.nlink,
		UID:    fi.entry.GetAttributes().GetUid(),
		GID:    fi.entry.GetAttributes().GetGid(),
		Fileid: fi.fileID,
	}
}

func (f *seaweedFile) Name() string { return f.virtualPath }

func (f *seaweedFile) Read(p []byte) (int, error) {
	if f.writable {
		if f.offset >= int64(len(f.content)) {
			return 0, io.EOF
		}
		n := copy(p, f.content[f.offset:])
		f.offset += int64(n)
		if n < len(p) {
			return n, io.EOF
		}
		return n, nil
	}
	n, err := f.ReadAt(p, f.offset)
	f.offset += int64(n)
	return n, err
}

func (f *seaweedFile) ReadAt(p []byte, off int64) (int, error) {
	if f.writable {
		return bytes.NewReader(f.content).ReadAt(p, off)
	}
	if len(f.info.entry.Content) > 0 {
		reader := bytes.NewReader(f.info.entry.Content)
		return reader.ReadAt(p, off)
	}

	fileSize := int64(filer.FileSize(f.info.entry))
	if fileSize == 0 || off >= fileSize {
		return 0, io.EOF
	}
	if f.reader == nil {
		visibleIntervals, err := filer.NonOverlappingVisibleIntervals(context.Background(), filer.LookupFn(f.fs), f.info.entry.GetChunks(), 0, fileSize)
		if err != nil {
			return 0, err
		}
		chunkViews := filer.ViewFromVisibleIntervals(visibleIntervals, 0, fileSize)
		f.reader = filer.NewChunkReaderAtFromClient(context.Background(), f.fs.readerCache, chunkViews, fileSize, filer.DefaultPrefetchCount)
	}
	return f.reader.ReadAt(p, off)
}

func (f *seaweedFile) Write(p []byte) (int, error) {
	if !f.writable {
		return 0, billy.ErrReadOnly
	}
	if f.closed {
		return 0, os.ErrClosed
	}

	endOffset := int(f.offset) + len(p)
	if endOffset > maxBufferedWriteSize {
		return 0, billy.ErrNotSupported
	}
	if endOffset > len(f.content) {
		grown := make([]byte, endOffset)
		copy(grown, f.content)
		f.content = grown
	}
	copy(f.content[f.offset:], p)
	f.offset = int64(endOffset)
	f.dirty = true
	return len(p), nil
}

func (f *seaweedFile) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		f.offset = offset
	case io.SeekCurrent:
		f.offset += offset
	case io.SeekEnd:
		if f.writable {
			f.offset = int64(len(f.content)) + offset
		} else {
			f.offset = f.info.size + offset
		}
	default:
		return 0, fmt.Errorf("invalid whence %d", whence)
	}
	if f.offset < 0 {
		f.offset = 0
	}
	return f.offset, nil
}

func (f *seaweedFile) Close() error {
	if f.closed {
		return nil
	}
	f.closed = true
	if !f.writable || !f.dirty {
		return nil
	}

	updatedEntry, err := f.fs.persistContent(context.Background(), f.info.actualPath, f.content)
	if err != nil {
		return err
	}
	updatedInfo, err := f.fs.materializeFileInfo(context.Background(), f.virtualPath, f.info.actualPath, updatedEntry)
	if err != nil {
		return err
	}
	f.info = updatedInfo
	f.dirty = false
	return nil
}
func (f *seaweedFile) Lock() error { return nil }
func (f *seaweedFile) Unlock() error {
	return nil
}

func (f *seaweedFile) Truncate(size int64) error {
	if !f.writable {
		return billy.ErrReadOnly
	}
	if size < 0 || size > maxBufferedWriteSize {
		return billy.ErrNotSupported
	}
	target := int(size)
	if target <= len(f.content) {
		f.content = f.content[:target]
	} else {
		grown := make([]byte, target)
		copy(grown, f.content)
		f.content = grown
	}
	if f.offset > size {
		f.offset = size
	}
	f.dirty = true
	return nil
}
