package weed_server

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"path"
	"strings"
	"time"

	"golang.org/x/net/webdav"
	"google.golang.org/grpc"

	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/chrislusf/seaweedfs/weed/util/chunk_cache"

	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/security"
)

type WebDavOption struct {
	Filer            string
	FilerGrpcAddress string
	DomainName       string
	BucketsPath      string
	GrpcDialOption   grpc.DialOption
	Collection       string
	Uid              uint32
	Gid              uint32
	Cipher           bool
	CacheDir         string
	CacheSizeMB      int64
}

type WebDavServer struct {
	option         *WebDavOption
	secret         security.SigningKey
	filer          *filer.Filer
	grpcDialOption grpc.DialOption
	Handler        *webdav.Handler
}

func NewWebDavServer(option *WebDavOption) (ws *WebDavServer, err error) {

	fs, _ := NewWebDavFileSystem(option)

	ws = &WebDavServer{
		option:         option,
		grpcDialOption: security.LoadClientTLS(util.GetViper(), "grpc.filer"),
		Handler: &webdav.Handler{
			FileSystem: fs,
			LockSystem: webdav.NewMemLS(),
		},
	}

	return ws, nil
}

// adapted from https://github.com/mattn/davfs/blob/master/plugin/mysql/mysql.go

type WebDavFileSystem struct {
	option         *WebDavOption
	secret         security.SigningKey
	filer          *filer.Filer
	grpcDialOption grpc.DialOption
	chunkCache     *chunk_cache.TieredChunkCache
	signature      int32
}

type FileInfo struct {
	name          string
	size          int64
	mode          os.FileMode
	modifiledTime time.Time
	isDirectory   bool
}

func (fi *FileInfo) Name() string       { return fi.name }
func (fi *FileInfo) Size() int64        { return fi.size }
func (fi *FileInfo) Mode() os.FileMode  { return fi.mode }
func (fi *FileInfo) ModTime() time.Time { return fi.modifiledTime }
func (fi *FileInfo) IsDir() bool        { return fi.isDirectory }
func (fi *FileInfo) Sys() interface{}   { return nil }

type WebDavFile struct {
	fs             *WebDavFileSystem
	name           string
	isDirectory    bool
	off            int64
	entry          *filer_pb.Entry
	entryViewCache []filer.VisibleInterval
	reader         io.ReaderAt
}

func NewWebDavFileSystem(option *WebDavOption) (webdav.FileSystem, error) {

	chunkCache := chunk_cache.NewTieredChunkCache(256, option.CacheDir, option.CacheSizeMB, 1024*1024)
	return &WebDavFileSystem{
		option:     option,
		chunkCache: chunkCache,
		signature:  util.RandomInt32(),
	}, nil
}

var _ = filer_pb.FilerClient(&WebDavFileSystem{})

func (fs *WebDavFileSystem) WithFilerClient(fn func(filer_pb.SeaweedFilerClient) error) error {

	return pb.WithCachedGrpcClient(func(grpcConnection *grpc.ClientConn) error {
		client := filer_pb.NewSeaweedFilerClient(grpcConnection)
		return fn(client)
	}, fs.option.FilerGrpcAddress, fs.option.GrpcDialOption)

}
func (fs *WebDavFileSystem) AdjustedUrl(location *filer_pb.Location) string {
	return location.Url
}

func clearName(name string) (string, error) {
	slashed := strings.HasSuffix(name, "/")
	name = path.Clean(name)
	if !strings.HasSuffix(name, "/") && slashed {
		name += "/"
	}
	if !strings.HasPrefix(name, "/") {
		return "", os.ErrInvalid
	}
	return name, nil
}

func (fs *WebDavFileSystem) Mkdir(ctx context.Context, fullDirPath string, perm os.FileMode) error {

	glog.V(2).Infof("WebDavFileSystem.Mkdir %v", fullDirPath)

	if !strings.HasSuffix(fullDirPath, "/") {
		fullDirPath += "/"
	}

	var err error
	if fullDirPath, err = clearName(fullDirPath); err != nil {
		return err
	}

	_, err = fs.stat(ctx, fullDirPath)
	if err == nil {
		return os.ErrExist
	}

	return fs.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		dir, name := util.FullPath(fullDirPath).DirAndName()
		request := &filer_pb.CreateEntryRequest{
			Directory: dir,
			Entry: &filer_pb.Entry{
				Name:        name,
				IsDirectory: true,
				Attributes: &filer_pb.FuseAttributes{
					Mtime:    time.Now().Unix(),
					Crtime:   time.Now().Unix(),
					FileMode: uint32(perm | os.ModeDir),
					Uid:      fs.option.Uid,
					Gid:      fs.option.Gid,
				},
			},
			Signatures: []int32{fs.signature},
		}

		glog.V(1).Infof("mkdir: %v", request)
		if err := filer_pb.CreateEntry(client, request); err != nil {
			return fmt.Errorf("mkdir %s/%s: %v", dir, name, err)
		}

		return nil
	})
}

func (fs *WebDavFileSystem) OpenFile(ctx context.Context, fullFilePath string, flag int, perm os.FileMode) (webdav.File, error) {

	glog.V(2).Infof("WebDavFileSystem.OpenFile %v %x", fullFilePath, flag)

	var err error
	if fullFilePath, err = clearName(fullFilePath); err != nil {
		return nil, err
	}

	if flag&os.O_CREATE != 0 {
		// file should not have / suffix.
		if strings.HasSuffix(fullFilePath, "/") {
			return nil, os.ErrInvalid
		}
		_, err = fs.stat(ctx, fullFilePath)
		if err == nil {
			if flag&os.O_EXCL != 0 {
				return nil, os.ErrExist
			}
			fs.removeAll(ctx, fullFilePath)
		}

		dir, name := util.FullPath(fullFilePath).DirAndName()
		err = fs.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
			if err := filer_pb.CreateEntry(client, &filer_pb.CreateEntryRequest{
				Directory: dir,
				Entry: &filer_pb.Entry{
					Name:        name,
					IsDirectory: perm&os.ModeDir > 0,
					Attributes: &filer_pb.FuseAttributes{
						Mtime:       time.Now().Unix(),
						Crtime:      time.Now().Unix(),
						FileMode:    uint32(perm),
						Uid:         fs.option.Uid,
						Gid:         fs.option.Gid,
						Collection:  fs.option.Collection,
						Replication: "000",
						TtlSec:      0,
					},
				},
				Signatures: []int32{fs.signature},
			}); err != nil {
				return fmt.Errorf("create %s: %v", fullFilePath, err)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		return &WebDavFile{
			fs:          fs,
			name:        fullFilePath,
			isDirectory: false,
		}, nil
	}

	fi, err := fs.stat(ctx, fullFilePath)
	if err != nil {
		return nil, os.ErrNotExist
	}
	if !strings.HasSuffix(fullFilePath, "/") && fi.IsDir() {
		fullFilePath += "/"
	}

	return &WebDavFile{
		fs:          fs,
		name:        fullFilePath,
		isDirectory: false,
	}, nil

}

func (fs *WebDavFileSystem) removeAll(ctx context.Context, fullFilePath string) error {
	var err error
	if fullFilePath, err = clearName(fullFilePath); err != nil {
		return err
	}

	dir, name := util.FullPath(fullFilePath).DirAndName()

	return filer_pb.Remove(fs, dir, name, true, false, false, false, []int32{fs.signature})

}

func (fs *WebDavFileSystem) RemoveAll(ctx context.Context, name string) error {

	glog.V(2).Infof("WebDavFileSystem.RemoveAll %v", name)

	return fs.removeAll(ctx, name)
}

func (fs *WebDavFileSystem) Rename(ctx context.Context, oldName, newName string) error {

	glog.V(2).Infof("WebDavFileSystem.Rename %v to %v", oldName, newName)

	var err error
	if oldName, err = clearName(oldName); err != nil {
		return err
	}
	if newName, err = clearName(newName); err != nil {
		return err
	}

	of, err := fs.stat(ctx, oldName)
	if err != nil {
		return os.ErrExist
	}
	if of.IsDir() {
		if strings.HasSuffix(oldName, "/") {
			oldName = strings.TrimRight(oldName, "/")
		}
		if strings.HasSuffix(newName, "/") {
			newName = strings.TrimRight(newName, "/")
		}
	}

	_, err = fs.stat(ctx, newName)
	if err == nil {
		return os.ErrExist
	}

	oldDir, oldBaseName := util.FullPath(oldName).DirAndName()
	newDir, newBaseName := util.FullPath(newName).DirAndName()

	return fs.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.AtomicRenameEntryRequest{
			OldDirectory: oldDir,
			OldName:      oldBaseName,
			NewDirectory: newDir,
			NewName:      newBaseName,
		}

		_, err := client.AtomicRenameEntry(ctx, request)
		if err != nil {
			return fmt.Errorf("renaming %s/%s => %s/%s: %v", oldDir, oldBaseName, newDir, newBaseName, err)
		}

		return nil

	})
}

func (fs *WebDavFileSystem) stat(ctx context.Context, fullFilePath string) (os.FileInfo, error) {
	var err error
	if fullFilePath, err = clearName(fullFilePath); err != nil {
		return nil, err
	}

	fullpath := util.FullPath(fullFilePath)

	var fi FileInfo
	entry, err := filer_pb.GetEntry(fs, fullpath)
	if entry == nil {
		return nil, os.ErrNotExist
	}
	if err != nil {
		return nil, err
	}
	fi.size = int64(filer.FileSize(entry))
	fi.name = string(fullpath)
	fi.mode = os.FileMode(entry.Attributes.FileMode)
	fi.modifiledTime = time.Unix(entry.Attributes.Mtime, 0)
	fi.isDirectory = entry.IsDirectory

	if fi.name == "/" {
		fi.modifiledTime = time.Now()
		fi.isDirectory = true
	}
	return &fi, nil
}

func (fs *WebDavFileSystem) Stat(ctx context.Context, name string) (os.FileInfo, error) {

	glog.V(2).Infof("WebDavFileSystem.Stat %v", name)

	return fs.stat(ctx, name)
}

func (f *WebDavFile) Write(buf []byte) (int, error) {

	glog.V(2).Infof("WebDavFileSystem.Write %v", f.name)

	dir, _ := util.FullPath(f.name).DirAndName()

	var err error
	ctx := context.Background()
	if f.entry == nil {
		f.entry, err = filer_pb.GetEntry(f.fs, util.FullPath(f.name))
	}

	if f.entry == nil {
		return 0, err
	}
	if err != nil {
		return 0, err
	}

	var fileId, host string
	var auth security.EncodedJwt
	var collection, replication string

	if err = f.fs.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.AssignVolumeRequest{
			Count:       1,
			Replication: "",
			Collection:  f.fs.option.Collection,
			Path:        f.name,
		}

		resp, err := client.AssignVolume(ctx, request)
		if err != nil {
			glog.V(0).Infof("assign volume failure %v: %v", request, err)
			return err
		}
		if resp.Error != "" {
			return fmt.Errorf("assign volume failure %v: %v", request, resp.Error)
		}

		fileId, host, auth = resp.FileId, resp.Url, security.EncodedJwt(resp.Auth)
		collection, replication = resp.Collection, resp.Replication

		return nil
	}); err != nil {
		return 0, fmt.Errorf("filerGrpcAddress assign volume: %v", err)
	}

	fileUrl := fmt.Sprintf("http://%s/%s", host, fileId)
	uploadResult, err := operation.UploadData(fileUrl, f.name, f.fs.option.Cipher, buf, false, "", nil, auth)
	if err != nil {
		glog.V(0).Infof("upload data %v to %s: %v", f.name, fileUrl, err)
		return 0, fmt.Errorf("upload data: %v", err)
	}
	if uploadResult.Error != "" {
		glog.V(0).Infof("upload failure %v to %s: %v", f.name, fileUrl, err)
		return 0, fmt.Errorf("upload result: %v", uploadResult.Error)
	}

	f.entry.Content = nil
	f.entry.Chunks = append(f.entry.Chunks, uploadResult.ToPbFileChunk(fileId, f.off))

	err = f.fs.WithFilerClient(func(client filer_pb.SeaweedFilerClient) error {
		f.entry.Attributes.Mtime = time.Now().Unix()
		f.entry.Attributes.Collection = collection
		f.entry.Attributes.Replication = replication

		request := &filer_pb.UpdateEntryRequest{
			Directory:  dir,
			Entry:      f.entry,
			Signatures: []int32{f.fs.signature},
		}

		if _, err := client.UpdateEntry(ctx, request); err != nil {
			return fmt.Errorf("update %s: %v", f.name, err)
		}

		return nil
	})

	if err == nil {
		glog.V(3).Infof("WebDavFileSystem.Write %v: written [%d,%d)", f.name, f.off, f.off+int64(len(buf)))
		f.off += int64(len(buf))
	}

	return len(buf), err
}

func (f *WebDavFile) Close() error {

	glog.V(2).Infof("WebDavFileSystem.Close %v", f.name)

	if f.entry != nil {
		f.entry = nil
		f.entryViewCache = nil
	}

	return nil
}

func (f *WebDavFile) Read(p []byte) (readSize int, err error) {

	glog.V(2).Infof("WebDavFileSystem.Read %v", f.name)

	if f.entry == nil {
		f.entry, err = filer_pb.GetEntry(f.fs, util.FullPath(f.name))
	}
	if f.entry == nil {
		return 0, err
	}
	if err != nil {
		return 0, err
	}
	fileSize := int64(filer.FileSize(f.entry))
	if fileSize == 0 {
		return 0, io.EOF
	}
	if f.entryViewCache == nil {
		f.entryViewCache, _ = filer.NonOverlappingVisibleIntervals(filer.LookupFn(f.fs), f.entry.Chunks)
		f.reader = nil
	}
	if f.reader == nil {
		chunkViews := filer.ViewFromVisibleIntervals(f.entryViewCache, 0, math.MaxInt64)
		f.reader = filer.NewChunkReaderAtFromClient(f.fs, chunkViews, f.fs.chunkCache, fileSize)
	}

	readSize, err = f.reader.ReadAt(p, f.off)

	glog.V(3).Infof("WebDavFileSystem.Read %v: [%d,%d)", f.name, f.off, f.off+int64(readSize))
	f.off += int64(readSize)

	if err != nil && err != io.EOF {
		glog.Errorf("file read %s: %v", f.name, err)
	}

	return

}

func (f *WebDavFile) Readdir(count int) (ret []os.FileInfo, err error) {

	glog.V(2).Infof("WebDavFileSystem.Readdir %v count %d", f.name, count)

	dir, _ := util.FullPath(f.name).DirAndName()

	err = filer_pb.ReadDirAllEntries(f.fs, util.FullPath(dir), "", func(entry *filer_pb.Entry, isLast bool) error {
		fi := FileInfo{
			size:          int64(filer.FileSize(entry)),
			name:          entry.Name,
			mode:          os.FileMode(entry.Attributes.FileMode),
			modifiledTime: time.Unix(entry.Attributes.Mtime, 0),
			isDirectory:   entry.IsDirectory,
		}

		if !strings.HasSuffix(fi.name, "/") && fi.IsDir() {
			fi.name += "/"
		}
		glog.V(4).Infof("entry: %v", fi.name)
		ret = append(ret, &fi)
		return nil
	})

	old := f.off
	if old >= int64(len(ret)) {
		if count > 0 {
			return nil, io.EOF
		}
		return nil, nil
	}
	if count > 0 {
		f.off += int64(count)
		if f.off > int64(len(ret)) {
			f.off = int64(len(ret))
		}
	} else {
		f.off = int64(len(ret))
		old = 0
	}

	return ret[old:f.off], nil
}

func (f *WebDavFile) Seek(offset int64, whence int) (int64, error) {

	glog.V(2).Infof("WebDavFile.Seek %v %v %v", f.name, offset, whence)

	ctx := context.Background()

	var err error
	switch whence {
	case io.SeekStart:
		f.off = 0
	case io.SeekEnd:
		if fi, err := f.fs.stat(ctx, f.name); err != nil {
			return 0, err
		} else {
			f.off = fi.Size()
		}
	}
	f.off += offset
	return f.off, err
}

func (f *WebDavFile) Stat() (os.FileInfo, error) {

	glog.V(2).Infof("WebDavFile.Stat %v", f.name)

	ctx := context.Background()

	return f.fs.stat(ctx, f.name)
}
