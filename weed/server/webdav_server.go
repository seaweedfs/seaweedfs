package weed_server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"

	"github.com/joeslay/seaweedfs/weed/operation"
	"github.com/joeslay/seaweedfs/weed/pb/filer_pb"
	"github.com/joeslay/seaweedfs/weed/util"
	"golang.org/x/net/webdav"
	"google.golang.org/grpc"

	"github.com/joeslay/seaweedfs/weed/filer2"
	"github.com/joeslay/seaweedfs/weed/glog"
	"github.com/joeslay/seaweedfs/weed/security"
	"github.com/spf13/viper"
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
}

type WebDavServer struct {
	option         *WebDavOption
	secret         security.SigningKey
	filer          *filer2.Filer
	grpcDialOption grpc.DialOption
	Handler        *webdav.Handler
}

func NewWebDavServer(option *WebDavOption) (ws *WebDavServer, err error) {

	fs, _ := NewWebDavFileSystem(option)

	ws = &WebDavServer{
		option:         option,
		grpcDialOption: security.LoadClientTLS(viper.Sub("grpc"), "filer"),
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
	filer          *filer2.Filer
	grpcDialOption grpc.DialOption
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
	entryViewCache []filer2.VisibleInterval
}

func NewWebDavFileSystem(option *WebDavOption) (webdav.FileSystem, error) {
	return &WebDavFileSystem{
		option: option,
	}, nil
}

func (fs *WebDavFileSystem) WithFilerClient(ctx context.Context, fn func(filer_pb.SeaweedFilerClient) error) error {

	return util.WithCachedGrpcClient(ctx, func(grpcConnection *grpc.ClientConn) error {
		client := filer_pb.NewSeaweedFilerClient(grpcConnection)
		return fn(client)
	}, fs.option.FilerGrpcAddress, fs.option.GrpcDialOption)

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

	return fs.WithFilerClient(ctx, func(client filer_pb.SeaweedFilerClient) error {
		dir, name := filer2.FullPath(fullDirPath).DirAndName()
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
		}

		glog.V(1).Infof("mkdir: %v", request)
		if _, err := client.CreateEntry(ctx, request); err != nil {
			return fmt.Errorf("mkdir %s/%s: %v", dir, name, err)
		}

		return nil
	})
}

func (fs *WebDavFileSystem) OpenFile(ctx context.Context, fullFilePath string, flag int, perm os.FileMode) (webdav.File, error) {

	glog.V(2).Infof("WebDavFileSystem.OpenFile %v", fullFilePath)

	var err error
	if fullFilePath, err = clearName(fullFilePath); err != nil {
		return nil, err
	}

	if flag&os.O_CREATE != 0 {
		// file should not have / suffix.
		if strings.HasSuffix(fullFilePath, "/") {
			return nil, os.ErrInvalid
		}
		// based directory should be exists.
		dir, _ := path.Split(fullFilePath)
		_, err := fs.stat(ctx, dir)
		if err != nil {
			return nil, os.ErrInvalid
		}
		_, err = fs.stat(ctx, fullFilePath)
		if err == nil {
			if flag&os.O_EXCL != 0 {
				return nil, os.ErrExist
			}
			fs.removeAll(ctx, fullFilePath)
		}

		dir, name := filer2.FullPath(fullFilePath).DirAndName()
		err = fs.WithFilerClient(ctx, func(client filer_pb.SeaweedFilerClient) error {
			if _, err := client.CreateEntry(ctx, &filer_pb.CreateEntryRequest{
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

	fi, err := fs.stat(ctx, fullFilePath)
	if err != nil {
		return err
	}

	if fi.IsDir() {
		//_, err = fs.db.Exec(`delete from filesystem where fullFilePath like $1 escape '\'`, strings.Replace(fullFilePath, `%`, `\%`, -1)+`%`)
	} else {
		//_, err = fs.db.Exec(`delete from filesystem where fullFilePath = ?`, fullFilePath)
	}
	dir, name := filer2.FullPath(fullFilePath).DirAndName()
	err = fs.WithFilerClient(ctx, func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.DeleteEntryRequest{
			Directory:    dir,
			Name:         name,
			IsDeleteData: true,
		}

		glog.V(3).Infof("removing entry: %v", request)
		_, err := client.DeleteEntry(ctx, request)
		if err != nil {
			return fmt.Errorf("remove %s: %v", fullFilePath, err)
		}

		return nil
	})
	return err
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

	oldDir, oldBaseName := filer2.FullPath(oldName).DirAndName()
	newDir, newBaseName := filer2.FullPath(newName).DirAndName()

	return fs.WithFilerClient(ctx, func(client filer_pb.SeaweedFilerClient) error {

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

	var fi FileInfo
	entry, err := filer2.GetEntry(ctx, fs, fullFilePath)
	if entry == nil {
		return nil, os.ErrNotExist
	}
	if err != nil {
		return nil, err
	}
	fi.size = int64(filer2.TotalSize(entry.GetChunks()))
	fi.name = fullFilePath
	fi.mode = os.FileMode(entry.Attributes.FileMode)
	fi.modifiledTime = time.Unix(entry.Attributes.Mtime, 0)
	fi.isDirectory = entry.IsDirectory

	_, fi.name = path.Split(path.Clean(fi.name))
	if fi.name == "" {
		fi.name = "/"
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

	var err error
	ctx := context.Background()
	if f.entry == nil {
		f.entry, err = filer2.GetEntry(ctx, f.fs, f.name)
	}

	if f.entry == nil {
		return 0, err
	}
	if err != nil {
		return 0, err
	}

	var fileId, host string
	var auth security.EncodedJwt

	if err = f.fs.WithFilerClient(ctx, func(client filer_pb.SeaweedFilerClient) error {

		request := &filer_pb.AssignVolumeRequest{
			Count:       1,
			Replication: "000",
			Collection:  f.fs.option.Collection,
		}

		resp, err := client.AssignVolume(ctx, request)
		if err != nil {
			glog.V(0).Infof("assign volume failure %v: %v", request, err)
			return err
		}

		fileId, host, auth = resp.FileId, resp.Url, security.EncodedJwt(resp.Auth)

		return nil
	}); err != nil {
		return 0, fmt.Errorf("filerGrpcAddress assign volume: %v", err)
	}

	fileUrl := fmt.Sprintf("http://%s/%s", host, fileId)
	bufReader := bytes.NewReader(buf)
	uploadResult, err := operation.Upload(fileUrl, f.name, bufReader, false, "application/octet-stream", nil, auth)
	if err != nil {
		glog.V(0).Infof("upload data %v to %s: %v", f.name, fileUrl, err)
		return 0, fmt.Errorf("upload data: %v", err)
	}
	if uploadResult.Error != "" {
		glog.V(0).Infof("upload failure %v to %s: %v", f.name, fileUrl, err)
		return 0, fmt.Errorf("upload result: %v", uploadResult.Error)
	}

	chunk := &filer_pb.FileChunk{
		FileId: fileId,
		Offset: f.off,
		Size:   uint64(len(buf)),
		Mtime:  time.Now().UnixNano(),
		ETag:   uploadResult.ETag,
	}

	f.entry.Chunks = append(f.entry.Chunks, chunk)
	dir, _ := filer2.FullPath(f.name).DirAndName()

	err = f.fs.WithFilerClient(ctx, func(client filer_pb.SeaweedFilerClient) error {
		f.entry.Attributes.Mtime = time.Now().Unix()

		request := &filer_pb.UpdateEntryRequest{
			Directory: dir,
			Entry:     f.entry,
		}

		if _, err := client.UpdateEntry(ctx, request); err != nil {
			return fmt.Errorf("update %s: %v", f.name, err)
		}

		return nil
	})

	if err != nil {
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
	ctx := context.Background()

	if f.entry == nil {
		f.entry, err = filer2.GetEntry(ctx, f.fs, f.name)
	}
	if f.entry == nil {
		return 0, err
	}
	if err != nil {
		return 0, err
	}
	if len(f.entry.Chunks) == 0 {
		return 0, io.EOF
	}
	if f.entryViewCache == nil {
		f.entryViewCache = filer2.NonOverlappingVisibleIntervals(f.entry.Chunks)
	}
	chunkViews := filer2.ViewFromVisibleIntervals(f.entryViewCache, f.off, len(p))

	totalRead, err := filer2.ReadIntoBuffer(ctx, f.fs, f.name, p, chunkViews, f.off)
	if err != nil {
		return 0, err
	}
	readSize = int(totalRead)

	f.off += totalRead
	if readSize == 0 {
		return 0, io.EOF
	}
	return
}

func (f *WebDavFile) Readdir(count int) (ret []os.FileInfo, err error) {

	glog.V(2).Infof("WebDavFileSystem.Readdir %v count %d", f.name, count)
	ctx := context.Background()

	dir := f.name
	if dir != "/" && strings.HasSuffix(dir, "/") {
		dir = dir[:len(dir)-1]
	}

	err = filer2.ReadDirAllEntries(ctx, f.fs, dir, func(entry *filer_pb.Entry) {
		fi := FileInfo{
			size:          int64(filer2.TotalSize(entry.GetChunks())),
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
	case 0:
		f.off = 0
	case 2:
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
