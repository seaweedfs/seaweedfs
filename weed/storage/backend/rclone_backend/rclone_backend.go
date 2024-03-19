//go:build rclone
// +build rclone

package rclone_backend

import (
	"bytes"
	"context"
	"fmt"
	"github.com/rclone/rclone/fs/config/configfile"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"io"
	"os"
	"text/template"
	"time"

	"github.com/google/uuid"

	_ "github.com/rclone/rclone/backend/all"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/fs/object"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
)

func init() {
	backend.BackendStorageFactories["rclone"] = &RcloneBackendFactory{}
	configfile.Install()
}

type RcloneBackendFactory struct {
}

func (factory *RcloneBackendFactory) StorageType() backend.StorageType {
	return "rclone"
}

func (factory *RcloneBackendFactory) BuildStorage(configuration backend.StringProperties, configPrefix string, id string) (backend.BackendStorage, error) {
	return newRcloneBackendStorage(configuration, configPrefix, id)
}

type RcloneBackendStorage struct {
	id              string
	remoteName      string
	keyTemplate     *template.Template
	keyTemplateText string
	fs              fs.Fs
}

func newRcloneBackendStorage(configuration backend.StringProperties, configPrefix string, id string) (s *RcloneBackendStorage, err error) {
	s = &RcloneBackendStorage{}
	s.id = id
	s.remoteName = configuration.GetString(configPrefix + "remote_name")
	s.keyTemplateText = configuration.GetString(configPrefix + "key_template")
	s.keyTemplate, err = template.New("keyTemplate").Parse(s.keyTemplateText)
	if err != nil {
		return
	}

	ctx := context.TODO()
	accounting.Start(ctx)

	fsPath := fmt.Sprintf("%s:", s.remoteName)
	s.fs, err = fs.NewFs(ctx, fsPath)
	if err != nil {
		glog.Errorf("failed to instantiate Rclone filesystem: %s", err)
		return
	}

	glog.V(0).Infof("created backend storage rclone.%s for remote name %s", s.id, s.remoteName)
	return
}

func (s *RcloneBackendStorage) ToProperties() map[string]string {
	m := make(map[string]string)
	m["remote_name"] = s.remoteName
	if len(s.keyTemplateText) > 0 {
		m["key_template"] = s.keyTemplateText
	}
	return m
}

func formatKey(key string, storage RcloneBackendStorage) (fKey string, err error) {
	var b bytes.Buffer
	if len(storage.keyTemplateText) == 0 {
		fKey = key
	} else {
		err = storage.keyTemplate.Execute(&b, key)
		if err == nil {
			fKey = b.String()
		}
	}
	return
}

func (s *RcloneBackendStorage) NewStorageFile(key string, tierInfo *volume_server_pb.VolumeInfo) backend.BackendStorageFile {
	f := &RcloneBackendStorageFile{
		backendStorage: s,
		key:            key,
		tierInfo:       tierInfo,
	}

	return f
}

func (s *RcloneBackendStorage) CopyFile(f *os.File, fn func(progressed int64, percentage float32) error) (key string, size int64, err error) {
	randomUuid, err := uuid.NewRandom()
	if err != nil {
		return key, 0, err
	}
	key = randomUuid.String()

	key, err = formatKey(key, *s)
	if err != nil {
		return key, 0, err
	}

	glog.V(1).Infof("copy dat file of %s to remote rclone.%s as %s", f.Name(), s.id, key)

	util.Retry("upload via Rclone", func() error {
		size, err = uploadViaRclone(s.fs, f.Name(), key, fn)
		return err
	})

	return
}

func uploadViaRclone(rfs fs.Fs, filename string, key string, fn func(progressed int64, percentage float32) error) (fileSize int64, err error) {
	ctx := context.TODO()

	file, err := os.Open(filename)
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			return
		}
	}(file)

	if err != nil {
		return 0, err
	}

	stat, err := file.Stat()
	if err != nil {
		return 0, err
	}

	info := object.NewStaticObjectInfo(key, stat.ModTime(), stat.Size(), true, nil, rfs)

	tr := accounting.NewStats(ctx).NewTransfer(info, rfs)
	defer tr.Done(ctx, err)
	acc := tr.Account(ctx, file)
	pr := ProgressReader{acc: acc, tr: tr, fn: fn}

	obj, err := rfs.Put(ctx, &pr, info)
	if err != nil {
		return 0, err
	}

	return obj.Size(), err
}

func (s *RcloneBackendStorage) DownloadFile(filename string, key string, fn func(progressed int64, percentage float32) error) (size int64, err error) {
	glog.V(1).Infof("download dat file of %s from remote rclone.%s as %s", filename, s.id, key)

	util.Retry("download via Rclone", func() error {
		size, err = downloadViaRclone(s.fs, filename, key, fn)
		return err
	})

	return
}

func downloadViaRclone(fs fs.Fs, filename string, key string, fn func(progressed int64, percentage float32) error) (fileSize int64, err error) {
	ctx := context.TODO()

	obj, err := fs.NewObject(ctx, key)
	if err != nil {
		return 0, err
	}

	rc, err := obj.Open(ctx)
	defer func(rc io.ReadCloser) {
		err := rc.Close()
		if err != nil {
			return
		}
	}(rc)

	if err != nil {
		return 0, err
	}

	file, err := os.Create(filename)
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			return
		}
	}(file)

	tr := accounting.NewStats(ctx).NewTransfer(obj, fs)
	defer tr.Done(ctx, err)
	acc := tr.Account(ctx, rc)
	pr := ProgressReader{acc: acc, tr: tr, fn: fn}

	written, err := io.Copy(file, &pr)
	if err != nil {
		return 0, err
	}

	return written, nil
}

func (s *RcloneBackendStorage) DeleteFile(key string) (err error) {
	glog.V(1).Infof("delete dat file %s from remote", key)

	util.Retry("delete via Rclone", func() error {
		err = deleteViaRclone(s.fs, key)
		return err
	})

	return
}

func deleteViaRclone(fs fs.Fs, key string) (err error) {
	ctx := context.TODO()

	obj, err := fs.NewObject(ctx, key)
	if err != nil {
		return err
	}

	return obj.Remove(ctx)
}

type RcloneBackendStorageFile struct {
	backendStorage *RcloneBackendStorage
	key            string
	tierInfo       *volume_server_pb.VolumeInfo
}

func (rcloneBackendStorageFile RcloneBackendStorageFile) ReadAt(p []byte, off int64) (n int, err error) {
	ctx := context.TODO()

	obj, err := rcloneBackendStorageFile.backendStorage.fs.NewObject(ctx, rcloneBackendStorageFile.key)
	if err != nil {
		return 0, err
	}

	opt := fs.RangeOption{Start: off, End: off + int64(len(p)) - 1}

	rc, err := obj.Open(ctx, &opt)
	defer func(rc io.ReadCloser) {
		err := rc.Close()
		if err != nil {
			return
		}
	}(rc)

	if err != nil {
		return 0, err
	}

	return io.ReadFull(rc, p)
}

func (rcloneBackendStorageFile RcloneBackendStorageFile) WriteAt(p []byte, off int64) (n int, err error) {
	panic("not implemented")
}

func (rcloneBackendStorageFile RcloneBackendStorageFile) Truncate(off int64) error {
	panic("not implemented")
}

func (rcloneBackendStorageFile RcloneBackendStorageFile) Close() error {
	return nil
}

func (rcloneBackendStorageFile RcloneBackendStorageFile) GetStat() (datSize int64, modTime time.Time, err error) {
	files := rcloneBackendStorageFile.tierInfo.GetFiles()

	if len(files) == 0 {
		err = fmt.Errorf("remote file info not found")
		return
	}

	datSize = int64(files[0].FileSize)
	modTime = time.Unix(int64(files[0].ModifiedTime), 0)

	return
}

func (rcloneBackendStorageFile RcloneBackendStorageFile) Name() string {
	return rcloneBackendStorageFile.key
}

func (rcloneBackendStorageFile RcloneBackendStorageFile) Sync() error {
	return nil
}
