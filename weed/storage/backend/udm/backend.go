package udm

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
)

func init() {
	backend.BackendStorageFactories[storageType] = &backendFactory{}
}

const storageType = "udm"

type backendFactory struct {
}

func (factory *backendFactory) StorageType() backend.StorageType {
	return storageType
}
func (factory *backendFactory) BuildStorage(configuration backend.StringProperties, configPrefix string, id string) (backend.BackendStorage, error) {
	return newBackendStorage(configuration, configPrefix, id)
}

type BackendStorage struct {
	id           string
	grpcServer   string
	readDisabled bool
	client       *ClientSet
}

func newBackendStorage(configuration backend.StringProperties, configPrefix string, id string) (*BackendStorage, error) {
	grpcServer := configuration.GetString(configPrefix + "grpc_server")
	readDisabled, _ := strconv.ParseBool(configuration.GetString(configPrefix + "read_disabled"))

	cl, err := NewClient(grpcServer, readDisabled)
	if err != nil {
		return nil, err
	}

	glog.V(1).Infof("Adding backend storage: %s.%s", storageType, id)

	return &BackendStorage{
		id:           id,
		client:       cl,
		grpcServer:   grpcServer,
		readDisabled: readDisabled,
	}, nil
}

func (s *BackendStorage) ToProperties() map[string]string {
	return map[string]string{
		"grpc_server": s.grpcServer,
	}
}

func (s *BackendStorage) NewStorageFile(key string, tierInfo *volume_server_pb.VolumeInfo) backend.BackendStorageFile {
	f := &backendStorageFile{
		backendStorage: s,
		key:            key,
		tierInfo:       tierInfo,
	}

	return f
}

func (s *BackendStorage) CopyFile(f *os.File, fn func(progressed int64, percentage float32) error) (key string, size int64, err error) {
	key = f.Name()

	glog.V(1).Infof("copying dat file of %s to remote udm.%s as %s", f.Name(), s.id, key)

	size, err = s.client.UploadFile(context.TODO(), f.Name(), key, fn)
	if err != nil {
		glog.V(1).Infof("failed to copy file: %v", err)
		return
	}

	return
}

func (s *BackendStorage) DownloadFile(fileName string, key string, fn func(progressed int64, percentage float32) error) (size int64, err error) {

	glog.V(1).Infof("download dat file of %s from remote s3.%s as %s", fileName, s.id, key)

	size, err = s.client.DownloadFile(context.TODO(), fileName, key, fn)

	return
}

func (s *BackendStorage) DeleteFile(key string) (err error) {

	glog.V(1).Infof("delete dat file %s from remote", key)

	err = s.client.DeleteFile(context.TODO(), key)

	return
}

type backendStorageFile struct {
	backendStorage *BackendStorage
	key            string
	tierInfo       *volume_server_pb.VolumeInfo
}

func (f *backendStorageFile) ReadAt(p []byte, off int64) (n int, err error) {
	length := len(p)
	data, err := f.backendStorage.client.ReadAt(context.TODO(), f.key, off, int64(length))
	if err != nil {
		return 0, err
	}

	n = len(data)

	copy(p, data)
	if length > n {
		for i := n; i < length; i++ {
			p[i] = 0
		}
	}

	return n, nil
}

func (f *backendStorageFile) WriteAt(p []byte, off int64) (n int, err error) {
	panic(fmt.Sprintf("Can not write %s at %d with length %d: not implemented", f.key, off, len(p)))
}

func (f *backendStorageFile) Truncate(off int64) error {
	panic("not implemented")
}

func (f *backendStorageFile) Close() error {
	return nil
}

func (f *backendStorageFile) GetStat() (datSize int64, modTime time.Time, err error) {
	return
}

func (f *backendStorageFile) Name() string {
	return f.key
}

func (f *backendStorageFile) Sync() error {
	return nil
}
