package s3_backend

import (
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/volume_server_pb"
	"github.com/chrislusf/seaweedfs/weed/storage/backend"
	"github.com/google/uuid"
)

func init() {
	backend.BackendStorageFactories["s3"] = &S3BackendFactory{}
}

type S3BackendFactory struct {
}

func (factory *S3BackendFactory) StorageType() backend.StorageType {
	return backend.StorageType("s3")
}
func (factory *S3BackendFactory) BuildStorage(configuration backend.StringProperties, id string) (backend.BackendStorage, error) {
	return newS3BackendStorage(configuration, id)
}

type S3BackendStorage struct {
	id                    string
	aws_access_key_id     string
	aws_secret_access_key string
	region                string
	bucket                string
	conn                  s3iface.S3API
}

func newS3BackendStorage(configuration backend.StringProperties, id string) (s *S3BackendStorage, err error) {
	s = &S3BackendStorage{}
	s.id = id
	s.aws_access_key_id = configuration.GetString("aws_access_key_id")
	s.aws_secret_access_key = configuration.GetString("aws_secret_access_key")
	s.region = configuration.GetString("region")
	s.bucket = configuration.GetString("bucket")
	s.conn, err = createSession(s.aws_access_key_id, s.aws_secret_access_key, s.region)

	glog.V(0).Infof("created backend storage s3.%s for region %s bucket %s", s.id, s.region, s.bucket)
	return
}

func (s *S3BackendStorage) ToProperties() map[string]string {
	m := make(map[string]string)
	m["aws_access_key_id"] = s.aws_access_key_id
	m["aws_secret_access_key"] = s.aws_secret_access_key
	m["region"] = s.region
	m["bucket"] = s.bucket
	return m
}

func (s *S3BackendStorage) NewStorageFile(key string, tierInfo *volume_server_pb.VolumeTierInfo) backend.BackendStorageFile {
	if strings.HasPrefix(key, "/") {
		key = key[1:]
	}

	f := &S3BackendStorageFile{
		backendStorage: s,
		key:            key,
		tierInfo:       tierInfo,
	}

	return f
}

func (s *S3BackendStorage) CopyFile(f *os.File, fn func(progressed int64, percentage float32) error) (key string, size int64, err error) {
	randomUuid, _ := uuid.NewRandom()
	key = randomUuid.String()

	glog.V(1).Infof("copying dat file of %s to remote s3.%s as %s", f.Name(), s.id, key)

	size, err = uploadToS3(s.conn, f.Name(), s.bucket, key, fn)

	return
}

type S3BackendStorageFile struct {
	backendStorage *S3BackendStorage
	key            string
	tierInfo       *volume_server_pb.VolumeTierInfo
}

func (s3backendStorageFile S3BackendStorageFile) ReadAt(p []byte, off int64) (n int, err error) {

	bytesRange := fmt.Sprintf("bytes=%d-%d", off, off+int64(len(p))-1)

	// glog.V(0).Infof("read %s %s", s3backendStorageFile.key, bytesRange)

	getObjectOutput, getObjectErr := s3backendStorageFile.backendStorage.conn.GetObject(&s3.GetObjectInput{
		Bucket: &s3backendStorageFile.backendStorage.bucket,
		Key:    &s3backendStorageFile.key,
		Range:  &bytesRange,
	})

	if getObjectErr != nil {
		return 0, fmt.Errorf("bucket %s GetObject %s: %v", s3backendStorageFile.backendStorage.bucket, s3backendStorageFile.key, getObjectErr)
	}
	defer getObjectOutput.Body.Close()

	glog.V(4).Infof("read %s %s", s3backendStorageFile.key, bytesRange)
	glog.V(4).Infof("content range: %s, contentLength: %d", *getObjectOutput.ContentRange, *getObjectOutput.ContentLength)

	for {
		if n, err = getObjectOutput.Body.Read(p); err == nil && n < len(p) {
			p = p[n:]
		} else {
			break
		}
	}

	if err == io.EOF {
		err = nil
	}

	return
}

func (s3backendStorageFile S3BackendStorageFile) WriteAt(p []byte, off int64) (n int, err error) {
	panic("not implemented")
}

func (s3backendStorageFile S3BackendStorageFile) Truncate(off int64) error {
	panic("not implemented")
}

func (s3backendStorageFile S3BackendStorageFile) Close() error {
	return nil
}

func (s3backendStorageFile S3BackendStorageFile) GetStat() (datSize int64, modTime time.Time, err error) {

	files := s3backendStorageFile.tierInfo.GetFiles()

	if len(files) == 0 {
		err = fmt.Errorf("remote file info not found")
		return
	}

	datSize = int64(files[0].FileSize)
	modTime = time.Unix(int64(files[0].ModifiedTime), 0)

	return
}

func (s3backendStorageFile S3BackendStorageFile) Name() string {
	return s3backendStorageFile.key
}
