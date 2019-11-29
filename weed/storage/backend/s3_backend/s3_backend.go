package s3_backend

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage/backend"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func init() {
	backend.BackendStorageFactories["s3"] = &S3BackendFactory{}
}

type S3BackendFactory struct {
}

func (factory *S3BackendFactory) StorageType() backend.StorageType {
	return backend.StorageType("s3")
}
func (factory *S3BackendFactory) BuildStorage(configuration util.Configuration, id string) (backend.BackendStorage, error) {
	return newS3BackendStorage(configuration, id)
}

type S3BackendStorage struct {
	id     string
	conn   s3iface.S3API
	region string
	bucket string
}

func newS3BackendStorage(configuration util.Configuration, id string) (s *S3BackendStorage, err error) {
	s = &S3BackendStorage{}
	s.id = id
	s.conn, err = createSession(
		configuration.GetString("aws_access_key_id"),
		configuration.GetString("aws_secret_access_key"),
		configuration.GetString("region"))
	s.region = configuration.GetString("region")
	s.bucket = configuration.GetString("bucket")

	glog.V(0).Infof("created s3 backend storage %s for region %s bucket %s", s.Name(), s.region, s.bucket)
	return
}

func (s *S3BackendStorage) Name() string {
	return "s3." + s.id
}

func (s *S3BackendStorage) NewStorageFile(key string) backend.BackendStorageFile {
	if strings.HasPrefix(key, "/") {
		key = key[1:]
	}

	f := &S3BackendStorageFile{
		backendStorage: s,
		key:            key,
	}

	return f
}

type S3BackendStorageFile struct {
	backendStorage *S3BackendStorage
	key            string
}

func (s3backendStorageFile S3BackendStorageFile) ReadAt(p []byte, off int64) (n int, err error) {
	bytesRange := fmt.Sprintf("bytes=%d-%d", off, off+int64(len(p))-1)
	getObjectOutput, getObjectErr := s3backendStorageFile.backendStorage.conn.GetObject(&s3.GetObjectInput{
		Bucket: &s3backendStorageFile.backendStorage.bucket,
		Key:    &s3backendStorageFile.key,
		Range:  &bytesRange,
	})

	if getObjectErr != nil {
		return 0, fmt.Errorf("bucket %s GetObject %s: %v",
			s3backendStorageFile.backendStorage.bucket, s3backendStorageFile.key, getObjectErr)
	}
	defer getObjectOutput.Body.Close()

	return getObjectOutput.Body.Read(p)

}

func (s3backendStorageFile S3BackendStorageFile) WriteAt(p []byte, off int64) (n int, err error) {
	panic("implement me")
}

func (s3backendStorageFile S3BackendStorageFile) Truncate(off int64) error {
	panic("implement me")
}

func (s3backendStorageFile S3BackendStorageFile) Close() error {
	return nil
}

func (s3backendStorageFile S3BackendStorageFile) GetStat() (datSize int64, modTime time.Time, err error) {

	headObjectOutput, headObjectErr := s3backendStorageFile.backendStorage.conn.HeadObject(&s3.HeadObjectInput{
		Bucket: &s3backendStorageFile.backendStorage.bucket,
		Key:    &s3backendStorageFile.key,
	})

	if headObjectErr != nil {
		return 0, time.Now(), fmt.Errorf("bucket %s HeadObject %s: %v",
			s3backendStorageFile.backendStorage.bucket, s3backendStorageFile.key, headObjectErr)
	}

	datSize = int64(*headObjectOutput.ContentLength)
	modTime = *headObjectOutput.LastModified

	return
}

func (s3backendStorageFile S3BackendStorageFile) String() string {
	return s3backendStorageFile.key
}

func (s3backendStorageFile *S3BackendStorageFile) GetName() string {
	return "s3"
}

func (s3backendStorageFile S3BackendStorageFile) Instantiate(src *os.File) error {
	panic("implement me")
}
