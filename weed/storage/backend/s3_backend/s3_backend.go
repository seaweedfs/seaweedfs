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
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/util"
)

var (
	_ backend.DataStorageBackend = &S3Backend{}
)

func init() {
	backend.StorageBackends = append(backend.StorageBackends, &S3Backend{})
}

type S3Backend struct {
	conn   s3iface.S3API
	region string
	bucket string
	vid    needle.VolumeId
	key    string
}

func (s3backend S3Backend) ReadAt(p []byte, off int64) (n int, err error) {
	bytesRange := fmt.Sprintf("bytes=%d-%d", off, off+int64(len(p))-1)
	getObjectOutput, getObjectErr := s3backend.conn.GetObject(&s3.GetObjectInput{
		Bucket: &s3backend.bucket,
		Key:    &s3backend.key,
		Range:  &bytesRange,
	})

	if getObjectErr != nil {
		return 0, fmt.Errorf("bucket %s GetObject %s: %v", s3backend.bucket, s3backend.key, getObjectErr)
	}
	defer getObjectOutput.Body.Close()

	return getObjectOutput.Body.Read(p)

}

func (s3backend S3Backend) WriteAt(p []byte, off int64) (n int, err error) {
	panic("implement me")
}

func (s3backend S3Backend) Truncate(off int64) error {
	panic("implement me")
}

func (s3backend S3Backend) Close() error {
	return nil
}

func (s3backend S3Backend) GetStat() (datSize int64, modTime time.Time, err error) {

	headObjectOutput, headObjectErr := s3backend.conn.HeadObject(&s3.HeadObjectInput{
		Bucket: &s3backend.bucket,
		Key:    &s3backend.key,
	})

	if headObjectErr != nil {
		return 0, time.Now(), fmt.Errorf("bucket %s HeadObject %s: %v", s3backend.bucket, s3backend.key, headObjectErr)
	}

	datSize = int64(*headObjectOutput.ContentLength)
	modTime = *headObjectOutput.LastModified

	return
}

func (s3backend S3Backend) String() string {
	return fmt.Sprintf("%s/%s", s3backend.bucket, s3backend.key)
}

func (s3backend *S3Backend) GetName() string {
	return "s3"
}

func (s3backend S3Backend) Instantiate(src *os.File) error {
	panic("implement me")
}

func (s3backend *S3Backend) Initialize(configuration util.Configuration, prefix string, vid needle.VolumeId) error {
	glog.V(0).Infof("storage.backend.s3.region: %v", configuration.GetString("region"))
	glog.V(0).Infof("storage.backend.s3.bucket: %v", configuration.GetString("bucket"))
	glog.V(0).Infof("storage.backend.s3.directory: %v", configuration.GetString("directory"))

	return s3backend.initialize(
		configuration.GetString("aws_access_key_id"),
		configuration.GetString("aws_secret_access_key"),
		configuration.GetString("region"),
		configuration.GetString("bucket"),
		prefix,
		vid,
	)
}

func (s3backend *S3Backend) initialize(awsAccessKeyId, awsSecretAccessKey, region, bucket string,
	prefix string, vid needle.VolumeId) (err error) {
	s3backend.region = region
	s3backend.bucket = bucket
	s3backend.conn, err = createSession(awsAccessKeyId, awsSecretAccessKey, region)

	s3backend.vid = vid
	s3backend.key = fmt.Sprintf("%s_%d.dat", prefix, vid)
	if strings.HasPrefix(s3backend.key, "/") {
		s3backend.key = s3backend.key[1:]
	}

	return err
}
