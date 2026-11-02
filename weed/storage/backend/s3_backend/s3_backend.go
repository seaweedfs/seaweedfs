package s3_backend

import (
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/util"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/google/uuid"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/storage/backend"
)

func init() {
	backend.BackendStorageFactories["s3"] = &S3BackendFactory{}
}

type S3BackendFactory struct {
}

func (factory *S3BackendFactory) StorageType() backend.StorageType {
	return backend.StorageType("s3")
}
func (factory *S3BackendFactory) BuildStorage(configuration backend.StringProperties, configPrefix string, id string) (backend.BackendStorage, error) {
	return newS3BackendStorage(configuration, configPrefix, id)
}

type S3BackendStorage struct {
	id                    string
	aws_access_key_id     string
	aws_secret_access_key string
	region                string
	bucket                string
	endpoint              string
	storageClass          string
	forcePathStyle        bool
	uploadConcurrency     int
	downloadConcurrency   int
	httpOptions           httpOptions
	conn                  s3iface.S3API
}

func newS3BackendStorage(configuration backend.StringProperties, configPrefix string, id string) (s *S3BackendStorage, err error) {
	s = &S3BackendStorage{}
	s.id = id
	s.aws_access_key_id = configuration.GetString(configPrefix + "aws_access_key_id")
	s.aws_secret_access_key = configuration.GetString(configPrefix + "aws_secret_access_key")
	s.region = configuration.GetString(configPrefix + "region")
	s.bucket = configuration.GetString(configPrefix + "bucket")
	s.endpoint = configuration.GetString(configPrefix + "endpoint")
	s.storageClass = configuration.GetString(configPrefix + "storage_class")
	s.forcePathStyle = util.ParseBool(configuration.GetString(configPrefix+"force_path_style"), true)
	s.uploadConcurrency = parseConcurrency(configuration.GetString(configPrefix+"upload_concurrency"), defaultUploadConcurrency)
	s.downloadConcurrency = parseConcurrency(configuration.GetString(configPrefix+"download_concurrency"), defaultDownloadConcurrency)
	s.httpOptions = httpOptions{
		disableHTTP2:    util.ParseBool(configuration.GetString(configPrefix+"disable_http2"), false),
		maxConnsPerHost: parseNonNegative(configuration.GetString(configPrefix+"max_connections"), "max_connections"),
		idleConnTimeout: parseIdleConnTimeout(configuration.GetString(configPrefix + "idle_connection_timeout_seconds")),
	}
	if s.storageClass == "" {
		s.storageClass = "STANDARD_IA"
	}

	s.conn, err = createSession(s.aws_access_key_id, s.aws_secret_access_key, s.region, s.endpoint, s.forcePathStyle, s.httpOptions)

	glog.V(0).Infof("created backend storage s3.%s for region %s bucket %s", s.id, s.region, s.bucket)
	return
}

func (s *S3BackendStorage) ToProperties() map[string]string {
	m := make(map[string]string)
	m["aws_access_key_id"] = s.aws_access_key_id
	m["aws_secret_access_key"] = s.aws_secret_access_key
	m["region"] = s.region
	m["bucket"] = s.bucket
	m["endpoint"] = s.endpoint
	m["storage_class"] = s.storageClass
	m["force_path_style"] = util.BoolToString(s.forcePathStyle)
	m["upload_concurrency"] = strconv.Itoa(s.uploadConcurrency)
	m["download_concurrency"] = strconv.Itoa(s.downloadConcurrency)
	m["disable_http2"] = util.BoolToString(s.httpOptions.disableHTTP2)
	m["max_connections"] = strconv.Itoa(s.httpOptions.maxConnsPerHost)
	m["idle_connection_timeout_seconds"] = strconv.FormatInt(int64(s.httpOptions.idleConnTimeout/time.Second), 10)
	return m
}

const (
	defaultUploadConcurrency   = 5
	defaultDownloadConcurrency = 5
)

func parseConcurrency(value string, def int) int {
	if value == "" {
		return def
	}
	if n, err := strconv.Atoi(value); err == nil && n > 0 {
		return n
	}
	glog.Warningf("invalid concurrency value %q, using default %d", value, def)
	return def
}

// parseNonNegative reads an optional non-negative integer setting; empty or
// invalid means 0 (the default).
func parseNonNegative(value, name string) int {
	if value == "" {
		return 0
	}
	if n, err := strconv.Atoi(value); err == nil && n >= 0 {
		return n
	}
	glog.Warningf("invalid %s value %q, using the default", name, value)
	return 0
}

// maxIdleConnTimeoutSeconds is the largest number of seconds a time.Duration
// can hold.
const maxIdleConnTimeoutSeconds = math.MaxInt64 / int64(time.Second)

// parseIdleConnTimeout reads idle_connection_timeout_seconds; empty, invalid
// or too large to represent as a time.Duration means 0 (the default). It
// parses 64-bit seconds so the range is the same on 32-bit platforms.
func parseIdleConnTimeout(value string) time.Duration {
	if value == "" {
		return 0
	}
	seconds, err := strconv.ParseInt(value, 10, 64)
	if err != nil || seconds < 0 {
		glog.Warningf("invalid idle_connection_timeout_seconds value %q, using the default", value)
		return 0
	}
	if seconds > maxIdleConnTimeoutSeconds {
		glog.Warningf("idle_connection_timeout_seconds %q is out of range, using the default", value)
		return 0
	}
	return time.Duration(seconds) * time.Second
}

func (s *S3BackendStorage) NewStorageFile(key string, tierInfo *volume_server_pb.VolumeInfo) backend.BackendStorageFile {
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

func (s *S3BackendStorage) CopyFile(f *os.File, fn func(progressed int64, percentage float32) error, concurrency int) (key string, size int64, err error) {
	randomUuid, _ := uuid.NewRandom()
	key = randomUuid.String()

	if concurrency <= 0 {
		concurrency = s.uploadConcurrency
	}

	glog.V(1).Infof("copying dat file of %s to remote s3.%s as %s", f.Name(), s.id, key)

	util.Retry("upload to S3", func() error {
		size, err = uploadToS3(s.conn, f.Name(), s.bucket, key, s.storageClass, fn, concurrency)
		return err
	})

	return
}

func (s *S3BackendStorage) DownloadFile(fileName string, key string, fn func(progressed int64, percentage float32) error, concurrency int) (size int64, err error) {
	if concurrency <= 0 {
		concurrency = s.downloadConcurrency
	}

	glog.V(1).Infof("download dat file of %s from remote s3.%s as %s", fileName, s.id, key)

	size, err = downloadFromS3(s.conn, fileName, s.bucket, key, fn, concurrency)

	return
}

func (s *S3BackendStorage) DeleteFile(key string) (err error) {

	glog.V(1).Infof("delete dat file %s from remote", key)

	err = deleteFromS3(s.conn, s.bucket, key)

	return
}

type S3BackendStorageFile struct {
	backendStorage *S3BackendStorage
	key            string
	tierInfo       *volume_server_pb.VolumeInfo
}

func (s3backendStorageFile S3BackendStorageFile) ReadAt(p []byte, off int64) (n int, err error) {
	if off < 0 {
		return 0, fmt.Errorf("negative offset %d", off)
	}

	requested := len(p)
	if requested == 0 {
		return 0, nil
	}

	datSize, _, _ := s3backendStorageFile.GetStat()

	if datSize > 0 && off >= datSize {
		return 0, io.EOF
	}

	bytesRange := fmt.Sprintf("bytes=%d-%d", off, off+int64(len(p))-1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	getObjectOutput, getObjectErr := s3backendStorageFile.backendStorage.conn.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: &s3backendStorageFile.backendStorage.bucket,
		Key:    &s3backendStorageFile.key,
		Range:  &bytesRange,
	})

	if getObjectErr != nil {
		return 0, fmt.Errorf("bucket %s GetObject %s: %v", s3backendStorageFile.backendStorage.bucket, s3backendStorageFile.key, getObjectErr)
	}
	defer getObjectOutput.Body.Close()
	stalled := time.AfterFunc(s3ReadTimeout, cancel)
	defer stalled.Stop()

	// glog.V(3).Infof("read %s %s", s3backendStorageFile.key, bytesRange)
	// glog.V(3).Infof("content range: %s, contentLength: %d", *getObjectOutput.ContentRange, *getObjectOutput.ContentLength)

	var readCount int
	for {
		p = p[readCount:]
		if len(p) == 0 {
			break
		}
		stalled.Reset(s3ReadTimeout)
		readCount, err = getObjectOutput.Body.Read(p)
		n += readCount

		if err != nil {
			break
		}
	}

	if err == io.EOF && n == requested {
		err = nil
	}
	if err != nil && ctx.Err() != nil {
		err = fmt.Errorf("bucket %s read %s: no data for %v: %w", s3backendStorageFile.backendStorage.bucket, s3backendStorageFile.key, s3ReadTimeout, err)
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

func (s3backendStorageFile S3BackendStorageFile) Sync() error {
	return nil
}
