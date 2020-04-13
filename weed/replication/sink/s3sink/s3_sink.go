package S3Sink

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/replication/sink"
	"github.com/chrislusf/seaweedfs/weed/replication/source"
	"github.com/chrislusf/seaweedfs/weed/util"
)

type S3Sink struct {
	conn        s3iface.S3API
	region      string
	bucket      string
	dir         string
	endpoint    string
	filerSource *source.FilerSource
}

func init() {
	sink.Sinks = append(sink.Sinks, &S3Sink{})
}

func (s3sink *S3Sink) GetName() string {
	return "s3"
}

func (s3sink *S3Sink) GetSinkToDirectory() string {
	return s3sink.dir
}

func (s3sink *S3Sink) Initialize(configuration util.Configuration, prefix string) error {
	glog.V(0).Infof("sink.s3.region: %v", configuration.GetString(prefix+"region"))
	glog.V(0).Infof("sink.s3.bucket: %v", configuration.GetString(prefix+"bucket"))
	glog.V(0).Infof("sink.s3.directory: %v", configuration.GetString(prefix+"directory"))
	glog.V(0).Infof("sink.s3.endpoint: %v", configuration.GetString(prefix+"endpoint"))
	return s3sink.initialize(
		configuration.GetString(prefix+"aws_access_key_id"),
		configuration.GetString(prefix+"aws_secret_access_key"),
		configuration.GetString(prefix+"region"),
		configuration.GetString(prefix+"bucket"),
		configuration.GetString(prefix+"directory"),
		configuration.GetString(prefix+"endpoint"),
	)
}

func (s3sink *S3Sink) SetSourceFiler(s *source.FilerSource) {
	s3sink.filerSource = s
}

func (s3sink *S3Sink) initialize(awsAccessKeyId, awsSecretAccessKey, region, bucket, dir, endpoint string) error {
	s3sink.region = region
	s3sink.bucket = bucket
	s3sink.dir = dir
	s3sink.endpoint = endpoint

	config := &aws.Config{
		Region:   aws.String(s3sink.region),
		Endpoint: aws.String(s3sink.endpoint),
	}
	if awsAccessKeyId != "" && awsSecretAccessKey != "" {
		config.Credentials = credentials.NewStaticCredentials(awsAccessKeyId, awsSecretAccessKey, "")
	}

	sess, err := session.NewSession(config)
	if err != nil {
		return fmt.Errorf("create aws session: %v", err)
	}
	s3sink.conn = s3.New(sess)

	return nil
}

func (s3sink *S3Sink) DeleteEntry(key string, isDirectory, deleteIncludeChunks bool) error {

	key = cleanKey(key)

	if isDirectory {
		key = key + "/"
	}

	return s3sink.deleteObject(key)

}

func (s3sink *S3Sink) CreateEntry(key string, entry *filer_pb.Entry) error {
	key = cleanKey(key)

	if entry.IsDirectory {
		return nil
	}

	uploadId, err := s3sink.createMultipartUpload(key, entry)
	if err != nil {
		return err
	}

	totalSize := filer2.TotalSize(entry.Chunks)
	chunkViews := filer2.ViewFromChunks(entry.Chunks, 0, int64(totalSize))

	parts := make([]*s3.CompletedPart, len(chunkViews))

	var wg sync.WaitGroup
	for chunkIndex, chunk := range chunkViews {
		partId := chunkIndex + 1
		wg.Add(1)
		go func(chunk *filer2.ChunkView, index int) {
			defer wg.Done()
			if part, uploadErr := s3sink.uploadPart(key, uploadId, partId, chunk); uploadErr != nil {
				err = uploadErr
			} else {
				parts[index] = part
			}
		}(chunk, chunkIndex)
	}
	wg.Wait()

	if err != nil {
		s3sink.abortMultipartUpload(key, uploadId)
		return err
	}

	return s3sink.completeMultipartUpload(context.Background(), key, uploadId, parts)

}

func (s3sink *S3Sink) UpdateEntry(key string, oldEntry *filer_pb.Entry, newParentPath string, newEntry *filer_pb.Entry, deleteIncludeChunks bool) (foundExistingEntry bool, err error) {
	key = cleanKey(key)
	// TODO improve efficiency
	return false, nil
}

func cleanKey(key string) string {
	if strings.HasPrefix(key, "/") {
		key = key[1:]
	}
	return key
}
