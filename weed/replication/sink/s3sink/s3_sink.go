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

	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/replication/sink"
	"github.com/chrislusf/seaweedfs/weed/replication/source"
	"github.com/chrislusf/seaweedfs/weed/util"
)

type S3Sink struct {
	conn          s3iface.S3API
	region        string
	bucket        string
	dir           string
	endpoint      string
	acl           string
	filerSource   *source.FilerSource
	isIncremental bool
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

func (s3sink *S3Sink) IsIncremental() bool {
	return s3sink.isIncremental
}

func (s3sink *S3Sink) Initialize(configuration util.Configuration, prefix string) error {
	glog.V(0).Infof("sink.s3.region: %v", configuration.GetString(prefix+"region"))
	glog.V(0).Infof("sink.s3.bucket: %v", configuration.GetString(prefix+"bucket"))
	glog.V(0).Infof("sink.s3.directory: %v", configuration.GetString(prefix+"directory"))
	glog.V(0).Infof("sink.s3.endpoint: %v", configuration.GetString(prefix+"endpoint"))
	glog.V(0).Infof("sink.s3.acl: %v", configuration.GetString(prefix+"acl"))
	glog.V(0).Infof("sink.s3.is_incremental: %v", configuration.GetString(prefix+"is_incremental"))
	s3sink.isIncremental = configuration.GetBool(prefix + "is_incremental")
	return s3sink.initialize(
		configuration.GetString(prefix+"aws_access_key_id"),
		configuration.GetString(prefix+"aws_secret_access_key"),
		configuration.GetString(prefix+"region"),
		configuration.GetString(prefix+"bucket"),
		configuration.GetString(prefix+"directory"),
		configuration.GetString(prefix+"endpoint"),
		configuration.GetString(prefix+"acl"),
	)
}

func (s3sink *S3Sink) SetSourceFiler(s *source.FilerSource) {
	s3sink.filerSource = s
}

func (s3sink *S3Sink) initialize(awsAccessKeyId, awsSecretAccessKey, region, bucket, dir, endpoint, acl string) error {
	s3sink.region = region
	s3sink.bucket = bucket
	s3sink.dir = dir
	s3sink.endpoint = endpoint
	s3sink.acl = acl

	config := &aws.Config{
		Region:                        aws.String(s3sink.region),
		Endpoint:                      aws.String(s3sink.endpoint),
		S3ForcePathStyle:              aws.Bool(true),
		S3DisableContentMD5Validation: aws.Bool(true),
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

func (s3sink *S3Sink) DeleteEntry(key string, isDirectory, deleteIncludeChunks bool, signatures []int32) error {

	key = cleanKey(key)

	if isDirectory {
		key = key + "/"
	}

	return s3sink.deleteObject(key)

}

func (s3sink *S3Sink) CreateEntry(key string, entry *filer_pb.Entry, signatures []int32) error {
	key = cleanKey(key)

	if entry.IsDirectory {
		return nil
	}

	uploadId, err := s3sink.createMultipartUpload(key, entry)
	if err != nil {
		return fmt.Errorf("createMultipartUpload: %v", err)
	}

	totalSize := filer.FileSize(entry)
	chunkViews := filer.ViewFromChunks(s3sink.filerSource.LookupFileId, entry.Chunks, 0, int64(totalSize))

	parts := make([]*s3.CompletedPart, len(chunkViews))

	var wg sync.WaitGroup
	for chunkIndex, chunk := range chunkViews {
		partId := chunkIndex + 1
		wg.Add(1)
		go func(chunk *filer.ChunkView, index int) {
			defer wg.Done()
			if part, uploadErr := s3sink.uploadPart(key, uploadId, partId, chunk); uploadErr != nil {
				err = uploadErr
				glog.Errorf("uploadPart: %v", uploadErr)
			} else {
				parts[index] = part
			}
		}(chunk, chunkIndex)
	}
	wg.Wait()

	if err != nil {
		s3sink.abortMultipartUpload(key, uploadId)
		return fmt.Errorf("uploadPart: %v", err)
	}

	return s3sink.completeMultipartUpload(context.Background(), key, uploadId, parts)

}

func (s3sink *S3Sink) UpdateEntry(key string, oldEntry *filer_pb.Entry, newParentPath string, newEntry *filer_pb.Entry, deleteIncludeChunks bool, signatures []int32) (foundExistingEntry bool, err error) {
	key = cleanKey(key)
	return true, s3sink.CreateEntry(key, newEntry, signatures)
}

func cleanKey(key string) string {
	if strings.HasPrefix(key, "/") {
		key = key[1:]
	}
	return key
}
