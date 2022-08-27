package S3Sink

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/replication/sink"
	"github.com/seaweedfs/seaweedfs/weed/replication/source"
	"github.com/seaweedfs/seaweedfs/weed/util"
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
		return nil
	}

	input := &s3.DeleteObjectInput{
		Bucket: aws.String(s3sink.bucket),
		Key:    aws.String(key),
	}

	result, err := s3sink.conn.DeleteObject(input)

	if err == nil {
		glog.V(2).Infof("[%s] delete %s: %v", s3sink.bucket, key, result)
	} else {
		glog.Errorf("[%s] delete %s: %v", s3sink.bucket, key, err)
	}

	return err

}

func (s3sink *S3Sink) CreateEntry(key string, entry *filer_pb.Entry, signatures []int32) (err error) {
	key = cleanKey(key)

	if entry.IsDirectory {
		return nil
	}

	reader := filer.NewFileReader(s3sink.filerSource, entry)

	fileSize := int64(filer.FileSize(entry))

	partSize := int64(8 * 1024 * 1024) // The minimum/default allowed part size is 5MB
	for partSize*1000 < fileSize {
		partSize *= 4
	}

	// Create an uploader with the session and custom options
	uploader := s3manager.NewUploaderWithClient(s3sink.conn, func(u *s3manager.Uploader) {
		u.PartSize = partSize
		u.Concurrency = 8
	})

	// process tagging
	tags := ""
	if true {
		for k, v := range entry.Extended {
			if len(tags) > 0 {
				tags = tags + "&"
			}
			tags = tags + k + "=" + string(v)
		}
	}

	// Upload the file to S3.
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket:  aws.String(s3sink.bucket),
		Key:     aws.String(key),
		Body:    reader,
		Tagging: aws.String(tags),
	})

	return

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
