package S3Sink

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

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
	isBucketToBucket bool
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

func (s3sink *S3Sink) IsBucketToBucket() bool {
	return s3sink.isBucketToBucket
}

func (s3sink *S3Sink) Initialize(configuration util.Configuration, prefix string) error {
	glog.V(0).Infof("sink.s3.region: %v", configuration.GetString(prefix+"region"))
	glog.V(0).Infof("sink.s3.endpoint: %v", configuration.GetString(prefix+"endpoint"))
	glog.V(0).Infof("sink.s3.acl: %v", configuration.GetString(prefix+"acl"))
	glog.V(0).Infof("sink.s3.is_bucket_to_bucket: %v", configuration.GetString(prefix+"is_bucket_to_bucket"))
	s3sink.isBucketToBucket = configuration.GetBool(prefix + "is_bucket_to_bucket")
	
	if s3sink.isBucketToBucket {
		s3sink.isIncremental = false
		return s3sink.initialize(
			configuration.GetString(prefix+"aws_access_key_id"),
			configuration.GetString(prefix+"aws_secret_access_key"),
			configuration.GetString(prefix+"region"),
			"",
			"",
			configuration.GetString(prefix+"endpoint"),
			configuration.GetString(prefix+"acl"),
		)
	} else {
		glog.V(0).Infof("sink.s3.bucket: %v", configuration.GetString(prefix+"bucket"))
		glog.V(0).Infof("sink.s3.directory: %v", configuration.GetString(prefix+"directory"))
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

	bucket := s3sink.bucket

	if isDirectory {
		if s3sink.isBucketToBucket {
			if IsKeyBucket(key) {
				return s3sink.deleteBucketIfExists(key)
			}
		}
		return nil
	}

	if s3sink.isBucketToBucket {
		bucket = GetBucketFromKey(key)
		key = RemoveBucketFromKey(key)
	}

	input := &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}

	result, err := s3sink.conn.DeleteObject(input)

	if err == nil {
		glog.V(2).Infof("[%s] delete %s: %v", bucket, key, result)
	} else {
		glog.Errorf("[%s] delete %s: %v", bucket, key, err)
	}

	return err

}

func (s3sink *S3Sink) CreateEntry(key string, entry *filer_pb.Entry, signatures []int32) (err error) {
	key = cleanKey(key)

	bucket := s3sink.bucket

	if entry.IsDirectory {
		if s3sink.isBucketToBucket {
			if IsKeyBucket(key) {
				return s3sink.createBucketIfNotExists(key)
			}
		}
		return nil
	}

	if s3sink.isBucketToBucket {
		bucket = GetBucketFromKey(key)
		s3sink.createBucketIfNotExists(bucket)
		key = RemoveBucketFromKey(key)
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
		Bucket:  aws.String(bucket),
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

func GetBucketFromKey(key string) string {
	key = cleanKey(key)
	return key[:strings.Index(key, "/")]
}

func RemoveBucketFromKey(key string) string {
	key = cleanKey(key)
	return key[strings.Index(key, "/")+1:]
}

func IsKeyBucket(key string) bool {
	key = cleanKey(key)
	return !strings.Contains(key, "/")
}

func (s3sink *S3Sink) createBucketIfNotExists(bucket string) error {
	// Check if the bucket exists
	_, err := s3sink.conn.HeadBucket(&s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})

	if err != nil {
		// Bucket does not exist, create it
		createBucketInput := &s3.CreateBucketInput{
			Bucket: aws.String(bucket),
			ACL:    aws.String(s3sink.acl),
		}

		_, err := s3sink.conn.CreateBucket(createBucketInput)
		if err != nil {
			return err
		}

		glog.V(0).Infof("Bucket %s created successfully", s3sink.bucket)
	} else {
		glog.V(0).Infof("Bucket %s already exists", s3sink.bucket)
	}

	return nil
}

func (s3sink *S3Sink) deleteBucketIfExists(bucket string) error {
	// Check if the bucket exists
	_, err := s3sink.conn.HeadBucket(&s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})

	if err != nil {
		// Bucket does not exist, nothing to delete
		glog.V(0).Infof("Bucket %s does not exist, nothing to delete", bucket)
		return nil
	}

	// Bucket exists, delete it
	deleteBucketInput := &s3.DeleteBucketInput{
		Bucket: aws.String(bucket),
	}

	_, err = s3sink.conn.DeleteBucket(deleteBucketInput)
	if err != nil {
		return err
	}

	glog.V(0).Infof("Bucket %s deleted successfully", bucket)

	return nil
}
