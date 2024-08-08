package S3Sink

import (
	"encoding/base64"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"strconv"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/replication/sink"
	"github.com/seaweedfs/seaweedfs/weed/replication/source"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

type S3Sink struct {
	conn                          s3iface.S3API
	filerSource                   *source.FilerSource
	isIncremental                 bool
	keepPartSize                  bool
	s3DisableContentMD5Validation bool
	s3ForcePathStyle              bool
	uploaderConcurrency           int
	uploaderMaxUploadParts        int
	uploaderPartSizeMb            int
	region                        string
	bucket                        string
	dir                           string
	endpoint                      string
	acl                           string
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
	configuration.SetDefault(prefix+"region", "us-east-2")
	configuration.SetDefault(prefix+"directory", "/")
	configuration.SetDefault(prefix+"keep_part_size", true)
	configuration.SetDefault(prefix+"uploader_max_upload_parts", 1000)
	configuration.SetDefault(prefix+"uploader_part_size_mb", 8)
	configuration.SetDefault(prefix+"uploader_concurrency", 8)
	configuration.SetDefault(prefix+"s3_disable_content_md5_validation", true)
	configuration.SetDefault(prefix+"s3_force_path_style", true)
	s3sink.region = configuration.GetString(prefix + "region")
	s3sink.bucket = configuration.GetString(prefix + "bucket")
	s3sink.dir = configuration.GetString(prefix + "directory")
	s3sink.endpoint = configuration.GetString(prefix + "endpoint")
	s3sink.acl = configuration.GetString(prefix + "acl")
	s3sink.isIncremental = configuration.GetBool(prefix + "is_incremental")
	s3sink.keepPartSize = configuration.GetBool(prefix + "keep_part_size")
	s3sink.s3DisableContentMD5Validation = configuration.GetBool(prefix + "s3_disable_content_md5_validation")
	s3sink.s3ForcePathStyle = configuration.GetBool(prefix + "s3_force_path_style")
	s3sink.uploaderMaxUploadParts = configuration.GetInt(prefix + "uploader_max_upload_parts")
	s3sink.uploaderPartSizeMb = configuration.GetInt(prefix + "uploader_part_size")
	s3sink.uploaderConcurrency = configuration.GetInt(prefix + "uploader_concurrency")

	glog.V(0).Infof("sink.s3.region: %v", s3sink.region)
	glog.V(0).Infof("sink.s3.bucket: %v", s3sink.bucket)
	glog.V(0).Infof("sink.s3.directory: %v", s3sink.dir)
	glog.V(0).Infof("sink.s3.endpoint: %v", s3sink.endpoint)
	glog.V(0).Infof("sink.s3.acl: %v", s3sink.acl)
	glog.V(0).Infof("sink.s3.is_incremental: %v", s3sink.isIncremental)
	glog.V(0).Infof("sink.s3.s3_disable_content_md5_validation: %v", s3sink.s3DisableContentMD5Validation)
	glog.V(0).Infof("sink.s3.s3_force_path_style: %v", s3sink.s3ForcePathStyle)
	glog.V(0).Infof("sink.s3.keep_part_size: %v", s3sink.keepPartSize)
	if s3sink.uploaderMaxUploadParts > s3manager.MaxUploadParts {
		s3sink.uploaderMaxUploadParts = s3manager.MaxUploadParts
		glog.Warningf("uploader_max_upload_parts is greater than the maximum number of parts allowed when uploading multiple parts to Amazon S3")
		glog.V(0).Infof("sink.s3.uploader_max_upload_parts: %v => %v", s3sink.uploaderMaxUploadParts, s3manager.MaxUploadParts)
	} else {
		glog.V(0).Infof("sink.s3.uploader_max_upload_parts: %v", s3sink.uploaderMaxUploadParts)
	}
	glog.V(0).Infof("sink.s3.uploader_part_size_mb: %v", s3sink.uploaderPartSizeMb)
	glog.V(0).Infof("sink.s3.uploader_concurrency: %v", s3sink.uploaderConcurrency)

	return s3sink.initialize(
		configuration.GetString(prefix+"aws_access_key_id"),
		configuration.GetString(prefix+"aws_secret_access_key"),
	)
}

func (s3sink *S3Sink) SetSourceFiler(s *source.FilerSource) {
	s3sink.filerSource = s
}

func (s3sink *S3Sink) initialize(awsAccessKeyId, awsSecretAccessKey string) error {
	config := &aws.Config{
		Region:                        aws.String(s3sink.region),
		Endpoint:                      aws.String(s3sink.endpoint),
		S3DisableContentMD5Validation: aws.Bool(s3sink.s3DisableContentMD5Validation),
		S3ForcePathStyle:              aws.Bool(s3sink.s3ForcePathStyle),
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

	// Create an uploader with the session and custom options
	uploader := s3manager.NewUploaderWithClient(s3sink.conn, func(u *s3manager.Uploader) {
		u.PartSize = int64(s3sink.uploaderPartSizeMb * 1024 * 1024)
		u.Concurrency = s3sink.uploaderConcurrency
		u.MaxUploadParts = s3sink.uploaderMaxUploadParts
	})

	if s3sink.keepPartSize {
		switch chunkCount := len(entry.Chunks); {
		case chunkCount > 1:
			if firstChunkSize := int64(entry.Chunks[0].Size); firstChunkSize > s3manager.MinUploadPartSize {
				uploader.PartSize = firstChunkSize
			}
		default:
			uploader.PartSize = 0
		}
	}

	doSaveMtime := true
	if entry.Extended == nil {
		entry.Extended = make(map[string][]byte)
	} else if _, ok := entry.Extended[s3_constants.AmzUserMetaMtime]; ok {
		doSaveMtime = false
	}
	if doSaveMtime {
		entry.Extended[s3_constants.AmzUserMetaMtime] = []byte(strconv.FormatInt(entry.Attributes.Mtime, 10))
	}
	// process tagging
	tags := ""
	for k, v := range entry.Extended {
		if len(tags) > 0 {
			tags = tags + "&"
		}
		tags = tags + k + "=" + string(v)
	}

	// Upload the file to S3.
	uploadInput := s3manager.UploadInput{
		Bucket:  aws.String(s3sink.bucket),
		Key:     aws.String(key),
		Body:    reader,
		Tagging: aws.String(tags),
	}
	if len(entry.Attributes.Md5) > 0 {
		uploadInput.ContentMD5 = aws.String(base64.StdEncoding.EncodeToString([]byte(entry.Attributes.Md5)))
	}
	_, err = uploader.Upload(&uploadInput)

	return err

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
