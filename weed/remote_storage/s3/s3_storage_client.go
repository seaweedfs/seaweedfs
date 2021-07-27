package s3

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/remote_storage"
	"github.com/chrislusf/seaweedfs/weed/util"
	"strings"
)

func init() {
	remote_storage.RemoteStorageClientMakers["s3"] = new(s3RemoteStorageMaker)
}

type s3RemoteStorageMaker struct{}

func (s s3RemoteStorageMaker) Make(conf *filer_pb.RemoteConf) (remote_storage.RemoteStorageClient, error) {
	client := &s3RemoteStorageClient{
		conf: conf,
	}
	config := &aws.Config{
		Region:           aws.String(conf.S3Region),
		Endpoint:         aws.String(conf.S3Endpoint),
		S3ForcePathStyle: aws.Bool(true),
	}
	if conf.S3AccessKey != "" && conf.S3SecretKey != "" {
		config.Credentials = credentials.NewStaticCredentials(conf.S3AccessKey, conf.S3SecretKey, "")
	}

	sess, err := session.NewSession(config)
	if err != nil {
		return nil, fmt.Errorf("create aws session: %v", err)
	}
	client.conn = s3.New(sess)
	return client, nil
}

type s3RemoteStorageClient struct {
	conf *filer_pb.RemoteConf
	conn s3iface.S3API
}

func (s s3RemoteStorageClient) Traverse(rootDir string, visitFn remote_storage.VisitFunc) (err error) {
	if !strings.HasPrefix(rootDir, "/") {
		return fmt.Errorf("remote directory %s should start with /", rootDir)
	}
	bucket := strings.Split(rootDir[1:], "/")[0]
	prefix := rootDir[1+len(bucket):]
	if len(prefix) > 0 && strings.HasPrefix(prefix, "/") {
		prefix = prefix[1:]
	}

	listInput := &s3.ListObjectsV2Input{
		Bucket:              aws.String(bucket),
		ContinuationToken:   nil,
		Delimiter:           nil, // not aws.String("/"), iterate through all entries
		EncodingType:        nil,
		ExpectedBucketOwner: nil,
		FetchOwner:          nil,
		MaxKeys:             nil, // aws.Int64(1000),
		Prefix:              aws.String(prefix),
		RequestPayer:        nil,
		StartAfter:          nil,
	}
	isLastPage := false
	for !isLastPage && err == nil {
		listErr := s.conn.ListObjectsV2Pages(listInput, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			for _, content := range page.Contents {
				key := *content.Key
				dir, name := util.FullPath("/" + key).DirAndName()
				if err := visitFn(dir, name, false, &filer_pb.RemoteEntry{
					LastModifiedAt: (*content.LastModified).Unix(),
					Size:           *content.Size,
					ETag:           *content.ETag,
					StorageName:    s.conf.Name,
				}); err != nil {
					return false
				}
			}
			listInput.ContinuationToken = page.NextContinuationToken
			isLastPage = lastPage
			return true
		})
		if listErr != nil {
			err = fmt.Errorf("list %v: %v", rootDir, listErr)
		}
	}
	return
}
