package s3

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/remote_storage"
	"github.com/chrislusf/seaweedfs/weed/util"
	"io"
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

var _ = remote_storage.RemoteStorageClient(&s3RemoteStorageClient{})

func (s *s3RemoteStorageClient) Traverse(remote *filer_pb.RemoteStorageLocation, visitFn remote_storage.VisitFunc) (err error) {

	pathKey := remote.Path[1:]

	listInput := &s3.ListObjectsV2Input{
		Bucket:              aws.String(remote.Bucket),
		ContinuationToken:   nil,
		Delimiter:           nil, // not aws.String("/"), iterate through all entries
		EncodingType:        nil,
		ExpectedBucketOwner: nil,
		FetchOwner:          nil,
		MaxKeys:             nil, // aws.Int64(1000),
		Prefix:              aws.String(pathKey),
		RequestPayer:        nil,
		StartAfter:          nil,
	}
	isLastPage := false
	for !isLastPage && err == nil {
		listErr := s.conn.ListObjectsV2Pages(listInput, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			for _, content := range page.Contents {
				key := *content.Key
				key = "/" + key
				dir, name := util.FullPath(key).DirAndName()
				if err := visitFn(dir, name, false, &filer_pb.RemoteEntry{
					RemoteMtime: (*content.LastModified).Unix(),
					RemoteSize:  *content.Size,
					RemoteETag:  *content.ETag,
					StorageName: s.conf.Name,
				}); err != nil {
					return false
				}
			}
			listInput.ContinuationToken = page.NextContinuationToken
			isLastPage = lastPage
			return true
		})
		if listErr != nil {
			err = fmt.Errorf("list %v: %v", remote, listErr)
		}
	}
	return
}
func (s *s3RemoteStorageClient) ReadFile(loc *filer_pb.RemoteStorageLocation, offset int64, size int64) (data []byte, err error) {
	downloader := s3manager.NewDownloaderWithClient(s.conn, func(u *s3manager.Downloader) {
		u.PartSize = int64(4 * 1024 * 1024)
		u.Concurrency = 1
	})

	dataSlice := make([]byte, int(size))
	writerAt := aws.NewWriteAtBuffer(dataSlice)

	_, err = downloader.Download(writerAt, &s3.GetObjectInput{
		Bucket: aws.String(loc.Bucket),
		Key:    aws.String(loc.Path[1:]),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", offset, offset+size-1)),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to download file %s%s: %v", loc.Bucket, loc.Path, err)
	}

	return writerAt.Bytes(), nil
}

func (s *s3RemoteStorageClient) WriteDirectory(loc *filer_pb.RemoteStorageLocation, entry *filer_pb.Entry) (err error) {
	return nil
}

func (s *s3RemoteStorageClient) WriteFile(loc *filer_pb.RemoteStorageLocation, entry *filer_pb.Entry, reader io.Reader) (remoteEntry *filer_pb.RemoteEntry, err error) {

	fileSize := int64(filer.FileSize(entry))

	partSize := int64(8 * 1024 * 1024) // The minimum/default allowed part size is 5MB
	for partSize*1000 < fileSize {
		partSize *= 4
	}

	// Create an uploader with the session and custom options
	uploader := s3manager.NewUploaderWithClient(s.conn, func(u *s3manager.Uploader) {
		u.PartSize = partSize
		u.Concurrency = 5
	})

	// process tagging
	tags := ""
	for k, v := range entry.Extended {
		if len(tags) > 0 {
			tags = tags + "&"
		}
		tags = tags + k + "=" + string(v)
	}

	// Upload the file to S3.
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket:               aws.String(loc.Bucket),
		Key:                  aws.String(loc.Path[1:]),
		Body:                 reader,
		ACL:                  aws.String("private"),
		ServerSideEncryption: aws.String("AES256"),
		StorageClass:         aws.String("STANDARD_IA"),
		Tagging:              aws.String(tags),
	})

	//in case it fails to upload
	if err != nil {
		return nil, fmt.Errorf("upload to s3 %s/%s%s: %v", loc.Name, loc.Bucket, loc.Path, err)
	}

	// read back the remote entry
	return s.readFileRemoteEntry(loc)

}

func toTagging(attributes map[string][]byte) *s3.Tagging {
	tagging := &s3.Tagging{}
	for k, v := range attributes {
		tagging.TagSet = append(tagging.TagSet, &s3.Tag{
			Key:   aws.String(k),
			Value: aws.String(string(v)),
		})
	}
	return tagging
}

func (s *s3RemoteStorageClient) readFileRemoteEntry(loc *filer_pb.RemoteStorageLocation) (*filer_pb.RemoteEntry, error) {
	resp, err := s.conn.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(loc.Bucket),
		Key:    aws.String(loc.Path[1:]),
	})
	if err != nil {
		return nil, err
	}

	return &filer_pb.RemoteEntry{
		RemoteMtime: resp.LastModified.Unix(),
		RemoteSize:  *resp.ContentLength,
		RemoteETag:  *resp.ETag,
		StorageName: s.conf.Name,
	}, nil

}

func (s *s3RemoteStorageClient) UpdateFileMetadata(loc *filer_pb.RemoteStorageLocation, entry *filer_pb.Entry) (err error) {
	tagging := toTagging(entry.Extended)
	if len(tagging.TagSet) > 0 {
		_, err = s.conn.PutObjectTagging(&s3.PutObjectTaggingInput{
			Bucket:  aws.String(loc.Bucket),
			Key:     aws.String(loc.Path[1:]),
			Tagging: toTagging(entry.Extended),
		})
	} else {
		_, err = s.conn.DeleteObjectTagging(&s3.DeleteObjectTaggingInput{
			Bucket: aws.String(loc.Bucket),
			Key:    aws.String(loc.Path[1:]),
		})
	}
	return
}
func (s *s3RemoteStorageClient) DeleteFile(loc *filer_pb.RemoteStorageLocation) (err error) {
	_, err = s.conn.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(loc.Bucket),
		Key:    aws.String(loc.Path[1:]),
	})
	return
}
