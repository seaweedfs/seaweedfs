package s3

import (
	"fmt"
	"io"
	"reflect"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	v4 "github.com/aws/aws-sdk-go/aws/signer/v4"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

func init() {
	remote_storage.RemoteStorageClientMakers["s3"] = new(s3RemoteStorageMaker)
}

type s3RemoteStorageMaker struct{}

func (s s3RemoteStorageMaker) HasBucket() bool {
	return true
}

func (s s3RemoteStorageMaker) Make(conf *remote_pb.RemoteConf) (remote_storage.RemoteStorageClient, error) {
	client := &s3RemoteStorageClient{
		supportTagging: true,
		conf:           conf,
	}
	config := &aws.Config{
		Region:                        aws.String(conf.S3Region),
		Endpoint:                      aws.String(conf.S3Endpoint),
		S3ForcePathStyle:              aws.Bool(conf.S3ForcePathStyle),
		S3DisableContentMD5Validation: aws.Bool(true),
	}
	if conf.S3AccessKey != "" && conf.S3SecretKey != "" {
		config.Credentials = credentials.NewStaticCredentials(conf.S3AccessKey, conf.S3SecretKey, "")
	}

	sess, err := session.NewSession(config)
	if err != nil {
		return nil, fmt.Errorf("create aws session: %v", err)
	}
	if conf.S3V4Signature {
		sess.Handlers.Sign.PushBackNamed(v4.SignRequestHandler)
	}
	sess.Handlers.Build.PushBack(func(r *request.Request) {
		r.HTTPRequest.Header.Set("User-Agent", "SeaweedFS/"+util.VERSION_NUMBER)
	})
	sess.Handlers.Build.PushFront(skipSha256PayloadSigning)
	client.conn = s3.New(sess)
	return client, nil
}

type s3RemoteStorageClient struct {
	conf           *remote_pb.RemoteConf
	conn           s3iface.S3API
	supportTagging bool
}

var _ = remote_storage.RemoteStorageClient(&s3RemoteStorageClient{supportTagging: true})

func (s *s3RemoteStorageClient) Traverse(remote *remote_pb.RemoteStorageLocation, visitFn remote_storage.VisitFunc) (err error) {

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
		var localErr error
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
					localErr = err
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
		if localErr != nil {
			err = fmt.Errorf("process %v: %v", remote, localErr)
		}
	}
	return
}
func (s *s3RemoteStorageClient) ReadFile(loc *remote_pb.RemoteStorageLocation, offset int64, size int64) (data []byte, err error) {
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

func (s *s3RemoteStorageClient) WriteDirectory(loc *remote_pb.RemoteStorageLocation, entry *filer_pb.Entry) (err error) {
	return nil
}

func (s *s3RemoteStorageClient) RemoveDirectory(loc *remote_pb.RemoteStorageLocation) (err error) {
	return nil
}

func (s *s3RemoteStorageClient) WriteFile(loc *remote_pb.RemoteStorageLocation, entry *filer_pb.Entry, reader io.Reader) (remoteEntry *filer_pb.RemoteEntry, err error) {

	fileSize := int64(filer.FileSize(entry))

	partSize := int64(8 * 1024 * 1024) // The minimum/default allowed part size is 5MB
	for partSize*1000 < fileSize {
		partSize *= 4
	}

	// Create an uploader with the session and custom options
	uploader := s3manager.NewUploaderWithClient(s.conn, func(u *s3manager.Uploader) {
		u.PartSize = partSize
		u.Concurrency = 1
	})

	// process tagging
	tags := ""
	var awsTags *string
	// openstack swift doesn't support s3 object tagging
	if s.conf.S3SupportTagging {
		for k, v := range entry.Extended {
			if len(tags) > 0 {
				tags = tags + "&"
			}
			tags = tags + k + "=" + string(v)
		}
		awsTags = aws.String(tags)
	}

	// Upload the file to S3.
	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket:       aws.String(loc.Bucket),
		Key:          aws.String(loc.Path[1:]),
		Body:         reader,
		Tagging:      awsTags,
		StorageClass: aws.String(s.conf.S3StorageClass),
	})

	//in case it fails to upload
	if err != nil {
		return nil, fmt.Errorf("upload to %s/%s%s: %v", loc.Name, loc.Bucket, loc.Path, err)
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

func (s *s3RemoteStorageClient) readFileRemoteEntry(loc *remote_pb.RemoteStorageLocation) (*filer_pb.RemoteEntry, error) {
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

func (s *s3RemoteStorageClient) UpdateFileMetadata(loc *remote_pb.RemoteStorageLocation, oldEntry *filer_pb.Entry, newEntry *filer_pb.Entry) (err error) {
	if reflect.DeepEqual(oldEntry.Extended, newEntry.Extended) {
		return nil
	}
	tagging := toTagging(newEntry.Extended)
	if len(tagging.TagSet) > 0 {
		_, err = s.conn.PutObjectTagging(&s3.PutObjectTaggingInput{
			Bucket:  aws.String(loc.Bucket),
			Key:     aws.String(loc.Path[1:]),
			Tagging: toTagging(newEntry.Extended),
		})
	} else {
		_, err = s.conn.DeleteObjectTagging(&s3.DeleteObjectTaggingInput{
			Bucket: aws.String(loc.Bucket),
			Key:    aws.String(loc.Path[1:]),
		})
	}
	return
}
func (s *s3RemoteStorageClient) DeleteFile(loc *remote_pb.RemoteStorageLocation) (err error) {
	_, err = s.conn.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(loc.Bucket),
		Key:    aws.String(loc.Path[1:]),
	})
	return
}

func (s *s3RemoteStorageClient) ListBuckets() (buckets []*remote_storage.Bucket, err error) {
	resp, err := s.conn.ListBuckets(&s3.ListBucketsInput{})
	if err != nil {
		return nil, fmt.Errorf("list buckets: %v", err)
	}
	for _, b := range resp.Buckets {
		buckets = append(buckets, &remote_storage.Bucket{
			Name:      *b.Name,
			CreatedAt: *b.CreationDate,
		})
	}
	return
}

func (s *s3RemoteStorageClient) CreateBucket(name string) (err error) {
	_, err = s.conn.CreateBucket(&s3.CreateBucketInput{
		ACL:                        nil,
		Bucket:                     aws.String(name),
		CreateBucketConfiguration:  nil,
		GrantFullControl:           nil,
		GrantRead:                  nil,
		GrantReadACP:               nil,
		GrantWrite:                 nil,
		GrantWriteACP:              nil,
		ObjectLockEnabledForBucket: nil,
	})
	if err != nil {
		return fmt.Errorf("%s create bucket %s: %v", s.conf.Name, name, err)
	}
	return
}

func (s *s3RemoteStorageClient) DeleteBucket(name string) (err error) {
	_, err = s.conn.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: aws.String(name),
	})
	if err != nil {
		return fmt.Errorf("delete bucket %s: %v", name, err)
	}
	return
}
