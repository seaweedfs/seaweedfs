package s3

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	v4 "github.com/aws/aws-sdk-go/aws/signer/v4"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/version"
)

func init() {
	remote_storage.RemoteStorageClientMakers["s3"] = new(s3RemoteStorageMaker)
}

type s3RemoteStorageMaker struct{}

func (s s3RemoteStorageMaker) HasBucket() bool {
	return true
}

func (s s3RemoteStorageMaker) Make(conf *remote_pb.RemoteConf) (remote_storage.RemoteStorageClient, error) {
	return MakeWithHTTPClient(conf, nil)
}

// s3CompatibleOptions carries the per-provider AWS SDK knobs the shared client
// builder needs. Every S3-SDK-backed provider (s3 plus the wasabi/b2/... family)
// converges on the same s3RemoteStorageClient and dials a caller-supplied
// endpoint, differing only in which RemoteConf fields it reads.
type s3CompatibleOptions struct {
	name           string
	endpoint       string
	region         string
	accessKey      string
	secretKey      string
	forcePathStyle bool
	signV4         bool
	// anonymousWhenNoCreds disables SigV4 signing for public buckets when no
	// credentials are supplied. Only the generic "s3" provider does this.
	anonymousWhenNoCreds bool
	// setUserAgent adds the SeaweedFS User-Agent header (generic "s3" only).
	setUserAgent bool
}

// s3CompatibleClientOptions returns the client knobs for an S3-SDK-backed
// RemoteConf and whether conf is such a type. Keeping the type-to-fields
// mapping in one place lets the client builder and the SSRF endpoint guard
// agree on exactly which endpoint each provider dials.
func s3CompatibleClientOptions(conf *remote_pb.RemoteConf) (s3CompatibleOptions, bool) {
	switch conf.Type {
	case "s3":
		return s3CompatibleOptions{
			name:                 "s3",
			endpoint:             conf.S3Endpoint,
			region:               conf.S3Region,
			accessKey:            conf.S3AccessKey,
			secretKey:            conf.S3SecretKey,
			forcePathStyle:       conf.S3ForcePathStyle,
			signV4:               conf.S3V4Signature,
			anonymousWhenNoCreds: true,
			setUserAgent:         true,
		}, true
	case "aliyun":
		return s3CompatibleOptions{
			name:      "aliyun",
			endpoint:  conf.AliyunEndpoint,
			region:    conf.AliyunRegion,
			accessKey: util.Nvl(conf.AliyunAccessKey, os.Getenv("ALICLOUD_ACCESS_KEY_ID")),
			secretKey: util.Nvl(conf.AliyunSecretKey, os.Getenv("ALICLOUD_ACCESS_KEY_SECRET")),
		}, true
	case "b2":
		return s3CompatibleOptions{
			name:           "backblaze",
			endpoint:       conf.BackblazeEndpoint,
			region:         conf.BackblazeRegion,
			accessKey:      conf.BackblazeKeyId,
			secretKey:      conf.BackblazeApplicationKey,
			forcePathStyle: true,
		}, true
	case "baidu":
		return s3CompatibleOptions{
			name:      "baidu",
			endpoint:  conf.BaiduEndpoint,
			region:    conf.BaiduRegion,
			accessKey: util.Nvl(conf.BaiduAccessKey, os.Getenv("BDCLOUD_ACCESS_KEY")),
			secretKey: util.Nvl(conf.BaiduSecretKey, os.Getenv("BDCLOUD_SECRET_KEY")),
			signV4:    true,
		}, true
	case "contabo":
		return s3CompatibleOptions{
			name:           "contabo",
			endpoint:       conf.ContaboEndpoint,
			region:         conf.ContaboRegion,
			accessKey:      util.Nvl(conf.ContaboAccessKey, os.Getenv("ACCESS_KEY")),
			secretKey:      util.Nvl(conf.ContaboSecretKey, os.Getenv("SECRET_KEY")),
			forcePathStyle: true,
		}, true
	case "filebase":
		return s3CompatibleOptions{
			name:           "filebase",
			endpoint:       conf.FilebaseEndpoint,
			region:         "us-east-1",
			accessKey:      util.Nvl(conf.FilebaseAccessKey, os.Getenv("AWS_ACCESS_KEY_ID")),
			secretKey:      util.Nvl(conf.FilebaseSecretKey, os.Getenv("AWS_SECRET_ACCESS_KEY")),
			forcePathStyle: true,
			signV4:         true,
		}, true
	case "storj":
		return s3CompatibleOptions{
			name:           "storj",
			endpoint:       conf.StorjEndpoint,
			region:         "us-west-2",
			accessKey:      util.Nvl(conf.StorjAccessKey, os.Getenv("AWS_ACCESS_KEY_ID")),
			secretKey:      util.Nvl(conf.StorjSecretKey, os.Getenv("AWS_SECRET_ACCESS_KEY")),
			forcePathStyle: true,
		}, true
	case "tencent":
		return s3CompatibleOptions{
			name:           "tencent",
			endpoint:       conf.TencentEndpoint,
			region:         "us-west-2",
			accessKey:      util.Nvl(conf.TencentSecretId, os.Getenv("COS_SECRETID")),
			secretKey:      util.Nvl(conf.TencentSecretKey, os.Getenv("COS_SECRETKEY")),
			forcePathStyle: true,
		}, true
	case "wasabi":
		return s3CompatibleOptions{
			name:           "wasabi",
			endpoint:       conf.WasabiEndpoint,
			region:         conf.WasabiRegion,
			accessKey:      conf.WasabiAccessKey,
			secretKey:      conf.WasabiSecretKey,
			forcePathStyle: true,
		}, true
	}
	return s3CompatibleOptions{}, false
}

// S3CompatibleEndpoint returns the endpoint an S3-SDK-backed RemoteConf would
// dial directly, and whether conf is such a type. The volume server validates
// this endpoint against the SSRF deny-list for every S3-compatible provider,
// not just the generic "s3" type.
func S3CompatibleEndpoint(conf *remote_pb.RemoteConf) (string, bool) {
	opt, ok := s3CompatibleClientOptions(conf)
	if !ok {
		return "", false
	}
	return opt.endpoint, true
}

// MakeWithHTTPClient builds the client for any S3-SDK-backed remote storage
// type using the supplied *http.Client (or the AWS SDK default when nil).
// Callers that need to pin the dial path against DNS rebinding can pass a
// client whose transport has a guarded DialContext.
func MakeWithHTTPClient(conf *remote_pb.RemoteConf, httpClient *http.Client) (remote_storage.RemoteStorageClient, error) {
	opt, ok := s3CompatibleClientOptions(conf)
	if !ok {
		return nil, fmt.Errorf("%q is not an S3-compatible remote storage type", conf.Type)
	}
	client := &s3RemoteStorageClient{
		conf: conf,
	}
	config := &aws.Config{
		Region:                        aws.String(opt.region),
		Endpoint:                      aws.String(opt.endpoint),
		S3ForcePathStyle:              aws.Bool(opt.forcePathStyle),
		S3DisableContentMD5Validation: aws.Bool(true),
	}
	if httpClient != nil {
		config.HTTPClient = httpClient
	}
	if opt.accessKey != "" && opt.secretKey != "" {
		config.Credentials = credentials.NewStaticCredentials(opt.accessKey, opt.secretKey, "")
	} else if opt.anonymousWhenNoCreds && opt.accessKey == "" && opt.secretKey == "" {
		// Explicitly disable signing for public buckets.
		config.Credentials = credentials.AnonymousCredentials
	}

	sess, err := session.NewSession(config)
	if err != nil {
		return nil, fmt.Errorf("create %s session: %w", opt.name, err)
	}
	if opt.signV4 {
		sess.Handlers.Sign.PushBackNamed(v4.SignRequestHandler)
	}
	if opt.setUserAgent {
		sess.Handlers.Build.PushBack(func(r *request.Request) {
			r.HTTPRequest.Header.Set("User-Agent", "SeaweedFS/"+version.VERSION_NUMBER)
		})
	}
	sess.Handlers.Build.PushFront(skipSha256PayloadSigning)
	client.conn = s3.New(sess)
	return client, nil
}

var skipSha256PayloadSigning = func(r *request.Request) {
	// see https://github.com/ceph/ceph/pull/15965/files
	if r.ClientInfo.ServiceID != "S3" {
		return
	}
	if r.Operation.Name == "PutObject" || r.Operation.Name == "UploadPart" {
		if len(r.HTTPRequest.Header.Get("X-Amz-Content-Sha256")) == 0 {
			r.HTTPRequest.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
		}
	}
}

type s3RemoteStorageClient struct {
	conf *remote_pb.RemoteConf
	conn s3iface.S3API
}

var _ = remote_storage.RemoteStorageClient(&s3RemoteStorageClient{})

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
				remoteEntry := &filer_pb.RemoteEntry{
					StorageName: s.conf.Name,
				}
				if content.LastModified != nil {
					remoteEntry.RemoteMtime = content.LastModified.Unix()
				}
				if content.Size != nil {
					remoteEntry.RemoteSize = *content.Size
				}
				if content.ETag != nil {
					remoteEntry.RemoteETag = *content.ETag
				}
				if err := visitFn(dir, name, false, remoteEntry); err != nil {
					localErr = err
					return false
				}
			}
			listInput.ContinuationToken = page.NextContinuationToken
			isLastPage = lastPage
			return true
		})
		if listErr != nil {
			err = fmt.Errorf("list %v: %w", remote, listErr)
		}
		if localErr != nil {
			err = fmt.Errorf("process %v: %w", remote, localErr)
		}
	}
	return
}

func (s *s3RemoteStorageClient) ListDirectory(ctx context.Context, loc *remote_pb.RemoteStorageLocation, visitFn remote_storage.VisitFunc) error {
	pathKey := loc.Path[1:]
	if pathKey != "" && !strings.HasSuffix(pathKey, "/") {
		pathKey += "/"
	}

	listInput := &s3.ListObjectsV2Input{
		Bucket:    aws.String(loc.Bucket),
		Prefix:    aws.String(pathKey),
		Delimiter: aws.String("/"),
	}

	var localErr error
	listErr := s.conn.ListObjectsV2PagesWithContext(ctx, listInput, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, prefix := range page.CommonPrefixes {
			if prefix.Prefix == nil {
				continue
			}
			dirKey := "/" + strings.TrimSuffix(*prefix.Prefix, "/")
			dir, name := util.FullPath(dirKey).DirAndName()
			if err := visitFn(dir, name, true, nil); err != nil {
				localErr = err
				return false
			}
		}
		for _, content := range page.Contents {
			key := "/" + *content.Key
			if strings.HasSuffix(key, "/") {
				continue // skip directory markers
			}
			dir, name := util.FullPath(key).DirAndName()
			remoteEntry := &filer_pb.RemoteEntry{
				StorageName: s.conf.Name,
			}
			if content.LastModified != nil {
				remoteEntry.RemoteMtime = content.LastModified.Unix()
			}
			if content.Size != nil {
				remoteEntry.RemoteSize = *content.Size
			}
			if content.ETag != nil {
				remoteEntry.RemoteETag = *content.ETag
			}
			if err := visitFn(dir, name, false, remoteEntry); err != nil {
				localErr = err
				return false
			}
		}
		return true
	})
	if listErr != nil {
		return fmt.Errorf("list directory %v: %w", loc, listErr)
	}
	if localErr != nil {
		return fmt.Errorf("process directory %v: %w", loc, localErr)
	}
	return nil
}

func (s *s3RemoteStorageClient) StatFile(loc *remote_pb.RemoteStorageLocation) (remoteEntry *filer_pb.RemoteEntry, err error) {
	resp, err := s.conn.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(loc.Bucket),
		Key:    aws.String(loc.Path[1:]),
	})
	if err != nil {
		if reqErr, ok := err.(awserr.RequestFailure); ok && reqErr.StatusCode() == http.StatusNotFound {
			return nil, remote_storage.ErrRemoteObjectNotFound
		}
		return nil, fmt.Errorf("stat s3 %s%s: %w", loc.Bucket, loc.Path, err)
	}
	remoteEntry = &filer_pb.RemoteEntry{
		StorageName: s.conf.Name,
	}
	if resp.ContentLength != nil {
		remoteEntry.RemoteSize = *resp.ContentLength
	}
	if resp.LastModified != nil {
		remoteEntry.RemoteMtime = resp.LastModified.Unix()
	}
	if resp.ETag != nil {
		remoteEntry.RemoteETag = *resp.ETag
	}
	// a HeadObject response is authoritative: no header means no encoding
	remoteEntry.RemoteContentEncoding = aws.String(aws.StringValue(resp.ContentEncoding))
	return remoteEntry, nil
}

func (s *s3RemoteStorageClient) ReadFile(loc *remote_pb.RemoteStorageLocation, offset int64, size int64) (data []byte, err error) {
	return s.ReadFileWithConcurrency(loc, offset, size, 5)
}

func (s *s3RemoteStorageClient) ReadFileWithConcurrency(loc *remote_pb.RemoteStorageLocation, offset int64, size int64, concurrency int) (data []byte, err error) {
	if concurrency <= 0 {
		concurrency = 5
	}
	downloader := s3manager.NewDownloaderWithClient(s.conn, func(u *s3manager.Downloader) {
		u.PartSize = int64(4 * 1024 * 1024)
		u.Concurrency = concurrency
	})

	dataSlice := make([]byte, int(size))
	writerAt := aws.NewWriteAtBuffer(dataSlice)

	n, err := downloader.Download(writerAt, &s3.GetObjectInput{
		Bucket: aws.String(loc.Bucket),
		Key:    aws.String(loc.Path[1:]),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", offset, offset+size-1)),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to download file %s%s: %v", loc.Bucket, loc.Path, err)
	}
	// The buffer is pre-sized to size, so a short read leaves the tail
	// zero-padded and would be cached as valid-looking but corrupt content.
	// Reject it instead.
	if n != size {
		return nil, fmt.Errorf("short read from %s%s at offset %d: got %d bytes, want %d", loc.Bucket, loc.Path, offset, n, size)
	}

	return writerAt.Bytes(), nil
}

func (s *s3RemoteStorageClient) ReadFileAsStream(ctx context.Context, loc *remote_pb.RemoteStorageLocation, offset int64, size int64) (reader io.ReadCloser, err error) {
	output, err := s.conn.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(loc.Bucket),
		Key:    aws.String(loc.Path[1:]),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", offset, offset+size-1)),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == s3.ErrCodeNoSuchKey {
			return nil, remote_storage.ErrRemoteObjectNotFound
		}
		return nil, fmt.Errorf("failed to open stream for %s%s: %v", loc.Bucket, loc.Path, err)
	}
	return output.Body, nil
}

func (s *s3RemoteStorageClient) WriteDirectory(loc *remote_pb.RemoteStorageLocation, entry *filer_pb.Entry) (err error) {
	return nil
}

func (s *s3RemoteStorageClient) RemoveDirectory(loc *remote_pb.RemoteStorageLocation) (err error) {
	// the trailing slash keeps sibling prefixes that share the name intact
	prefix := loc.Path[1:]
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	if prefix == "" {
		// the mount root maps to the whole bucket; wiping every object from a
		// single namespace event is too destructive, so keep them
		glog.Warningf("s3 %s: skip removing directory mapped to the bucket root", loc.Bucket)
		return nil
	}

	listInput := &s3.ListObjectsV2Input{
		Bucket: aws.String(loc.Bucket),
		Prefix: aws.String(prefix),
	}
	var deleteErr error
	listErr := s.conn.ListObjectsV2Pages(listInput, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		var objects []*s3.ObjectIdentifier
		for _, content := range page.Contents {
			objects = append(objects, &s3.ObjectIdentifier{Key: content.Key})
		}
		if len(objects) == 0 {
			return true
		}
		// a listing page holds at most 1000 keys, the DeleteObjects limit
		resp, batchErr := s.conn.DeleteObjects(&s3.DeleteObjectsInput{
			Bucket: aws.String(loc.Bucket),
			Delete: &s3.Delete{
				Objects: objects,
				Quiet:   aws.Bool(true),
			},
		})
		if batchErr != nil {
			deleteErr = batchErr
			return false
		}
		if len(resp.Errors) > 0 {
			// a batch can fail 1000 keys; report the scope, not every key
			failed := resp.Errors[0]
			deleteErr = fmt.Errorf("%d keys failed, first is %s: %s %s", len(resp.Errors), aws.StringValue(failed.Key), aws.StringValue(failed.Code), aws.StringValue(failed.Message))
			return false
		}
		return true
	})
	if listErr != nil {
		return fmt.Errorf("list %s/%s: %w", loc.Bucket, prefix, listErr)
	}
	if deleteErr != nil {
		return fmt.Errorf("remove directory %s/%s: %w", loc.Bucket, prefix, deleteErr)
	}
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
	uploadInput := &s3manager.UploadInput{
		Bucket:  aws.String(loc.Bucket),
		Key:     aws.String(loc.Path[1:]),
		Body:    reader,
		Tagging: awsTags,
	}
	if entry.Attributes != nil && entry.Attributes.Mime != "" {
		uploadInput.ContentType = aws.String(entry.Attributes.Mime)
	}
	if contentEncoding := remote_storage.EntryContentEncoding(entry); contentEncoding != "" {
		uploadInput.ContentEncoding = aws.String(contentEncoding)
	}
	if s.conf.S3StorageClass != "" {
		uploadInput.StorageClass = aws.String(s.conf.S3StorageClass)
	}
	_, err = uploader.Upload(uploadInput)

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
	return s.StatFile(loc)
}

// the largest object a single CopyObject call accepts
const s3CopyObjectSizeLimit = 5 * 1024 * 1024 * 1024

func (s *s3RemoteStorageClient) UpdateFileMetadata(loc *remote_pb.RemoteStorageLocation, oldEntry *filer_pb.Entry, newEntry *filer_pb.Entry) (err error) {
	if reflect.DeepEqual(oldEntry.Extended, newEntry.Extended) {
		return nil
	}

	// Content-Encoding is S3 system metadata, changeable without a content
	// rewrite only through an in-place copy
	if encoding := remote_storage.EntryContentEncoding(newEntry); encoding != remote_storage.EntryContentEncoding(oldEntry) {
		key := loc.Path[1:]
		if fileSize := int64(filer.FileSize(newEntry)); fileSize > s3CopyObjectSizeLimit {
			glog.Warningf("s3 %s/%s: applying the Content-Encoding change needs an object copy, but %d bytes exceeds the copy limit; it will apply on the next content write", loc.Bucket, key, fileSize)
		} else {
			// the replace directive drops everything not resent, so read the
			// object's current metadata and carry it over
			headOut, headErr := s.conn.HeadObject(&s3.HeadObjectInput{
				Bucket: aws.String(loc.Bucket),
				Key:    aws.String(key),
			})
			if headErr != nil {
				return fmt.Errorf("stat %s/%s before metadata copy: %w", loc.Bucket, key, headErr)
			}
			copyInput := &s3.CopyObjectInput{
				Bucket:                  aws.String(loc.Bucket),
				Key:                     aws.String(key),
				CopySource:              aws.String(url.PathEscape(loc.Bucket + "/" + key)),
				MetadataDirective:       aws.String(s3.MetadataDirectiveReplace),
				Metadata:                headOut.Metadata,
				ContentType:             headOut.ContentType,
				CacheControl:            headOut.CacheControl,
				ContentDisposition:      headOut.ContentDisposition,
				ContentLanguage:         headOut.ContentLanguage,
				WebsiteRedirectLocation: headOut.WebsiteRedirectLocation,
				ServerSideEncryption:    headOut.ServerSideEncryption,
				SSEKMSKeyId:             headOut.SSEKMSKeyId,
				StorageClass:            headOut.StorageClass,
			}
			if headOut.Expires != nil {
				if expires, parseErr := http.ParseTime(*headOut.Expires); parseErr == nil {
					copyInput.Expires = aws.Time(expires)
				}
			}
			if encoding != "" {
				copyInput.ContentEncoding = aws.String(encoding)
			}
			if newEntry.Attributes != nil && newEntry.Attributes.Mime != "" {
				copyInput.ContentType = aws.String(newEntry.Attributes.Mime)
			}
			if s.conf.S3StorageClass != "" {
				copyInput.StorageClass = aws.String(s.conf.S3StorageClass)
			}
			if _, err = s.conn.CopyObject(copyInput); err != nil {
				return fmt.Errorf("update content encoding of %s/%s: %w", loc.Bucket, key, err)
			}
		}
	}

	// same as the write path: a remote without tagging support rejects both
	// PutObjectTagging and DeleteObjectTagging
	if !s.conf.S3SupportTagging {
		return
	}

	tagging := toTagging(newEntry.Extended)
	if len(tagging.TagSet) > 0 {
		_, err = s.conn.PutObjectTagging(&s3.PutObjectTaggingInput{
			Bucket:  aws.String(loc.Bucket),
			Key:     aws.String(loc.Path[1:]),
			Tagging: tagging,
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
		return nil, fmt.Errorf("list buckets: %w", err)
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
