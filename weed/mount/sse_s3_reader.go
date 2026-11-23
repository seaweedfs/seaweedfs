package mount

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

var errSSES3Permission = errors.New("S3 gateway denied object read")

// SSES3Reader makes signed range requests for one mounted bucket. The S3
// gateway owns the encryption keys and applies its normal GetObject policy.
type SSES3Reader struct {
	client *s3.Client
	root   string
	bucket string
}

func NewSSES3Reader(ctx context.Context, endpoint, bucketPath string) (*SSES3Reader, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("invalid S3 endpoint")
	}
	if u.Scheme != "https" && !(u.Scheme == "http" && (u.Hostname() == "localhost" || net.ParseIP(u.Hostname()).IsLoopback())) {
		return nil, errors.New("S3 endpoint requires HTTPS or a loopback HTTP address")
	}
	cfg, err := config.LoadDefaultConfig(ctx, config.WithDefaultRegion("us-east-1"))
	if err != nil {
		return nil, err
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
	return &SSES3Reader{client: client, root: bucketPath + "/", bucket: path.Base(bucketPath)}, nil
}

func sseObjectETag(entry *filer_pb.Entry) string {
	if stored := string(entry.GetExtended()[s3_constants.ExtETagKey]); stored != "" {
		return `"` + strings.Trim(stored, `"`) + `"`
	}
	return `"` + filer.ETag(entry) + `"`
}

func (r *SSES3Reader) readAt(ctx context.Context, fullPath, etag, version string, size int64, buf []byte, offset int64) (int64, error) {
	key, ok := strings.CutPrefix(fullPath, r.root)
	if !ok || key == "" || key == "." || key == ".." || path.IsAbs(key) || path.Clean(key) != key || strings.HasPrefix(key, "../") {
		return 0, errNativeSSES3Read
	}
	if dir, name := path.Split(key); version != "" && name == "v_"+version {
		if object, found := strings.CutSuffix(strings.TrimSuffix(dir, "/"), s3_constants.VersionsFolder); found && object != "" {
			key = object
		}
	}
	if offset < 0 || size < 0 {
		return 0, errors.New("invalid S3 object range")
	}
	if len(buf) == 0 {
		return 0, nil
	}
	if offset >= size {
		return 0, io.EOF
	}
	n := min(int64(len(buf)), size-offset)
	last := offset + n - 1
	req := &s3.GetObjectInput{
		Bucket: aws.String(r.bucket), Key: aws.String(key),
		IfMatch: aws.String(etag), Range: aws.String(fmt.Sprintf("bytes=%d-%d", offset, last)),
	}
	if version != "" {
		req.VersionId = aws.String(version)
	}
	obj, err := r.client.GetObject(ctx, req)
	if err != nil {
		var respErr *smithyhttp.ResponseError
		if errors.As(err, &respErr) && (respErr.HTTPStatusCode() == 403 || respErr.HTTPStatusCode() == 401) {
			return 0, errSSES3Permission
		}
		return 0, err
	}
	defer obj.Body.Close()
	wantRange := fmt.Sprintf("bytes %d-%d/%d", offset, last, size)
	if aws.ToString(obj.ContentRange) != wantRange || aws.ToInt64(obj.ContentLength) != n ||
		aws.ToString(obj.ETag) != etag || obj.ServerSideEncryption != types.ServerSideEncryptionAes256 ||
		(version != "" && aws.ToString(obj.VersionId) != version) {
		return 0, errors.New("S3 response did not match the encrypted object range")
	}
	if _, err := io.ReadFull(obj.Body, buf[:n]); err != nil {
		return 0, err
	}
	return n, nil
}
