package mount

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/url"
	"path"
	"slices"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/cluster"
	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
)

var (
	errSSES3Permission = errors.New("S3 gateway denied object read")
	errNoS3Gateway     = errors.New("no S3 gateway is registered with the masters")
)

// SSES3Reader makes signed range requests for one mounted bucket. The S3
// gateway owns the encryption keys and applies its normal GetObject policy.
type SSES3Reader struct {
	cfg       aws.Config
	endpoints func(context.Context) ([]string, error)
	root      string
	bucket    string

	clientLock sync.Mutex
	client     *s3.Client
	endpoint   string
}

// NewSSES3Reader reads through the S3 gateways the filer's masters list for
// its filer group.
func NewSSES3Reader(ctx context.Context, bucketPath string, filers []pb.ServerAddress, grpcDialOption grpc.DialOption) (*SSES3Reader, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithDefaultRegion("us-east-1"))
	if err != nil {
		return nil, err
	}
	return &SSES3Reader{
		cfg: cfg,
		endpoints: func(ctx context.Context) ([]string, error) {
			return listS3Endpoints(ctx, filers, grpcDialOption)
		},
		root:   bucketPath + "/",
		bucket: path.Base(bucketPath),
	}, nil
}

func listS3Endpoints(ctx context.Context, filers []pb.ServerAddress, grpcDialOption grpc.DialOption) ([]string, error) {
	var masters []string
	var filerGroup string
	if err := pb.WithOneOfGrpcFilerClients(false, filers, grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := client.GetFilerConfiguration(ctx, &filer_pb.GetFilerConfigurationRequest{})
		if err != nil {
			return err
		}
		masters, filerGroup = resp.Masters, resp.FilerGroup
		return nil
	}); err != nil {
		return nil, err
	}
	var endpoints []string
	var err error
	// Only the leader lists the gateways, so a follower answers with none.
	for _, master := range masters {
		err = pb.WithMasterClient(ctx, false, pb.ServerAddress(master), grpcDialOption, false, func(client master_pb.SeaweedClient) error {
			resp, err := client.ListClusterNodes(ctx, &master_pb.ListClusterNodesRequest{ClientType: cluster.S3Type, FilerGroup: filerGroup})
			if err != nil {
				return err
			}
			for _, node := range resp.ClusterNodes {
				if node.S3Endpoint != "" {
					endpoints = append(endpoints, node.S3Endpoint)
				}
			}
			return nil
		})
		if len(endpoints) > 0 {
			return endpoints, nil
		}
	}
	if err != nil {
		return nil, err
	}
	return nil, errNoS3Gateway
}

// gatewayClient picks one of the registered gateways other than failed, using
// HTTPS whenever an HTTPS gateway is registered, and keeps it until a request
// cannot reach it.
func (r *SSES3Reader) gatewayClient(ctx context.Context, failed string) (*s3.Client, string, error) {
	r.clientLock.Lock()
	defer r.clientLock.Unlock()
	if r.client != nil && r.endpoint != failed {
		return r.client, r.endpoint, nil
	}
	endpoints, err := r.endpoints(ctx)
	if err != nil {
		return nil, "", err
	}
	endpoints = slices.DeleteFunc(endpoints, func(endpoint string) bool {
		u, err := url.Parse(endpoint)
		return err != nil || u.Scheme != "https" && u.Scheme != "http" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != ""
	})
	isHTTPS := func(endpoint string) bool {
		u, err := url.Parse(endpoint)
		return err == nil && u.Scheme == "https"
	}
	if https := slices.DeleteFunc(slices.Clone(endpoints), func(endpoint string) bool { return !isHTTPS(endpoint) }); len(https) > 0 {
		endpoints = https
	}
	endpoints = slices.DeleteFunc(endpoints, func(endpoint string) bool { return endpoint == failed })
	if len(endpoints) == 0 {
		return nil, "", errNoS3Gateway
	}
	endpoint := endpoints[rand.IntN(len(endpoints))]
	if !isHTTPS(endpoint) {
		glog.Warningf("-sseS3Read: no HTTPS S3 gateway is registered, reading %s over plain HTTP", endpoint)
	}
	r.client = s3.NewFromConfig(r.cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
	r.endpoint = endpoint
	return r.client, endpoint, nil
}

// getObject retries once on another gateway when the current one cannot be
// reached.
func (r *SSES3Reader) getObject(ctx context.Context, req *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	client, endpoint, err := r.gatewayClient(ctx, "")
	if err != nil {
		return nil, err
	}
	obj, err := client.GetObject(ctx, req)
	var sendErr *smithyhttp.RequestSendError
	if !errors.As(err, &sendErr) || ctx.Err() != nil {
		return obj, err
	}
	r.clientLock.Lock()
	if r.client == client {
		r.client = nil
	}
	r.clientLock.Unlock()
	retry, _, pickErr := r.gatewayClient(ctx, endpoint)
	if pickErr != nil {
		return nil, err
	}
	return retry.GetObject(ctx, req)
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
	versionFile := false
	if dir, name := path.Split(key); version != "" && name == "v_"+version {
		if object, found := strings.CutSuffix(strings.TrimSuffix(dir, "/"), s3_constants.VersionsFolder); found && object != "" {
			key, versionFile = object, true
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
	currentNull := version == "null" && !versionFile
	if version != "" && !currentNull {
		req.VersionId = aws.String(version)
	}
	obj, err := r.getObject(ctx, req)
	if currentNull && (err == nil && aws.ToString(obj.VersionId) != "" && aws.ToString(obj.VersionId) != version || err != nil && !isS3PermissionError(err)) {
		if err == nil {
			obj.Body.Close()
		}
		req.VersionId = aws.String(version)
		obj, err = r.getObject(ctx, req)
	}
	if err != nil {
		if isS3PermissionError(err) {
			return 0, errSSES3Permission
		}
		return 0, err
	}
	defer obj.Body.Close()
	wantRange := fmt.Sprintf("bytes %d-%d/%d", offset, last, size)
	if aws.ToString(obj.ContentRange) != wantRange || aws.ToInt64(obj.ContentLength) != n ||
		aws.ToString(obj.ETag) != etag || obj.ServerSideEncryption != types.ServerSideEncryptionAes256 ||
		(version != "" && aws.ToString(obj.VersionId) != version && (req.VersionId != nil || aws.ToString(obj.VersionId) != "")) {
		return 0, errors.New("S3 response did not match the encrypted object range")
	}
	if _, err := io.ReadFull(obj.Body, buf[:n]); err != nil {
		return 0, err
	}
	return n, nil
}

func isS3PermissionError(err error) bool {
	var respErr *smithyhttp.ResponseError
	return errors.As(err, &respErr) && (respErr.HTTPStatusCode() == 403 || respErr.HTTPStatusCode() == 401)
}
