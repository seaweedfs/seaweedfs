package s3

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws/credentials"
	awss3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/remote_pb"
	"github.com/seaweedfs/seaweedfs/weed/remote_storage"
	"github.com/stretchr/testify/require"
)

func TestS3MakeUsesAnonymousCredentialsWhenKeysAreEmpty(t *testing.T) {
	maker := s3RemoteStorageMaker{}
	conf := &remote_pb.RemoteConf{
		Type:             "s3",
		S3Region:         "us-east-1",
		S3Endpoint:       "http://localhost:8333",
		S3ForcePathStyle: true,
	}

	remoteClient, err := maker.Make(conf)
	require.NoError(t, err)

	client, ok := remoteClient.(*s3RemoteStorageClient)
	require.True(t, ok)

	s3Client, ok := client.conn.(*awss3.S3)
	require.True(t, ok)
	require.Same(t, credentials.AnonymousCredentials, s3Client.Config.Credentials)
}

func TestS3MakeUsesStaticCredentialsWhenKeysAreProvided(t *testing.T) {
	maker := s3RemoteStorageMaker{}
	conf := &remote_pb.RemoteConf{
		Type:             "s3",
		S3Region:         "us-east-1",
		S3Endpoint:       "http://localhost:8333",
		S3ForcePathStyle: true,
		S3AccessKey:      "test-access",
		S3SecretKey:      "test-secret",
	}

	remoteClient, err := maker.Make(conf)
	require.NoError(t, err)

	client, ok := remoteClient.(*s3RemoteStorageClient)
	require.True(t, ok)

	s3Client, ok := client.conn.(*awss3.S3)
	require.True(t, ok)
	require.NotSame(t, credentials.AnonymousCredentials, s3Client.Config.Credentials)

	credValue, err := s3Client.Config.Credentials.Get()
	require.NoError(t, err)
	require.Equal(t, conf.S3AccessKey, credValue.AccessKeyID)
	require.Equal(t, conf.S3SecretKey, credValue.SecretAccessKey)
}

func TestS3RemoteStorageClientImplementsInterface(t *testing.T) {
	var _ remote_storage.RemoteStorageClient = (*s3RemoteStorageClient)(nil)
}

func TestS3ErrRemoteObjectNotFoundIsAccessible(t *testing.T) {
	require.Error(t, remote_storage.ErrRemoteObjectNotFound)
	require.Equal(t, "remote object not found", remote_storage.ErrRemoteObjectNotFound.Error())
}

// captureRoundTripper records the PUT request that the s3manager uploader
// sends, and short-circuits all calls with a 200 so the SDK is satisfied.
type captureRoundTripper struct {
	uploadReq *http.Request
}

func (c *captureRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method == http.MethodPut {
		c.uploadReq = req.Clone(req.Context())
	}
	if req.Body != nil {
		_, _ = io.Copy(io.Discard, req.Body)
		_ = req.Body.Close()
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader("")),
		Header: http.Header{
			"ETag": []string{"\"etag\""},
		},
		Request: req,
	}, nil
}

func (c *captureRoundTripper) uploadContentType() string {
	if c.uploadReq == nil {
		return ""
	}
	return c.uploadReq.Header.Get("Content-Type")
}

func newCapturingS3Client(t *testing.T) (*s3RemoteStorageClient, *captureRoundTripper) {
	t.Helper()
	rt := &captureRoundTripper{}
	conf := &remote_pb.RemoteConf{
		Name:             "test",
		S3Region:         "us-east-1",
		S3Endpoint:       "https://example.invalid",
		S3ForcePathStyle: true,
		S3AccessKey:      "test-key",
		S3SecretKey:      "test-secret",
	}
	httpClient := &http.Client{Transport: rt}
	rs, err := MakeWithHTTPClient(conf, httpClient)
	require.NoError(t, err)
	return rs.(*s3RemoteStorageClient), rt
}

func TestS3WriteFilePassesMimeAsContentType(t *testing.T) {
	client, rt := newCapturingS3Client(t)
	loc := &remote_pb.RemoteStorageLocation{
		Name:   "test",
		Bucket: "bucket",
		Path:   "/dir/test.html",
	}
	entry := &filer_pb.Entry{
		Attributes: &filer_pb.FuseAttributes{Mime: "text/html"},
	}

	_, _ = client.WriteFile(loc, entry, bytes.NewReader([]byte("<html></html>")))

	require.NotNil(t, rt.uploadReq, "uploader should have issued a PUT")
	require.Equal(t, "text/html", rt.uploadContentType(), "Content-Type should match entry.Attributes.Mime")
}

func TestS3WriteFileOmitsContentTypeWhenMimeMissing(t *testing.T) {
	client, rt := newCapturingS3Client(t)
	loc := &remote_pb.RemoteStorageLocation{
		Name:   "test",
		Bucket: "bucket",
		Path:   "/dir/test.bin",
	}
	entry := &filer_pb.Entry{
		Attributes: &filer_pb.FuseAttributes{},
	}

	_, _ = client.WriteFile(loc, entry, bytes.NewReader([]byte("data")))

	require.NotNil(t, rt.uploadReq, "uploader should have issued a PUT")
	// When entry.Attributes.Mime is empty we don't force a Content-Type so the
	// remote can apply its own default rather than getting a misleading one.
	require.Equal(t, "", rt.uploadContentType())
}
