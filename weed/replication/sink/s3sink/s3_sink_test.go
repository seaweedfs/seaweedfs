package S3Sink

import (
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	awss3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/stretchr/testify/require"
)

func TestBuildTaggingString_ShouldStripTagPrefix(t *testing.T) {
	extended := map[string][]byte{
		s3_constants.AmzObjectTaggingPrefix + "env": []byte("production"),
	}

	tagging := buildTaggingString(extended)

	if strings.Contains(tagging, s3_constants.AmzObjectTaggingPrefix) {
		t.Errorf("tagging should not contain storage prefix %q, got %q", s3_constants.AmzObjectTaggingPrefix, tagging)
	}

	parsed, err := url.ParseQuery(tagging)
	if err != nil {
		t.Fatalf("tagging should be valid URL query: %v", err)
	}
	if v := parsed.Get("env"); v != "production" {
		t.Errorf("expected tag env=production, got %q", v)
	}
}

func TestBuildTaggingString_ShouldURLEncodeValues(t *testing.T) {
	extended := map[string][]byte{
		s3_constants.AmzObjectTaggingPrefix + "path": []byte("/a/b=c&d"),
	}

	tagging := buildTaggingString(extended)

	parsed, err := url.ParseQuery(tagging)
	if err != nil {
		t.Fatalf("tagging should be valid URL query: %v", err)
	}
	if v := parsed.Get("path"); v != "/a/b=c&d" {
		t.Errorf("expected tag value /a/b=c&d after decoding, got %q", v)
	}
}

// capturePutRoundTripper records the s3manager.Uploader's PUT and replies 200
// so the SDK is satisfied without making a real network call.
type capturePutRoundTripper struct {
	uploadReq *http.Request
}

func (c *capturePutRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
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
		Header:     http.Header{"ETag": []string{"\"etag\""}},
		Request:    req,
	}, nil
}

func newCapturingS3Sink(t *testing.T) (*S3Sink, *capturePutRoundTripper) {
	t.Helper()
	rt := &capturePutRoundTripper{}
	sess, err := session.NewSession(&aws.Config{
		Region:           aws.String("us-east-1"),
		Endpoint:         aws.String("https://example.invalid"),
		S3ForcePathStyle: aws.Bool(true),
		Credentials:      credentials.NewStaticCredentials("k", "s", ""),
		HTTPClient:       &http.Client{Transport: rt},
	})
	require.NoError(t, err)
	s := &S3Sink{
		conn:                awss3.New(sess),
		bucket:              "bucket",
		uploaderConcurrency: 1,
		uploaderPartSizeMb:  5,
	}
	return s, rt
}

func TestS3SinkCreateEntryPassesMimeAsContentType(t *testing.T) {
	sink, rt := newCapturingS3Sink(t)
	entry := &filer_pb.Entry{
		Attributes: &filer_pb.FuseAttributes{Mime: "text/html"},
		Content:    []byte("<html></html>"),
	}
	require.NoError(t, sink.CreateEntry("/dir/test.html", entry, nil))

	require.NotNil(t, rt.uploadReq, "uploader should have issued a PUT")
	require.Equal(t, "text/html", rt.uploadReq.Header.Get("Content-Type"))
}

func TestS3SinkCreateEntryOmitsContentTypeWhenMimeMissing(t *testing.T) {
	sink, rt := newCapturingS3Sink(t)
	entry := &filer_pb.Entry{
		Attributes: &filer_pb.FuseAttributes{},
		Content:    []byte("data"),
	}
	require.NoError(t, sink.CreateEntry("/dir/test.bin", entry, nil))

	require.NotNil(t, rt.uploadReq, "uploader should have issued a PUT")
	require.Equal(t, "", rt.uploadReq.Header.Get("Content-Type"))
}

func TestBuildTaggingString_EmptyWhenNoTags(t *testing.T) {
	extended := map[string][]byte{
		"Content-Encoding":             []byte("gzip"),
		s3_constants.AmzUserMetaMtime:  []byte("12345"),
		s3_constants.SeaweedFSSSES3Key: []byte(`{"algorithm":"AES256","encryptedDEK":"abc"}`),
	}

	tagging := buildTaggingString(extended)

	if tagging != "" {
		t.Errorf("expected empty tagging when no tag keys present, got %q", tagging)
	}
}

func TestS3SinkCreateEntryForwardsUserMetadata(t *testing.T) {
	sink, rt := newCapturingS3Sink(t)
	entry := &filer_pb.Entry{
		Attributes: &filer_pb.FuseAttributes{Mtime: 1700000000},
		Content:    []byte("data"),
		Extended: map[string][]byte{
			"X-Amz-Meta-Project":                        []byte("alpha"),
			"x-amz-meta-legacy-Key":                     []byte("old"),
			s3_constants.AmzObjectTaggingPrefix + "env": []byte("prod"),
			s3_constants.AmzStorageClass:                []byte("STANDARD_IA"),
			"Content-Encoding":                          []byte("gzip"),
		},
	}
	require.NoError(t, sink.CreateEntry("/dir/test.bin", entry, nil))

	require.NotNil(t, rt.uploadReq, "uploader should have issued a PUT")
	var metadata []string
	for k := range rt.uploadReq.Header {
		if strings.HasPrefix(k, s3_constants.AmzUserMetaPrefix) {
			metadata = append(metadata, k)
		}
	}
	require.ElementsMatch(t, []string{"X-Amz-Meta-Project", "X-Amz-Meta-Legacy-Key", "X-Amz-Meta-Mtime"}, metadata)
	require.Equal(t, "alpha", rt.uploadReq.Header.Get("X-Amz-Meta-Project"))
	require.Equal(t, "old", rt.uploadReq.Header.Get("X-Amz-Meta-Legacy-Key"))
	require.Equal(t, "1700000000", rt.uploadReq.Header.Get("X-Amz-Meta-Mtime"))
	require.Equal(t, "env=prod", rt.uploadReq.Header.Get("X-Amz-Tagging"))
}

func TestS3SinkCreateEntryLeavesEntryExtendedUnchanged(t *testing.T) {
	sink, rt := newCapturingS3Sink(t)
	entry := &filer_pb.Entry{
		Attributes: &filer_pb.FuseAttributes{Mtime: 1700000000},
		Content:    []byte("data"),
	}
	require.NoError(t, sink.CreateEntry("/dir/a.bin", entry, nil))
	require.Nil(t, entry.Extended)
	require.Equal(t, "1700000000", rt.uploadReq.Header.Get("X-Amz-Meta-Mtime"))

	entry.Extended = map[string][]byte{"X-Amz-Meta-Mtime": []byte("1600000000")}
	require.NoError(t, sink.CreateEntry("/dir/b.bin", entry, nil))
	require.Equal(t, map[string][]byte{"X-Amz-Meta-Mtime": []byte("1600000000")}, entry.Extended)
	require.Equal(t, "1600000000", rt.uploadReq.Header.Get("X-Amz-Meta-Mtime"))
}

func TestS3SinkCreateEntryEncodesNonASCIIMetadata(t *testing.T) {
	sink, rt := newCapturingS3Sink(t)
	entry := &filer_pb.Entry{
		Attributes: &filer_pb.FuseAttributes{},
		Content:    []byte("data"),
		Extended: map[string][]byte{
			"X-Amz-Meta-City":  []byte("Zürich"),
			"X-Amz-Meta-Note":  []byte("line1\nline2"),
			"X-Amz-Meta-Plain": []byte("=?utf-8?q?caf=C3=A9?="),
		},
	}
	require.NoError(t, sink.CreateEntry("/dir/test.bin", entry, nil))

	require.Equal(t, "=?utf-8?q?Z=C3=BCrich?=", rt.uploadReq.Header.Get("X-Amz-Meta-City"))
	require.Equal(t, "=?utf-8?q?line1=0Aline2?=", rt.uploadReq.Header.Get("X-Amz-Meta-Note"))
	require.Equal(t, "=?utf-8?q?caf=C3=A9?=", rt.uploadReq.Header.Get("X-Amz-Meta-Plain"))
}
