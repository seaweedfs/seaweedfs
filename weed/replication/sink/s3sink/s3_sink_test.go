package S3Sink

import (
	"net/url"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
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

func TestBuildTaggingString_EmptyWhenNoTags(t *testing.T) {
	extended := map[string][]byte{
		"Content-Encoding":            []byte("gzip"),
		s3_constants.AmzUserMetaMtime: []byte("12345"),
		s3_constants.SeaweedFSSSES3Key: []byte(`{"algorithm":"AES256","encryptedDEK":"abc"}`),
	}

	tagging := buildTaggingString(extended)

	if tagging != "" {
		t.Errorf("expected empty tagging when no tag keys present, got %q", tagging)
	}
}
