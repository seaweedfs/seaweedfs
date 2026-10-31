package s3api

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// With encoding-type=url the upload keys have to be url-encoded, as ListObjects
// does; otherwise XML 1.0 cannot carry a key with a control character, the
// encoder turns it into U+FFFD, and no client can recover the key to abort the
// upload.
func TestSelectMultipartUploadsEncodingTypeURL(t *testing.T) {
	entries := []*filer_pb.Entry{
		uploadEntry("1", "m\x01p b+.bin", 10),
		uploadEntry("2", "plain/key.txt", 20),
	}
	input := listUploadsInput("m\x01", "m\x01", "", 1000)
	input.EncodingType = aws.String(s3.EncodingTypeUrl)
	out := selectMultipartUploads(input, entries)

	require.Len(t, out.Upload, 1)
	assert.Equal(t, "m%01p+b%2B.bin", *out.Upload[0].Key)
	decoded, err := url.QueryUnescape(*out.Upload[0].Key)
	require.NoError(t, err)
	assert.Equal(t, "m\x01p b+.bin", decoded)
	assert.Equal(t, "m%01", *out.Prefix)
	assert.Equal(t, "m%01", *out.KeyMarker)

	body, err := xml.Marshal(out)
	require.NoError(t, err)
	assert.NotContains(t, string(body), "\uFFFD")
	assert.Contains(t, string(body), "<Key>m%01p+b%2B.bin</Key>")
	assert.Contains(t, string(body), "<EncodingType>url</EncodingType>")
}

func TestSelectMultipartUploadsEncodingTypeURLNextKeyMarker(t *testing.T) {
	entries := []*filer_pb.Entry{
		uploadEntry("1", "a\x1fb", 10),
		uploadEntry("2", "c", 20),
	}
	input := listUploadsInput("", "", "", 1)
	input.EncodingType = aws.String(s3.EncodingTypeUrl)
	out := selectMultipartUploads(input, entries)

	require.True(t, *out.IsTruncated)
	assert.Equal(t, "a%1Fb", *out.NextKeyMarker)
	// The upload id is opaque, not a key name.
	assert.Equal(t, "1", *out.NextUploadIdMarker)

	body, err := xml.Marshal(out)
	require.NoError(t, err)
	assert.NotContains(t, string(body), "\uFFFD")
}

// Without encoding-type=url the response is unchanged, and the keys keep their
// raw form for clients that do not ask for encoding.
func TestSelectMultipartUploadsKeepsKeysRawWithoutEncodingType(t *testing.T) {
	entries := []*filer_pb.Entry{
		uploadEntry("1", "m\x01p b+.bin", 10),
		uploadEntry("2", "plain/key.txt", 20),
	}
	out := selectMultipartUploads(listUploadsInput("", "", "", 1000), entries)

	var got []string
	for _, upload := range out.Upload {
		got = append(got, *upload.Key)
	}
	assert.Equal(t, []string{"m\x01p b+.bin", "plain/key.txt"}, got)
}

type listUploadsTestPage struct {
	XMLName   xml.Name `xml:"ListMultipartUploadsResult"`
	KeyMarker string   `xml:"KeyMarker"`
	Uploads   []struct {
		Key string `xml:"Key"`
	} `xml:"Upload"`
}

// The request key-marker is a raw key even under encoding-type=url: SDKs
// decode the escaped response fields and send the key itself back, so a
// marker like "a%2Bb" is that literal key, not the escape of "a+b".
func TestListMultipartUploadsUrlKeyMarkerIsRawKey(t *testing.T) {
	filer := &fakeBucketDeleteFiler{entriesByDir: map[string][]*filer_pb.Entry{
		"/buckets/test/.uploads": {
			uploadEntry("1", "c d", 10),
			uploadEntry("2", "a+b", 10),
			uploadEntry("3", "a%2Bb", 10),
			uploadEntry("4", "z", 10),
			uploadEntry("5", "c%20d", 10),
		},
	}}
	s3a := newFailoverTestServer(t, startFakeFiler(t, filer))
	s3a.option.BucketsPath = "/buckets"
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	s3a.bucketConfigCache.Set("test", &BucketConfig{Name: "test"})
	s3a.iam = &IdentityAccessManagement{}

	r := httptest.NewRequest(http.MethodGet, "/test?uploads&"+url.Values{
		"encoding-type": {"url"},
		"key-marker":    {"a%2Bb"},
	}.Encode(), nil)
	r = mux.SetURLVars(r, map[string]string{"bucket": "test"})
	w := httptest.NewRecorder()
	s3a.ListMultipartUploadsHandler(w, r)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	var page listUploadsTestPage
	require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &page), w.Body.String())

	var got []string
	for _, upload := range page.Uploads {
		key, err := url.QueryUnescape(upload.Key)
		require.NoError(t, err)
		got = append(got, key)
	}
	assert.Equal(t, []string{"a+b", "c d", "c%20d", "z"}, got)

	decoded, err := url.QueryUnescape(page.KeyMarker)
	require.NoError(t, err)
	assert.Equal(t, "a%2Bb", decoded)
}
