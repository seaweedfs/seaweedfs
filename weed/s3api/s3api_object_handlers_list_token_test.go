package s3api

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type listV2TestPage struct {
	XMLName               xml.Name `xml:"ListBucketResult"`
	IsTruncated           bool     `xml:"IsTruncated"`
	NextContinuationToken string   `xml:"NextContinuationToken"`
	Contents              []struct {
		Key string `xml:"Key"`
	} `xml:"Contents"`
}

func listTokenTestFile(name string) *filer_pb.Entry {
	return &filer_pb.Entry{Name: name, Attributes: &filer_pb.FuseAttributes{}}
}

func newListTokenTestServer(t *testing.T, keys []string) *S3ApiServer {
	t.Helper()
	entries := make([]*filer_pb.Entry, 0, len(keys))
	for _, key := range keys {
		entries = append(entries, listTokenTestFile(key))
	}
	filer := &fakeBucketDeleteFiler{entriesByDir: map[string][]*filer_pb.Entry{
		"/buckets/test": entries,
	}}
	s3a := newFailoverTestServer(t, startFakeFiler(t, filer))
	s3a.option.BucketsPath = "/buckets"
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	s3a.bucketConfigCache.Set("test", &BucketConfig{Name: "test"})
	return s3a
}

func listV2Page(t *testing.T, s3a *S3ApiServer, query url.Values) listV2TestPage {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, "/test?"+query.Encode(), nil)
	r = mux.SetURLVars(r, map[string]string{"bucket": "test"})
	w := httptest.NewRecorder()
	s3a.ListObjectsV2Handler(w, r)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var page listV2TestPage
	require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &page), w.Body.String())
	return page
}

// A page that ends on a key XML 1.0 cannot carry must still resume right after
// that key. The token used to be the raw key, which the XML encoder turned into
// U+FFFD, so the next page resumed past the mangled name and skipped every key
// sorting between it and the real one.
func TestListObjectsV2ContinuationTokenWithXMLUnsafeKeys(t *testing.T) {
	keys := []string{"a\x01b", "a\x02b", "a\x1fb", "a b", "b", "c"}

	for _, encodingTypeURL := range []bool{false, true} {
		t.Run("encoding-type=url:"+strconv.FormatBool(encodingTypeURL), func(t *testing.T) {
			s3a := newListTokenTestServer(t, keys)

			var got []string
			token := ""
			for page := 0; ; page++ {
				require.Less(t, page, len(keys), "listing did not terminate")
				query := url.Values{"list-type": {"2"}, "max-keys": {"2"}}
				if encodingTypeURL {
					query.Set("encoding-type", "url")
				}
				if token != "" {
					query.Set("continuation-token", token)
				}
				result := listV2Page(t, s3a, query)
				for _, content := range result.Contents {
					key := content.Key
					if encodingTypeURL {
						decoded, err := url.QueryUnescape(key)
						require.NoError(t, err)
						key = decoded
					}
					got = append(got, key)
				}
				if !result.IsTruncated {
					break
				}
				require.NotEmpty(t, result.NextContinuationToken)
				token = result.NextContinuationToken
			}

			if encodingTypeURL {
				assert.Equal(t, keys, got)
				return
			}
			// Without encoding-type=url the keys themselves still lose control
			// characters to the XML encoder, but no key may be skipped.
			require.Len(t, got, len(keys))
			assert.Equal(t, "c", got[len(got)-1])
		})
	}
}

// A token for a key that XML carries unchanged stays the bare key.
func TestListObjectsV2ContinuationTokenUnchangedForNormalKeys(t *testing.T) {
	keys := []string{"a\x01b", "a\x02b", "a\x1fb", "a b", "b", "c"}
	s3a := newListTokenTestServer(t, keys)

	result := listV2Page(t, s3a, url.Values{"list-type": {"2"}, "max-keys": {"5"}})
	require.True(t, result.IsTruncated)
	assert.Equal(t, "b", result.NextContinuationToken)
}

func TestListObjectsV2ContinuationTokenRoundTrip(t *testing.T) {
	for _, key := range []string{"a\x01b", "a\x1fb", "a\xffb", "base64:literal", "dir/file", "a b", "b"} {
		token := encodeListContinuationToken(key)
		assert.Equal(t, key, decodeListContinuationToken(token), "token %q", token)
	}
	for _, key := range []string{"b", "dir/file", "a b"} {
		assert.Equal(t, key, encodeListContinuationToken(key))
	}
	// Tokens written before the encoding existed are still bare keys.
	assert.Equal(t, "dir/file", decodeListContinuationToken("dir/file"))
}
