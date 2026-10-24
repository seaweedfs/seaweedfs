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
	StartAfter            string   `xml:"StartAfter"`
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

// The StartAfter echo is the wire encoding of the decoded key, so a client
// that feeds the echo back lands on the same resume point instead of
// drifting to the literal escape text.
func TestListObjectsV2StartAfterEchoStable(t *testing.T) {
	s3a := newListTokenTestServer(t, []string{"a b", "c"})
	for _, sent := range []string{"a%20b", "a b"} {
		result := listV2Page(t, s3a, url.Values{
			"list-type": {"2"}, "encoding-type": {"url"}, "start-after": {sent},
		})
		assert.Equal(t, "a%20b", result.StartAfter, "start-after %q", sent)
		require.Len(t, result.Contents, 1)
		assert.Equal(t, "c", result.Contents[0].Key)
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

func TestDecodeListContinuationTokenKeepsForeignTokensRaw(t *testing.T) {
	// A prefix-looking token that does not decode is a key, not a token.
	assert.Equal(t, "base64:%%%", decodeListContinuationToken("base64:%%%"))
	// An empty payload is never something encodeListContinuationToken wrote.
	assert.Equal(t, "base64:", decodeListContinuationToken("base64:"))
	// A legacy bare token is a raw key even when its suffix is valid base64:
	// the decoded key could have traveled bare, so the input never was an
	// encoded token.
	for _, legacy := range []string{"base64:Yw", "base64:YQ", "base64:cGFy", "base64:a2V5LXdpdGg"} {
		assert.Equal(t, legacy, decodeListContinuationToken(legacy), "token %q", legacy)
	}
}

// Some tokens are ambiguous: "base64:AQ" is both a possible legacy bare key
// and the encoded form of "\x01". The encoded reading wins because a decoded
// key that cannot travel bare is a shape only encodeListContinuationToken
// writes — and a wrong raw reading skips entries, while a wrong encoded
// reading only re-lists them.
func TestDecodeListContinuationTokenResolvesCollisionEncoded(t *testing.T) {
	assert.Equal(t, "\x01", decodeListContinuationToken("base64:AQ"))
	assert.Equal(t, "\xff", decodeListContinuationToken("base64:_w"))
	assert.Equal(t, "base64:AQ", decodeListContinuationToken("base64:YmFzZTY0OkFR"))
}

type listV1TestPage struct {
	XMLName     xml.Name `xml:"ListBucketResult"`
	IsTruncated bool     `xml:"IsTruncated"`
	Marker      string   `xml:"Marker"`
	NextMarker  string   `xml:"NextMarker"`
	Contents    []struct {
		Key string `xml:"Key"`
	} `xml:"Contents"`
}

func listV1Page(t *testing.T, s3a *S3ApiServer, query url.Values) listV1TestPage {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, "/test?"+query.Encode(), nil)
	r = mux.SetURLVars(r, map[string]string{"bucket": "test"})
	w := httptest.NewRecorder()
	s3a.ListObjectsV1Handler(w, r)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	var page listV1TestPage
	require.NoError(t, xml.Unmarshal(w.Body.Bytes(), &page), w.Body.String())
	return page
}

// The V1 marker carries the same resume position as the V2 token, so it gets
// the same treatment: a page ending on an XML-unsafe key must still resume
// right after it.
func TestListObjectsV1MarkerWithXMLUnsafeKeys(t *testing.T) {
	keys := []string{"a\x01b", "a\x02b", "a\x1fb", "a b", "b", "c"}
	s3a := newListTokenTestServer(t, keys)

	var got []string
	marker := ""
	for page := 0; ; page++ {
		require.Less(t, page, len(keys), "listing did not terminate")
		query := url.Values{"max-keys": {"2"}, "encoding-type": {"url"}}
		if marker != "" {
			query.Set("marker", marker)
		}
		result := listV1Page(t, s3a, query)
		for _, content := range result.Contents {
			key, err := url.QueryUnescape(content.Key)
			require.NoError(t, err)
			got = append(got, key)
		}
		if !result.IsTruncated {
			break
		}
		require.NotEmpty(t, result.NextMarker)
		marker = result.NextMarker
	}
	require.Equal(t, keys, got)
}

// A url-encoded marker is only a wire echo when re-escaping its unescaped
// form reproduces it: a real echo decodes, a raw key merely containing '%'
// or a malformed escape passes through untouched.
func TestDecodeMarkerEchoKeepsNonCanonicalRawKey(t *testing.T) {
	assert.Equal(t, "a b", decodeMarkerEcho("a%20b", true))
	assert.Equal(t, "a%41", decodeMarkerEcho("a%2541", true))
	assert.Equal(t, "a%41", decodeMarkerEcho("a%41", true))
	assert.Equal(t, "a%2G", decodeMarkerEcho("a%2G", true))
	assert.Equal(t, "a%20b", decodeMarkerEcho("a%20b", false))
}

// Under encoding-type=url AWS encodes the StartAfter echo the same as the
// listed keys; a client resuming from the emitted form lands on the raw key.
func TestListObjectsV2UrlStartAfterEchoAndResume(t *testing.T) {
	s3a := newListTokenTestServer(t, []string{"a b", "a b0", "a c", "b"})

	result := listV2Page(t, s3a, url.Values{"list-type": {"2"}, "encoding-type": {"url"}, "start-after": {"a b"}})
	assert.Equal(t, urlPathEscape("a b"), result.StartAfter)

	var got []string
	for _, content := range result.Contents {
		key, err := url.QueryUnescape(content.Key)
		require.NoError(t, err)
		got = append(got, key)
	}
	assert.Equal(t, []string{"a b0", "a c", "b"}, got)

	// Echoing the emitted form back resumes at the same raw key.
	result = listV2Page(t, s3a, url.Values{"list-type": {"2"}, "encoding-type": {"url"}, "start-after": {result.StartAfter}})
	got = nil
	for _, content := range result.Contents {
		key, err := url.QueryUnescape(content.Key)
		require.NoError(t, err)
		got = append(got, key)
	}
	assert.Equal(t, []string{"a b0", "a c", "b"}, got)
}

// A url-encoded marker that is not the canonical urlPathEscape form of any key
// is the client's raw key, not an encoded echo: "a%41" must resume after that
// literal key instead of decoding to "aA" and skipping the keys between them.
func TestListObjectsV1UrlMarkerKeepsNonCanonicalRawKey(t *testing.T) {
	s3a := newListTokenTestServer(t, []string{"a%41", "a%42", "aA", "b"})

	result := listV1Page(t, s3a, url.Values{"encoding-type": {"url"}, "marker": {"a%41"}})
	var got []string
	for _, content := range result.Contents {
		key, err := url.QueryUnescape(content.Key)
		require.NoError(t, err)
		got = append(got, key)
	}
	assert.Equal(t, []string{"a%42", "aA", "b"}, got)
}

// The echoed request marker carries the client's token, not the decoded key:
// echoing the raw key would mangle an XML-unsafe name to U+FFFD, and under
// encoding-type=url the echo takes the same escaped form as the listed keys.
func TestListObjectsV1MarkerEchoStaysXMLSafe(t *testing.T) {
	s3a := newListTokenTestServer(t, []string{"a\x01b", "b", "c"})

	token := encodeListContinuationToken("a\x01b")
	result := listV1Page(t, s3a, url.Values{"max-keys": {"2"}, "marker": {token}})
	assert.Equal(t, token, result.Marker)

	result = listV1Page(t, s3a, url.Values{"max-keys": {"2"}, "marker": {"a b"}, "encoding-type": {"url"}})
	assert.Equal(t, urlPathEscape("a b"), result.Marker)
}

// A raw-key token resuming at an XML-unsafe prefix must not lose keys when a
// sibling directory shares the key's leading bytes: the start window begins at
// the marker's first path segment and descends only into blocks that can still
// hold keys past it.
func TestListObjectsV2ResumeXMLUnsafePrefixWithSiblingDir(t *testing.T) {
	filer := &fakeBucketDeleteFiler{entriesByDir: map[string][]*filer_pb.Entry{
		"/buckets/test/x": {
			{Name: "a\x01", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{}},
			listTokenTestFile("a\x01b"),
			{Name: "a\x01b", IsDirectory: true, Attributes: &filer_pb.FuseAttributes{}},
			listTokenTestFile("a\x01b0"),
		},
		"/buckets/test/x/a\x01": {
			listTokenTestFile("b"),
		},
		"/buckets/test/x/a\x01b": {
			listTokenTestFile("0"),
		},
	}}
	s3a := newFailoverTestServer(t, startFakeFiler(t, filer))
	s3a.option.BucketsPath = "/buckets"
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	s3a.bucketConfigCache.Set("test", &BucketConfig{Name: "test"})

	result := listV2Page(t, s3a, url.Values{
		"list-type":          {"2"},
		"encoding-type":      {"url"},
		"prefix":             {"x/a\x01b"},
		"max-keys":           {"1"},
		"continuation-token": {"x/a\x01b"},
	})
	require.Len(t, result.Contents, 1)
	got, err := url.QueryUnescape(result.Contents[0].Key)
	require.NoError(t, err)
	assert.Equal(t, "x/a\x01b/0", got)
	assert.True(t, result.IsTruncated)
}
