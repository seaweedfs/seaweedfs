package s3api

import (
	"context"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Exercise the response builder as well as the directory walker: deferred
// directory resume markers can make the outer loop repeat prefixes.
// Derive the expected response from S3 keys, independently of filer traversal.
func TestListFilerEntriesListingRegression(t *testing.T) {
	keys := []string{
		"a", "a-c", "a.txt", "a/b", "a/c/d", "a0",
		"boo/bar", "boo/baz", "boo/boo", "boo/boo2",
		"foo_bar", "foo_bar_baz", "foo_baz", "logs/", "quux ab", "space dir/value", "z",
	}
	sort.Strings(keys)
	prefixObject := newDir("a")
	prefixObject.MarkPrefixObject()
	file := func(name string) *filer_pb.Entry {
		return &filer_pb.Entry{Name: name, Attributes: &filer_pb.FuseAttributes{}}
	}
	filer := &fakeBucketDeleteFiler{entriesByDir: map[string][]*filer_pb.Entry{
		"/buckets/test": {
			prefixObject, file("a-c"), file("a.txt"), file("a0"), newDir("boo"),
			file("foo_bar"), file("foo_bar_baz"), file("foo_baz"),
			directoryMarker("logs"), file("quux ab"), newDir("space dir"), file("z"),
		},
		"/buckets/test/a":         {file("b"), newDir("c")},
		"/buckets/test/a/c":       {file("d")},
		"/buckets/test/boo":       {file("bar"), file("baz"), file("boo"), file("boo2")},
		"/buckets/test/logs":      {},
		"/buckets/test/space dir": {file("value")},
	}}
	s3a := newFailoverTestServer(t, startFakeFiler(t, filer))
	s3a.option.BucketsPath = "/buckets"
	s3a.bucketConfigCache = NewBucketConfigCache(time.Minute)
	for _, versioning := range []string{"", "Enabled", "Suspended"} {
		t.Run("versioning="+versioning, func(t *testing.T) {
			s3a.bucketConfigCache.Set("test", &BucketConfig{Name: "test", Versioning: versioning})
			for _, encoded := range []bool{false, true} {
				t.Run(fmt.Sprintf("encoded=%t", encoded), func(t *testing.T) {
					testListingRegressionMatrix(t, s3a, keys, encoded)
				})
			}
		})
	}
}

func testListingRegressionMatrix(t *testing.T, s3a *S3ApiServer, keys []string, encoded bool) {
	t.Helper()
	for _, prefix := range []string{"", "a", "a/", "a/c/", "boo", "boo/", "boo/b", "foo", "foo_", "logs/", "space dir/", "missing/"} {
		for _, delimiter := range []string{"", "/", "_"} {
			for _, pageSize := range []uint16{1, 2, 1000} {
				t.Run(fmt.Sprintf("prefix=%s/delimiter=%s/max=%d", prefix, delimiter, pageSize), func(t *testing.T) {
					var wantKeys, wantPrefixes []string
					prefixes := map[string]bool{}
					for _, key := range keys {
						if !strings.HasPrefix(key, prefix) {
							continue
						}
						if delimiter != "" {
							if i := strings.Index(strings.TrimPrefix(key, prefix), delimiter); i >= 0 {
								prefixes[key[:len(prefix)+i+len(delimiter)]] = true
								continue
							}
						}
						wantKeys = append(wantKeys, key)
					}
					for p := range prefixes {
						wantPrefixes = append(wantPrefixes, p)
					}
					sort.Strings(wantKeys)
					sort.Strings(wantPrefixes)

					var gotKeys, gotPrefixes []string
					marker := ""
					for page := 0; ; page++ {
						require.Less(t, page, len(keys)+1, "listing did not terminate")
						response, err := s3a.listFilerEntries(context.Background(), listObjectsRequest{
							bucket: "test", prefix: prefix, delimiter: delimiter,
							marker:        adjustMarkerForDelimiter(marker, prefix, delimiter),
							requestMarker: marker, maxKeys: pageSize,
							encodingTypeUrl: encoded,
						})
						require.NoError(t, err)
						decode := func(value string) string {
							if !encoded {
								return value
							}
							decoded, err := url.QueryUnescape(value)
							require.NoError(t, err)
							return decoded
						}
						for _, entry := range response.Contents {
							gotKeys = append(gotKeys, decode(entry.Key))
						}
						for _, entry := range response.CommonPrefixes {
							gotPrefixes = append(gotPrefixes, decode(entry.Prefix))
						}
						if !response.IsTruncated {
							break
						}
						require.Greater(t, response.NextMarker, marker, "pagination must advance")
						marker = response.NextMarker
					}
					// Sorting retains duplicates, so repeated prefixes still fail.
					sort.Strings(gotKeys)
					sort.Strings(gotPrefixes)
					assert.Equal(t, wantKeys, gotKeys)
					assert.Equal(t, wantPrefixes, gotPrefixes)
				})
			}
		}
	}
}
