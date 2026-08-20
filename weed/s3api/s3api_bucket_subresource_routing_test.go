package s3api

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestUnroutedBucketSubresource guards the rule that keeps a bucket GET carrying an
// unimplemented subresource from being answered with a bucket listing: everything
// SeaweedFS implements is matched by its own route before the ListObjects catch-all,
// so anything left over that is not a listing parameter is a subresource.
func TestUnroutedBucketSubresource(t *testing.T) {
	for _, query := range []string{
		"",
		"prefix=a&max-keys=10",
		"list-type=2&continuation-token=x",
		"delimiter=/&encoding-type=url",
		"x-id=ListObjectsV2",
		// The listing handlers read allow-unordered and validate it against
		// delimiter, so the guard has to let it through to them.
		"allow-unordered=true",
		"allow-unordered=true&max-keys=1000",
		"list-type=2&allow-unordered=true",
		"prefix=a&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Signature=deadbeef&X-Amz-Expires=900",
		"AWSAccessKeyId=key&Signature=sig&Expires=1700000000",
	} {
		req, _ := http.NewRequest("GET", "http://localhost/bucket?"+query, nil)
		name, found := unroutedBucketSubresource(req)
		assert.False(t, found, "%q is a listing request, got %q", query, name)
	}

	for _, query := range []string{"torrent=", "replication=", "prefix=a&torrent="} {
		req, _ := http.NewRequest("GET", "http://localhost/bucket?"+query, nil)
		_, found := unroutedBucketSubresource(req)
		assert.True(t, found, "%q should be reported as a subresource", query)
	}
}
