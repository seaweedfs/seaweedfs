package s3api

import (
	"net/url"
	"strings"
)

// contentLocationPairHeader is sent with every chunk upload. The volume server
// strips the "Seaweed-" prefix and keeps "Content-Location" in the needle's
// Pairs, so a needle can be traced back to the S3 object it belongs to.
const contentLocationPairHeader = "Seaweed-Content-Location"

// contentLocationPairMap returns the needle pairs that record filerPath, or nil
// when the path is unknown.
func contentLocationPairMap(filerPath string) map[string]string {
	if filerPath == "" {
		return nil
	}
	return map[string]string{contentLocationPairHeader: escapeFilerPath(filerPath)}
}

// escapeFilerPath percent-encodes each segment of a filer path, so keys with
// newlines, '%' or non-ASCII bytes stay valid in an HTTP header.
func escapeFilerPath(p string) string {
	segments := strings.Split(p, "/")
	for i, s := range segments {
		segments[i] = url.PathEscape(s)
	}
	return strings.Join(segments, "/")
}
