package s3api

import (
	"net/http"
	"net/url"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
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

// copiedChunkContentLocation returns the path a copy of chunk records. A chunk
// encrypted for the volume server records none: its ciphertext is copied as
// is, and cipher uploads send no pairs.
func copiedChunkContentLocation(chunk *filer_pb.FileChunk, dstPath string) string {
	if len(chunk.CipherKey) > 0 {
		return ""
	}
	return dstPath
}

// objectContentLocation returns the filer path of the object a write belongs
// to. Callers that write to an internal path (UploadPart writes under
// .uploads/) pass object == "", and the key is taken from the request route.
func (s3a *S3ApiServer) objectContentLocation(r *http.Request, bucket, object string) string {
	if object == "" {
		_, object = s3_constants.GetBucketAndObject(r)
	}
	if object == "" {
		return ""
	}
	return s3a.toFilerPath(bucket, object)
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
