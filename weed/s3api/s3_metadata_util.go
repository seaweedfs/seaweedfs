package s3api

import (
	"net/http"
	"net/url"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// ParseS3Metadata extracts S3-specific metadata from HTTP request headers
// This includes: storage class, tags, user metadata, SSE headers, and ACL headers
// Used by S3 API handlers to prepare metadata before saving to filer
// Returns an S3 error code if tag parsing fails
func ParseS3Metadata(r *http.Request, existing map[string][]byte, isReplace bool) (metadata map[string][]byte, errCode s3err.ErrorCode) {
	metadata = make(map[string][]byte)

	// Copy existing metadata unless replacing
	if !isReplace {
		for k, v := range existing {
			metadata[k] = v
		}
	}

	// Storage class
	if sc := r.Header.Get(s3_constants.AmzStorageClass); sc != "" {
		metadata[s3_constants.AmzStorageClass] = []byte(sc)
	}

	// Content-Encoding (standard HTTP header used by S3)
	if ce := r.Header.Get("Content-Encoding"); ce != "" {
		metadata["Content-Encoding"] = []byte(ce)
	}

	// Object tagging
	if tags := r.Header.Get(s3_constants.AmzObjectTagging); tags != "" {
		// Use url.ParseQuery for robust parsing and automatic URL decoding
		parsedTags, err := url.ParseQuery(tags)
		if err != nil {
			// Return proper S3 error instead of silently dropping tags
			glog.Warningf("Invalid S3 tag format in header '%s': %v", tags, err)
			return nil, s3err.ErrInvalidTag
		}
		for key, values := range parsedTags {
			// According to S3 spec, if a key is provided multiple times, the last value is used.
			// A tag value can be an empty string but not nil.
			value := ""
			if len(values) > 0 {
				value = values[len(values)-1]
			}
			metadata[s3_constants.AmzObjectTagging+"-"+key] = []byte(value)
		}
	}

	// User-defined metadata (x-amz-meta-* headers)
	for header, values := range r.Header {
		if strings.HasPrefix(header, s3_constants.AmzUserMetaPrefix) {
			// Go's HTTP server canonicalizes headers (e.g., x-amz-meta-foo → X-Amz-Meta-Foo)
			// We store them as they come in (after canonicalization) to preserve the user's intent
			for _, value := range values {
				metadata[header] = []byte(value)
			}
		}
	}

	// SSE-C headers
	if algorithm := r.Header.Get(s3_constants.AmzServerSideEncryptionCustomerAlgorithm); algorithm != "" {
		metadata[s3_constants.AmzServerSideEncryptionCustomerAlgorithm] = []byte(algorithm)
	}
	if keyMD5 := r.Header.Get(s3_constants.AmzServerSideEncryptionCustomerKeyMD5); keyMD5 != "" {
		// Store as-is; SSE-C MD5 is base64 and case-sensitive
		metadata[s3_constants.AmzServerSideEncryptionCustomerKeyMD5] = []byte(keyMD5)
	}

	// ACL owner
	acpOwner := r.Header.Get(s3_constants.ExtAmzOwnerKey)
	if len(acpOwner) > 0 {
		metadata[s3_constants.ExtAmzOwnerKey] = []byte(acpOwner)
	}

	// ACL grants
	acpGrants := r.Header.Get(s3_constants.ExtAmzAclKey)
	if len(acpGrants) > 0 {
		metadata[s3_constants.ExtAmzAclKey] = []byte(acpGrants)
	}

	return metadata, s3err.ErrNone
}
