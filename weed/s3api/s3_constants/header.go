/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package s3_constants

import (
	"context"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
)

// S3 XML namespace
const (
	S3Namespace = "http://s3.amazonaws.com/doc/2006-03-01/"
)

// Standard S3 HTTP request constants
const (
	// S3 storage class
	AmzStorageClass = "x-amz-storage-class"

	// S3 user-defined metadata
	AmzUserMetaPrefix    = "X-Amz-Meta-"
	AmzUserMetaDirective = "X-Amz-Metadata-Directive"
	AmzUserMetaMtime     = "X-Amz-Meta-Mtime"

	// S3 object tagging
	AmzObjectTagging          = "X-Amz-Tagging"
	AmzObjectTaggingPrefix    = "X-Amz-Tagging-"
	AmzObjectTaggingDirective = "X-Amz-Tagging-Directive"
	AmzTagCount               = "x-amz-tagging-count"

	SeaweedFSUploadId                = "X-Seaweedfs-Upload-Id"
	SeaweedFSMultipartPartsCount     = "X-Seaweedfs-Multipart-Parts-Count"
	SeaweedFSMultipartPartBoundaries = "X-Seaweedfs-Multipart-Part-Boundaries" // JSON: [{part:1,start:0,end:2,etag:"abc"},{part:2,start:2,end:3,etag:"def"}]
	SeaweedFSExpiresS3               = "X-Seaweedfs-Expires-S3"
	AmzMpPartsCount                  = "x-amz-mp-parts-count"

	// S3 ACL headers
	AmzCannedAcl      = "X-Amz-Acl"
	AmzAclFullControl = "X-Amz-Grant-Full-Control"
	AmzAclRead        = "X-Amz-Grant-Read"
	AmzAclWrite       = "X-Amz-Grant-Write"
	AmzAclReadAcp     = "X-Amz-Grant-Read-Acp"
	AmzAclWriteAcp    = "X-Amz-Grant-Write-Acp"

	// S3 Object Lock headers
	AmzBucketObjectLockEnabled   = "X-Amz-Bucket-Object-Lock-Enabled"
	AmzObjectLockMode            = "X-Amz-Object-Lock-Mode"
	AmzObjectLockRetainUntilDate = "X-Amz-Object-Lock-Retain-Until-Date"
	AmzObjectLockLegalHold       = "X-Amz-Object-Lock-Legal-Hold"

	// S3 conditional headers
	IfMatch           = "If-Match"
	IfNoneMatch       = "If-None-Match"
	IfModifiedSince   = "If-Modified-Since"
	IfUnmodifiedSince = "If-Unmodified-Since"

	// S3 conditional copy headers
	AmzCopySourceIfMatch           = "X-Amz-Copy-Source-If-Match"
	AmzCopySourceIfNoneMatch       = "X-Amz-Copy-Source-If-None-Match"
	AmzCopySourceIfModifiedSince   = "X-Amz-Copy-Source-If-Modified-Since"
	AmzCopySourceIfUnmodifiedSince = "X-Amz-Copy-Source-If-Unmodified-Since"

	// S3 Server-Side Encryption with Customer-provided Keys (SSE-C)
	AmzServerSideEncryptionCustomerAlgorithm = "X-Amz-Server-Side-Encryption-Customer-Algorithm"
	AmzServerSideEncryptionCustomerKey       = "X-Amz-Server-Side-Encryption-Customer-Key"
	AmzServerSideEncryptionCustomerKeyMD5    = "X-Amz-Server-Side-Encryption-Customer-Key-MD5"
	AmzServerSideEncryptionContext           = "X-Amz-Server-Side-Encryption-Context"

	// S3 Server-Side Encryption with KMS (SSE-KMS)
	AmzServerSideEncryption                 = "X-Amz-Server-Side-Encryption"
	AmzServerSideEncryptionAwsKmsKeyId      = "X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id"
	AmzServerSideEncryptionBucketKeyEnabled = "X-Amz-Server-Side-Encryption-Bucket-Key-Enabled"

	// S3 SSE-C copy source headers
	AmzCopySourceServerSideEncryptionCustomerAlgorithm = "X-Amz-Copy-Source-Server-Side-Encryption-Customer-Algorithm"
	AmzCopySourceServerSideEncryptionCustomerKey       = "X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key"
	AmzCopySourceServerSideEncryptionCustomerKeyMD5    = "X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key-MD5"
)

// Metadata keys for internal storage
const (
	// SSE-KMS metadata keys
	AmzEncryptedDataKey      = "x-amz-encrypted-data-key"
	AmzEncryptionContextMeta = "x-amz-encryption-context"

	// SeaweedFS internal metadata prefix (used to filter internal headers from client responses)
	SeaweedFSInternalPrefix = "x-seaweedfs-"

	// SeaweedFS internal metadata keys for encryption (prefixed to avoid automatic HTTP header conversion)
	SeaweedFSSSEKMSKey = "x-seaweedfs-sse-kms-key" // Key for storing serialized SSE-KMS metadata
	SeaweedFSSSES3Key  = "x-seaweedfs-sse-s3-key"  // Key for storing serialized SSE-S3 metadata
	SeaweedFSSSEIV     = "x-seaweedfs-sse-c-iv"    // Key for storing SSE-C IV

	// Multipart upload metadata keys for SSE-KMS (consistent with internal metadata key pattern)
	SeaweedFSSSEKMSKeyID             = "x-seaweedfs-sse-kms-key-id"             // Key ID for multipart upload SSE-KMS inheritance
	SeaweedFSSSEKMSEncryption        = "x-seaweedfs-sse-kms-encryption"         // Encryption type for multipart upload SSE-KMS inheritance
	SeaweedFSSSEKMSBucketKeyEnabled  = "x-seaweedfs-sse-kms-bucket-key-enabled" // Bucket key setting for multipart upload SSE-KMS inheritance
	SeaweedFSSSEKMSEncryptionContext = "x-seaweedfs-sse-kms-encryption-context" // Encryption context for multipart upload SSE-KMS inheritance
	SeaweedFSSSEKMSBaseIV            = "x-seaweedfs-sse-kms-base-iv"            // Base IV for multipart upload SSE-KMS (for IV offset calculation)

	// Multipart upload metadata keys for SSE-S3
	SeaweedFSSSES3Encryption = "x-seaweedfs-sse-s3-encryption" // Encryption type for multipart upload SSE-S3 inheritance
	SeaweedFSSSES3BaseIV     = "x-seaweedfs-sse-s3-base-iv"    // Base IV for multipart upload SSE-S3 (for IV offset calculation)
	SeaweedFSSSES3KeyData    = "x-seaweedfs-sse-s3-key-data"   // Encrypted key data for multipart upload SSE-S3 inheritance
)

// SeaweedFS internal headers for filer communication
const (
	SeaweedFSSSEKMSKeyHeader    = "X-SeaweedFS-SSE-KMS-Key"     // Header for passing SSE-KMS metadata to filer
	SeaweedFSSSEIVHeader        = "X-SeaweedFS-SSE-IV"          // Header for passing SSE-C IV to filer (SSE-C only)
	SeaweedFSSSEKMSBaseIVHeader = "X-SeaweedFS-SSE-KMS-Base-IV" // Header for passing base IV for multipart SSE-KMS
	SeaweedFSSSES3BaseIVHeader  = "X-SeaweedFS-SSE-S3-Base-IV"  // Header for passing base IV for multipart SSE-S3
	SeaweedFSSSES3KeyDataHeader = "X-SeaweedFS-SSE-S3-Key-Data" // Header for passing key data for multipart SSE-S3
)

// Non-Standard S3 HTTP request constants
const (
	AmzIdentityId = "s3-identity-id"
	AmzAccountId  = "s3-account-id"
	AmzAuthType   = "s3-auth-type"
)

func GetBucketAndObject(r *http.Request) (bucket, object string) {
	vars := mux.Vars(r)
	bucket = vars["bucket"]
	object = NormalizeObjectKey(vars["object"])
	return
}

// NormalizeObjectKey normalizes object keys by removing duplicate slashes and converting backslashes.
// This normalizes keys from various sources (URL path, form values, etc.) to a consistent format.
// It also converts Windows-style backslashes to forward slashes for cross-platform compatibility.
// Returns keys WITHOUT leading slash to match S3 API format (e.g., "foo/bar" not "/foo/bar").
func NormalizeObjectKey(object string) string {
	// Convert Windows-style backslashes to forward slashes
	object = strings.ReplaceAll(object, "\\", "/")
	object = removeDuplicateSlashes(object)
	// Remove leading slash to match S3 API format
	object = strings.TrimPrefix(object, "/")
	return object
}

// removeDuplicateSlashes removes consecutive slashes from a path
func removeDuplicateSlashes(s string) string {
	var result strings.Builder
	result.Grow(len(s))

	lastWasSlash := false
	for _, r := range s {
		if r == '/' {
			if !lastWasSlash {
				result.WriteRune(r)
			}
			lastWasSlash = true
		} else {
			result.WriteRune(r)
			lastWasSlash = false
		}
	}
	return result.String()
}

func GetPrefix(r *http.Request) string {
	query := r.URL.Query()
	prefix := query.Get("prefix")
	prefix = removeDuplicateSlashes(prefix)
	return prefix
}

var PassThroughHeaders = map[string]string{
	"response-cache-control":       "Cache-Control",
	"response-content-disposition": "Content-Disposition",
	"response-content-encoding":    "Content-Encoding",
	"response-content-language":    "Content-Language",
	"response-content-type":        "Content-Type",
	"response-expires":             "Expires",
}

// IsSeaweedFSInternalHeader checks if a header key is a SeaweedFS internal header
// that should be filtered from client responses.
// Header names are case-insensitive in HTTP, so this function normalizes to lowercase.
func IsSeaweedFSInternalHeader(headerKey string) bool {
	return strings.HasPrefix(strings.ToLower(headerKey), SeaweedFSInternalPrefix)
}

// Context keys for storing authenticated identity information
type contextKey string

const (
	contextKeyIdentityName   contextKey = "s3-identity-name"
	contextKeyIdentityObject contextKey = "s3-identity-object"
)

// SetIdentityNameInContext stores the authenticated identity name in the request context
// This is the secure way to propagate identity - headers can be spoofed, context cannot
func SetIdentityNameInContext(ctx context.Context, identityName string) context.Context {
	if identityName != "" {
		return context.WithValue(ctx, contextKeyIdentityName, identityName)
	}
	return ctx
}

// GetIdentityNameFromContext retrieves the authenticated identity name from the request context
// Returns empty string if no identity is set (unauthenticated request)
// This is the secure way to retrieve identity - never read from headers directly
func GetIdentityNameFromContext(r *http.Request) string {
	if name, ok := r.Context().Value(contextKeyIdentityName).(string); ok {
		return name
	}
	return ""
}

// SetIdentityInContext stores the full authenticated identity object in the request context
// This is used to pass the full identity (including for JWT users) to handlers
func SetIdentityInContext(ctx context.Context, identity interface{}) context.Context {
	if identity != nil {
		return context.WithValue(ctx, contextKeyIdentityObject, identity)
	}
	return ctx
}

// GetIdentityFromContext retrieves the full identity object from the request context
// Returns nil if no identity is set (unauthenticated request)
func GetIdentityFromContext(r *http.Request) interface{} {
	return r.Context().Value(contextKeyIdentityObject)
}
