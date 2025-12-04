package s3api

import (
	"net/http"
	"strings"
)

// AWS Signature Version '4' constants.
const (
	signV4Algorithm = "AWS4-HMAC-SHA256"
	signV2Algorithm = "AWS"
	iso8601Format   = "20060102T150405Z"
	yyyymmdd        = "20060102"
)

// Verify if request has JWT.
func isRequestJWT(r *http.Request) bool {
	return strings.HasPrefix(r.Header.Get("Authorization"), "Bearer")
}

// Verify if request has AWS Signature Version '4'.
func isRequestSignatureV4(r *http.Request) bool {
	return strings.HasPrefix(r.Header.Get("Authorization"), signV4Algorithm)
}

// Verify if request has AWS Signature Version '2'.
func isRequestSignatureV2(r *http.Request) bool {
	return !strings.HasPrefix(r.Header.Get("Authorization"), signV4Algorithm) &&
		strings.HasPrefix(r.Header.Get("Authorization"), signV2Algorithm)
}

// Verify if request has AWS PreSign Version '4'.
func isRequestPresignedSignatureV4(r *http.Request) bool {
	_, ok := r.URL.Query()["X-Amz-Credential"]
	return ok
}

// Verify request has AWS PreSign Version '2'.
func isRequestPresignedSignatureV2(r *http.Request) bool {
	_, ok := r.URL.Query()["AWSAccessKeyId"]
	return ok
}

// Verify if request has AWS Post policy Signature Version '4'.
func isRequestPostPolicySignatureV4(r *http.Request) bool {
	return strings.Contains(r.Header.Get("Content-Type"), "multipart/form-data") &&
		r.Method == http.MethodPost
}

// Verify if the request has AWS Streaming Signature Version '4'. This is only valid for 'PUT' operation.
// Supports both with and without trailer variants:
// - STREAMING-AWS4-HMAC-SHA256-PAYLOAD (original)
// - STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER (with trailing checksums)
func isRequestSignStreamingV4(r *http.Request) bool {
	contentSha256 := r.Header.Get("x-amz-content-sha256")
	return (contentSha256 == streamingContentSHA256 || contentSha256 == streamingContentSHA256Trailer) &&
		r.Method == http.MethodPut
}

func isRequestUnsignedStreaming(r *http.Request) bool {
	return r.Header.Get("x-amz-content-sha256") == streamingUnsignedPayload &&
		r.Method == http.MethodPut
}

// Authorization type.
type authType int

// List of all supported auth types.
const (
	authTypeUnknown authType = iota
	authTypeAnonymous
	authTypePresigned
	authTypePresignedV2
	authTypePostPolicy
	authTypeStreamingSigned
	authTypeStreamingUnsigned
	authTypeSigned
	authTypeSignedV2
	authTypeJWT
)

// Get request authentication type.
func getRequestAuthType(r *http.Request) authType {
	var authType authType

	if isRequestSignatureV2(r) {
		authType = authTypeSignedV2
	} else if isRequestPresignedSignatureV2(r) {
		authType = authTypePresignedV2
	} else if isRequestSignStreamingV4(r) {
		authType = authTypeStreamingSigned
	} else if isRequestUnsignedStreaming(r) {
		authType = authTypeStreamingUnsigned
	} else if isRequestSignatureV4(r) {
		authType = authTypeSigned
	} else if isRequestPresignedSignatureV4(r) {
		authType = authTypePresigned
	} else if isRequestJWT(r) {
		authType = authTypeJWT
	} else if isRequestPostPolicySignatureV4(r) {
		authType = authTypePostPolicy
	} else if _, ok := r.Header["Authorization"]; !ok {
		authType = authTypeAnonymous
	} else {
		authType = authTypeUnknown
	}

	return authType
}
