package s3api

import (
	"net/http"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
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
func isRequestSignStreamingV4(r *http.Request) bool {
	return r.Header.Get("x-amz-content-sha256") == streamingContentSHA256 &&
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

	// Debug: log all headers
	glog.Infof("[DEBUG] getRequestAuthType: request %s %s", r.Method, r.URL.Path)
	glog.Infof("[DEBUG] getRequestAuthType: headers:")
	for name, values := range r.Header {
		for _, value := range values {
			if strings.Contains(strings.ToLower(name), "auth") ||
				strings.Contains(strings.ToLower(name), "sign") ||
				strings.Contains(strings.ToLower(name), "credential") ||
				strings.Contains(strings.ToLower(name), "amz") {
				glog.Infof("[DEBUG] getRequestAuthType: %s: %s", name, value)
			}
		}
	}

	if isRequestSignatureV2(r) {
		authType = authTypeSignedV2
		glog.Infof("[DEBUG] getRequestAuthType: detected SignatureV2")
	} else if isRequestPresignedSignatureV2(r) {
		authType = authTypePresignedV2
		glog.Infof("[DEBUG] getRequestAuthType: detected PresignedSignatureV2")
	} else if isRequestSignStreamingV4(r) {
		authType = authTypeStreamingSigned
		glog.Infof("[DEBUG] getRequestAuthType: detected StreamingV4")
	} else if isRequestUnsignedStreaming(r) {
		authType = authTypeStreamingUnsigned
		glog.Infof("[DEBUG] getRequestAuthType: detected UnsignedStreaming")
	} else if isRequestSignatureV4(r) {
		authType = authTypeSigned
		glog.Infof("[DEBUG] getRequestAuthType: detected SignatureV4")
	} else if isRequestPresignedSignatureV4(r) {
		authType = authTypePresigned
		glog.Infof("[DEBUG] getRequestAuthType: detected PresignedSignatureV4")
	} else if isRequestJWT(r) {
		authType = authTypeJWT
		glog.Infof("[DEBUG] getRequestAuthType: detected JWT")
	} else if isRequestPostPolicySignatureV4(r) {
		authType = authTypePostPolicy
		glog.Infof("[DEBUG] getRequestAuthType: detected PostPolicy")
	} else if _, ok := r.Header["Authorization"]; !ok {
		authType = authTypeAnonymous
		glog.Infof("[DEBUG] getRequestAuthType: detected Anonymous (no Authorization header)")
	} else {
		authType = authTypeUnknown
		glog.Infof("[DEBUG] getRequestAuthType: detected Unknown")
	}

	glog.Infof("[DEBUG] getRequestAuthType: request %s %s -> authType=%v, hasAuth=%v",
		r.Method, r.URL.Path, authType, r.Header.Get("Authorization") != "")

	return authType
}
