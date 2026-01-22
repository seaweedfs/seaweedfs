package iamapi

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"io"
	"bytes"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// Signature V4 constants
const (
	signV4Algorithm = "AWS4-HMAC-SHA256"
	iso8601Format   = "20060102T150405Z"
	yyyymmdd        = "20060102"
)

// v4AuthInfo holds parsed authentication data from an IAM API request
type v4AuthInfo struct {
	Signature     string
	AccessKey     string
	SignedHeaders []string
	Date          time.Time
	Region        string
	Service       string
	Scope         string
	HashedPayload string
	IsPresigned   bool
}

// isRequestPresignedSignatureV4 checks if request has presigned URL
func isRequestPresignedSignatureV4(r *http.Request) bool {
	return r.URL.Query().Get("X-Amz-Algorithm") == signV4Algorithm
}

// extractV4AuthInfoFromHeader extracts auth info from Authorization header
func extractV4AuthInfoFromHeader(r *http.Request) (*v4AuthInfo, s3err.ErrorCode) {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return nil, s3err.ErrAuthHeaderEmpty
	}

	// Parse date
	var t time.Time
	if xamz := r.Header.Get("x-amz-date"); xamz != "" {
		parsed, err := time.Parse(iso8601Format, xamz)
		if err != nil {
			return nil, s3err.ErrMalformedDate
		}
		t = parsed
	} else {
		return nil, s3err.ErrMissingDateHeader
	}

	// Validate clock skew
	const maxSkew = 15 * time.Minute
	now := time.Now().UTC()
	if now.Sub(t) > maxSkew || t.Sub(now) > maxSkew {
		return nil, s3err.ErrRequestTimeTooSkewed
	}

	// Simple parser for IAM requests - extract components
	authHeader = strings.Replace(authHeader, " ", "", -1)
	if !strings.HasPrefix(authHeader, signV4Algorithm) {
		return nil, s3err.ErrSignatureVersionNotSupported
	}

	authHeader = strings.TrimPrefix(authHeader, signV4Algorithm)
	parts := strings.Split(authHeader, ",")
	if len(parts) != 3 {
		return nil, s3err.ErrMissingFields
	}

	// Parse credential
	credPart := strings.TrimPrefix(parts[0], "Credential=")
	credElements := strings.Split(credPart, "/")
	if len(credElements) != 5 {
		return nil, s3err.ErrCredMalformed
	}

	accessKey := credElements[0]
	dateStr := credElements[1]
	region := credElements[2]
	service := credElements[3]

	scopeDate, err := time.Parse(yyyymmdd, dateStr)
	if err != nil {
		return nil, s3err.ErrMalformedCredentialDate
	}

	// Parse signed headers
	signedHeadersPart := strings.TrimPrefix(parts[1], "SignedHeaders=")
	signedHeaders := strings.Split(signedHeadersPart, ";")

	// Parse signature
	signature := strings.TrimPrefix(parts[2], "Signature=")

	hashedPayload := r.Header.Get("X-Amz-Content-Sha256")
	if hashedPayload == "" {
		// If header is missing, calculate hash from body (standard for some clients)
		// We must read and restore the body
		if r.Body != nil {
			bodyBytes, err := io.ReadAll(r.Body)
			if err != nil {
				return nil, s3err.ErrInternalError
			}
			r.Body.Close()
			r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
			
			hash := sha256.Sum256(bodyBytes)
			hashedPayload = hex.EncodeToString(hash[:])
		} else {
			// Empty body
			hash := sha256.Sum256([]byte(""))
			hashedPayload = hex.EncodeToString(hash[:])
		}
	}

	return &v4AuthInfo{
		Signature:     signature,
		AccessKey:     accessKey,
		SignedHeaders: signedHeaders,
		Date:          t,
		Region:        region,
		Service:       service,
		Scope:         fmt.Sprintf("%s/%s/%s/aws4_request", scopeDate.Format(yyyymmdd), region, service),
		HashedPayload: hashedPayload,
		IsPresigned:   false,
	}, s3err.ErrNone
}

// extractV4AuthInfoFromQuery extracts auth info from presigned URL query params
func extractV4AuthInfoFromQuery(r *http.Request) (*v4AuthInfo, s3err.ErrorCode) {
	query := r.URL.Query()

	if query.Get("X-Amz-Algorithm") != signV4Algorithm {
		return nil, s3err.ErrSignatureVersionNotSupported
	}

	dateStr := query.Get("X-Amz-Date")
	if dateStr == "" {
		return nil, s3err.ErrMissingDateHeader
	}

	t, err := time.Parse(iso8601Format, dateStr)
	if err != nil {
		return nil, s3err.ErrMalformedDate
	}

	credStr := query.Get("X-Amz-Credential")
	credElements := strings.Split(credStr, "/")
	if len(credElements) != 5 {
		return nil, s3err.ErrCredMalformed
	}

	return &v4AuthInfo{
		Signature:     query.Get("X-Amz-Signature"),
		AccessKey:     credElements[0],
		SignedHeaders: strings.Split(query.Get("X-Amz-SignedHeaders"), ";"),
		Date:          t,
		Region:        credElements[2],
		Service:       credElements[3],
		Scope:         fmt.Sprintf("%s/%s/%s/aws4_request", credElements[1], credElements[2], credElements[3]),
		HashedPayload: "UNSIGNED-PAYLOAD",
		IsPresigned:   true,
	}, s3err.ErrNone
}

// verifyIAMSignature verifies the AWS Signature V4 for an IAM API request
func verifyIAMSignature(r *http.Request, secretKey string, authInfo *v4AuthInfo) error {
	// Build canonical request
	canonicalRequest := buildCanonicalRequest(r, authInfo)

	// Build string to sign
	stringToSign := buildStringToSign(canonicalRequest, authInfo)

	// Calculate signature
	signingKey := getSigningKey(secretKey, authInfo.Date.Format(yyyymmdd), authInfo.Region, authInfo.Service)
	calculatedSignature := getSignature(signingKey, stringToSign)

	// Compare signatures
	if !compareSignature(calculatedSignature, authInfo.Signature) {
		return fmt.Errorf("signature mismatch")
	}

	return nil
}

func buildCanonicalRequest(r *http.Request, authInfo *v4AuthInfo) string {
	// Extract signed headers
	headers := make(http.Header)
	for _, h := range authInfo.SignedHeaders {
		if h == "host" {
			headers.Set("host", r.Host)
		} else {
			if val := r.Header.Get(h); val != "" {
				headers.Set(h, val)
			}
		}
	}

	canonicalHeaders := getCanonicalHeaders(headers)
	signedHeadersStr := strings.Join(authInfo.SignedHeaders, ";")

	return strings.Join([]string{
		r.Method,
		r.URL.Path,
		r.URL.RawQuery,
		canonicalHeaders,
		signedHeadersStr,
		authInfo.HashedPayload,
	}, "\n")
}

func buildStringToSign(canonicalRequest string, authInfo *v4AuthInfo) string {
	hasher := sha256.New()
	hasher.Write([]byte(canonicalRequest))
	hashedRequest := hex.EncodeToString(hasher.Sum(nil))

	return strings.Join([]string{
		signV4Algorithm,
		authInfo.Date.Format(iso8601Format),
		authInfo.Scope,
		hashedRequest,
	}, "\n")
}

func getCanonicalHeaders(headers http.Header) string {
	var keys []string
	normalized := make(map[string]string)

	for k, v := range headers {
		lowerKey := strings.ToLower(k)
		keys = append(keys, lowerKey)
		normalized[lowerKey] = strings.Join(v, ",")
	}

	sort.Strings(keys)

	var canonical strings.Builder
	for _, k := range keys {
		canonical.WriteString(k)
		canonical.WriteString(":")
		canonical.WriteString(normalized[k])
		canonical.WriteString("\n")
	}

	return canonical.String()
}

func getSigningKey(secretKey, date, region, service string) []byte {
	kDate := sumHMAC([]byte("AWS4"+secretKey), []byte(date))
	kRegion := sumHMAC(kDate, []byte(region))
	kService := sumHMAC(kRegion, []byte(service))
	kSigning := sumHMAC(kService, []byte("aws4_request"))
	return kSigning
}

func sumHMAC(key, data []byte) []byte {
	hash := hmac.New(sha256.New, key)
	hash.Write(data)
	return hash.Sum(nil)
}

func getSignature(signingKey []byte, stringToSign string) string {
	return hex.EncodeToString(sumHMAC(signingKey, []byte(stringToSign)))
}

func compareSignature(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}
