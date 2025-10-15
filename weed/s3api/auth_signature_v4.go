/*
 * The following code tries to reverse engineer the Amazon S3 APIs,
 * and is mostly copied from minio implementation.
 */

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package s3api

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"io"
	"net/http"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

func (iam *IdentityAccessManagement) reqSignatureV4Verify(r *http.Request) (*Identity, s3err.ErrorCode) {
	sha256sum := getContentSha256Cksum(r)
	switch {
	case isRequestSignatureV4(r):
		return iam.doesSignatureMatch(sha256sum, r)
	case isRequestPresignedSignatureV4(r):
		return iam.doesPresignedSignatureMatch(sha256sum, r)
	}
	return nil, s3err.ErrAccessDenied
}

// Constants specific to this file
const (
	emptySHA256              = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	streamingContentSHA256   = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	streamingUnsignedPayload = "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
	unsignedPayload          = "UNSIGNED-PAYLOAD"
	// Limit for IAM/STS request body size to prevent DoS attacks
	iamRequestBodyLimit = 10 * (1 << 20) // 10 MiB
)

// streamHashRequestBody computes SHA256 hash incrementally while preserving the body.
func streamHashRequestBody(r *http.Request, sizeLimit int64) (string, error) {
	if r.Body == nil {
		return emptySHA256, nil
	}

	limitedReader := io.LimitReader(r.Body, sizeLimit)
	hasher := sha256.New()
	var bodyBuffer bytes.Buffer

	// Use io.Copy with an io.MultiWriter to hash and buffer the body simultaneously.
	if _, err := io.Copy(io.MultiWriter(hasher, &bodyBuffer), limitedReader); err != nil {
		return "", err
	}

	r.Body = io.NopCloser(&bodyBuffer)

	if bodyBuffer.Len() == 0 {
		return emptySHA256, nil
	}

	return hex.EncodeToString(hasher.Sum(nil)), nil
}

// getContentSha256Cksum retrieves the "x-amz-content-sha256" header value.
func getContentSha256Cksum(r *http.Request) string {
	// If the client sends a SHA256 checksum of the object in this header, use it.
	if v := r.Header.Get("X-Amz-Content-Sha256"); v != "" {
		return v
	}

	// For a presigned request we look at the query param for sha256.
	if isRequestPresignedSignatureV4(r) {
		// X-Amz-Content-Sha256 header value is optional for presigned requests.
		return unsignedPayload
	}

	// X-Amz-Content-Sha256 header value is required for all non-presigned requests.
	return emptySHA256
}

// signValues data type represents structured form of AWS Signature V4 header.
type signValues struct {
	Credential    credentialHeader
	SignedHeaders []string
	Signature     string
}

// parseSignV4 parses the authorization header for signature v4.
func parseSignV4(v4Auth string) (sv signValues, aec s3err.ErrorCode) {
	// Replace all spaced strings, some clients can send spaced
	// parameters and some won't. So we pro-actively remove any spaces
	// to make parsing easier.
	v4Auth = strings.Replace(v4Auth, " ", "", -1)
	if v4Auth == "" {
		return sv, s3err.ErrAuthHeaderEmpty
	}

	// Verify if the header algorithm is supported or not.
	if !strings.HasPrefix(v4Auth, signV4Algorithm) {
		return sv, s3err.ErrSignatureVersionNotSupported
	}

	// Strip off the Algorithm prefix.
	v4Auth = strings.TrimPrefix(v4Auth, signV4Algorithm)
	authFields := strings.Split(strings.TrimSpace(v4Auth), ",")
	if len(authFields) != 3 {
		return sv, s3err.ErrMissingFields
	}

	// Initialize signature version '4' structured header.
	signV4Values := signValues{}

	var err s3err.ErrorCode
	// Save credential values.
	signV4Values.Credential, err = parseCredentialHeader(authFields[0])
	if err != s3err.ErrNone {
		return sv, err
	}

	// Save signed headers.
	signV4Values.SignedHeaders, err = parseSignedHeader(authFields[1])
	if err != s3err.ErrNone {
		return sv, err
	}

	// Save signature.
	signV4Values.Signature, err = parseSignature(authFields[2])
	if err != s3err.ErrNone {
		return sv, err
	}

	// Return the structure here.
	return signV4Values, s3err.ErrNone
}

// doesSignatureMatch verifies the request signature.
func (iam *IdentityAccessManagement) doesSignatureMatch(hashedPayload string, r *http.Request) (*Identity, s3err.ErrorCode) {

	// Copy request
	req := *r

	// Save authorization header.
	v4Auth := req.Header.Get("Authorization")

	// Parse signature version '4' header.
	signV4Values, errCode := parseSignV4(v4Auth)
	if errCode != s3err.ErrNone {
		return nil, errCode
	}

	// Compute payload hash for non-S3 services
	if signV4Values.Credential.scope.service != "s3" && hashedPayload == emptySHA256 && r.Body != nil {
		var err error
		hashedPayload, err = streamHashRequestBody(r, iamRequestBodyLimit)
		if err != nil {
			return nil, s3err.ErrInternalError
		}
	}

	// Extract all the signed headers along with its values.
	extractedSignedHeaders, errCode := extractSignedHeaders(signV4Values.SignedHeaders, r)
	if errCode != s3err.ErrNone {
		return nil, errCode
	}

	cred := signV4Values.Credential
	identity, foundCred, found := iam.lookupByAccessKey(cred.accessKey)
	if !found {
		return nil, s3err.ErrInvalidAccessKeyID
	}

	// Extract date, if not present throw error.
	var dateStr string
	if dateStr = req.Header.Get("x-amz-date"); dateStr == "" {
		if dateStr = r.Header.Get("Date"); dateStr == "" {
			return nil, s3err.ErrMissingDateHeader
		}
	}
	// Parse date header.
	t, e := time.Parse(iso8601Format, dateStr)
	if e != nil {
		return nil, s3err.ErrMalformedDate
	}

	// Query string.
	queryStr := req.URL.Query().Encode()

	// Check if reverse proxy is forwarding with prefix
	if forwardedPrefix := r.Header.Get("X-Forwarded-Prefix"); forwardedPrefix != "" {
		// Try signature verification with the forwarded prefix first.
		// This handles cases where reverse proxies strip URL prefixes and add the X-Forwarded-Prefix header.
		cleanedPath := buildPathWithForwardedPrefix(forwardedPrefix, req.URL.Path)
		errCode = iam.verifySignatureWithPath(extractedSignedHeaders, hashedPayload, queryStr, cleanedPath, req.Method, foundCred.SecretKey, t, signV4Values)
		if errCode == s3err.ErrNone {
			return identity, errCode
		}
	}

	// Try normal signature verification (without prefix)
	errCode = iam.verifySignatureWithPath(extractedSignedHeaders, hashedPayload, queryStr, req.URL.Path, req.Method, foundCred.SecretKey, t, signV4Values)
	if errCode == s3err.ErrNone {
		return identity, errCode
	}

	return nil, errCode
}

// buildPathWithForwardedPrefix combines forwarded prefix with URL path while preserving trailing slashes.
// This ensures compatibility with S3 SDK signatures that include trailing slashes for directory operations.
func buildPathWithForwardedPrefix(forwardedPrefix, urlPath string) string {
	fullPath := forwardedPrefix + urlPath
	hasTrailingSlash := strings.HasSuffix(urlPath, "/") && urlPath != "/"
	cleanedPath := path.Clean(fullPath)
	if hasTrailingSlash && !strings.HasSuffix(cleanedPath, "/") {
		cleanedPath += "/"
	}
	return cleanedPath
}

// verifySignatureWithPath verifies signature with a given path (used for both normal and prefixed paths).
func (iam *IdentityAccessManagement) verifySignatureWithPath(extractedSignedHeaders http.Header, hashedPayload, queryStr, urlPath, method, secretKey string, t time.Time, signV4Values signValues) s3err.ErrorCode {
	// Get canonical request.
	canonicalRequest := getCanonicalRequest(extractedSignedHeaders, hashedPayload, queryStr, urlPath, method)

	// Get string to sign from canonical request.
	stringToSign := getStringToSign(canonicalRequest, t, signV4Values.Credential.getScope())

	// Get hmac signing key.
	signingKey := getSigningKey(secretKey, signV4Values.Credential.scope.date.Format(yyyymmdd), signV4Values.Credential.scope.region, signV4Values.Credential.scope.service)

	// Calculate signature.
	newSignature := getSignature(signingKey, stringToSign)

	// Verify if signature match.
	if !compareSignatureV4(newSignature, signV4Values.Signature) {
		return s3err.ErrSignatureDoesNotMatch
	}

	return s3err.ErrNone
}

// verifyPresignedSignatureWithPath verifies presigned signature with a given path (used for both normal and prefixed paths).
func (iam *IdentityAccessManagement) verifyPresignedSignatureWithPath(extractedSignedHeaders http.Header, hashedPayload, queryStr, urlPath, method, secretKey string, t time.Time, credHeader credentialHeader, signature string) s3err.ErrorCode {
	// Get canonical request.
	canonicalRequest := getCanonicalRequest(extractedSignedHeaders, hashedPayload, queryStr, urlPath, method)

	// Get string to sign from canonical request.
	stringToSign := getStringToSign(canonicalRequest, t, credHeader.getScope())

	// Get hmac signing key.
	signingKey := getSigningKey(secretKey, credHeader.scope.date.Format(yyyymmdd), credHeader.scope.region, credHeader.scope.service)

	// Calculate expected signature.
	expectedSignature := getSignature(signingKey, stringToSign)

	// Verify if signature match.
	if !compareSignatureV4(expectedSignature, signature) {
		return s3err.ErrSignatureDoesNotMatch
	}

	return s3err.ErrNone
}

// Simple implementation for presigned signature verification
func (iam *IdentityAccessManagement) doesPresignedSignatureMatch(hashedPayload string, r *http.Request) (*Identity, s3err.ErrorCode) {
	// Parse presigned signature values from query parameters
	query := r.URL.Query()

	// Check required parameters
	algorithm := query.Get("X-Amz-Algorithm")
	if algorithm != signV4Algorithm {
		return nil, s3err.ErrSignatureVersionNotSupported
	}

	credential := query.Get("X-Amz-Credential")
	if credential == "" {
		return nil, s3err.ErrMissingFields
	}

	signature := query.Get("X-Amz-Signature")
	if signature == "" {
		return nil, s3err.ErrMissingFields
	}

	signedHeadersStr := query.Get("X-Amz-SignedHeaders")
	if signedHeadersStr == "" {
		return nil, s3err.ErrMissingFields
	}

	dateStr := query.Get("X-Amz-Date")
	if dateStr == "" {
		return nil, s3err.ErrMissingDateHeader
	}

	// Parse credential
	credHeader, err := parseCredentialHeader("Credential=" + credential)
	if err != s3err.ErrNone {
		return nil, err
	}

	// Look up identity by access key
	identity, foundCred, found := iam.lookupByAccessKey(credHeader.accessKey)
	if !found {
		return nil, s3err.ErrInvalidAccessKeyID
	}

	// Parse date
	t, e := time.Parse(iso8601Format, dateStr)
	if e != nil {
		return nil, s3err.ErrMalformedDate
	}

	// Check expiration
	expiresStr := query.Get("X-Amz-Expires")
	if expiresStr != "" {
		expires, parseErr := strconv.ParseInt(expiresStr, 10, 64)
		if parseErr != nil {
			return nil, s3err.ErrMalformedDate
		}
		// Check if current time is after the expiration time
		expirationTime := t.Add(time.Duration(expires) * time.Second)
		if time.Now().UTC().After(expirationTime) {
			return nil, s3err.ErrExpiredPresignRequest
		}
	}

	// Parse signed headers
	signedHeaders := strings.Split(signedHeadersStr, ";")

	// Extract signed headers from request
	extractedSignedHeaders := make(http.Header)
	for _, header := range signedHeaders {
		if header == "host" {
			extractedSignedHeaders[header] = []string{extractHostHeader(r)}
			continue
		}
		if values := r.Header[http.CanonicalHeaderKey(header)]; len(values) > 0 {
			extractedSignedHeaders[http.CanonicalHeaderKey(header)] = values
		}
	}

	// Remove signature from query for canonical request calculation
	queryForCanonical := r.URL.Query()
	queryForCanonical.Del("X-Amz-Signature")
	queryStr := strings.Replace(queryForCanonical.Encode(), "+", "%20", -1)

	var errCode s3err.ErrorCode
	// Check if reverse proxy is forwarding with prefix for presigned URLs
	if forwardedPrefix := r.Header.Get("X-Forwarded-Prefix"); forwardedPrefix != "" {
		// Try signature verification with the forwarded prefix first.
		// This handles cases where reverse proxies strip URL prefixes and add the X-Forwarded-Prefix header.
		cleanedPath := buildPathWithForwardedPrefix(forwardedPrefix, r.URL.Path)
		errCode = iam.verifyPresignedSignatureWithPath(extractedSignedHeaders, hashedPayload, queryStr, cleanedPath, r.Method, foundCred.SecretKey, t, credHeader, signature)
		if errCode == s3err.ErrNone {
			return identity, errCode
		}
	}

	// Try normal signature verification (without prefix)
	errCode = iam.verifyPresignedSignatureWithPath(extractedSignedHeaders, hashedPayload, queryStr, r.URL.Path, r.Method, foundCred.SecretKey, t, credHeader, signature)
	if errCode == s3err.ErrNone {
		return identity, errCode
	}

	return nil, errCode
}

// credentialHeader data type represents structured form of Credential
// string from authorization header.
type credentialHeader struct {
	accessKey string
	scope     struct {
		date    time.Time
		region  string
		service string
		request string
	}
}

func (c credentialHeader) getScope() string {
	return strings.Join([]string{
		c.scope.date.Format(yyyymmdd),
		c.scope.region,
		c.scope.service,
		c.scope.request,
	}, "/")
}

// parse credentialHeader string into its structured form.
func parseCredentialHeader(credElement string) (ch credentialHeader, aec s3err.ErrorCode) {
	creds := strings.SplitN(strings.TrimSpace(credElement), "=", 2)
	if len(creds) != 2 {
		return ch, s3err.ErrMissingFields
	}
	if creds[0] != "Credential" {
		return ch, s3err.ErrMissingCredTag
	}
	credElements := strings.Split(strings.TrimSpace(creds[1]), "/")
	if len(credElements) != 5 {
		return ch, s3err.ErrCredMalformed
	}
	// Save access key id.
	cred := credentialHeader{
		accessKey: credElements[0],
	}
	var e error
	cred.scope.date, e = time.Parse(yyyymmdd, credElements[1])
	if e != nil {
		return ch, s3err.ErrMalformedCredentialDate
	}

	cred.scope.region = credElements[2]
	cred.scope.service = credElements[3] // "s3"
	cred.scope.request = credElements[4] // "aws4_request"
	return cred, s3err.ErrNone
}

// Parse signature from signature tag.
func parseSignature(signElement string) (string, s3err.ErrorCode) {
	signFields := strings.Split(strings.TrimSpace(signElement), "=")
	if len(signFields) != 2 {
		return "", s3err.ErrMissingFields
	}
	if signFields[0] != "Signature" {
		return "", s3err.ErrMissingSignTag
	}
	if signFields[1] == "" {
		return "", s3err.ErrMissingFields
	}
	signature := signFields[1]
	return signature, s3err.ErrNone
}

// Parse slice of signed headers from signed headers tag.
func parseSignedHeader(signedHdrElement string) ([]string, s3err.ErrorCode) {
	signedHdrFields := strings.Split(strings.TrimSpace(signedHdrElement), "=")
	if len(signedHdrFields) != 2 {
		return nil, s3err.ErrMissingFields
	}
	if signedHdrFields[0] != "SignedHeaders" {
		return nil, s3err.ErrMissingSignHeadersTag
	}
	if signedHdrFields[1] == "" {
		return nil, s3err.ErrMissingFields
	}
	signedHeaders := strings.Split(signedHdrFields[1], ";")
	return signedHeaders, s3err.ErrNone
}

func (iam *IdentityAccessManagement) doesPolicySignatureV4Match(formValues http.Header) s3err.ErrorCode {

	// Parse credential tag.
	credHeader, err := parseCredentialHeader("Credential=" + formValues.Get("X-Amz-Credential"))
	if err != s3err.ErrNone {
		return err
	}

	identity, cred, found := iam.lookupByAccessKey(credHeader.accessKey)
	if !found {
		return s3err.ErrInvalidAccessKeyID
	}

	bucket := formValues.Get("bucket")
	if !identity.canDo(s3_constants.ACTION_WRITE, bucket, "") {
		return s3err.ErrAccessDenied
	}

	// Get signing key.
	signingKey := getSigningKey(cred.SecretKey, credHeader.scope.date.Format(yyyymmdd), credHeader.scope.region, credHeader.scope.service)

	// Get signature.
	newSignature := getSignature(signingKey, formValues.Get("Policy"))

	// Verify signature.
	if !compareSignatureV4(newSignature, formValues.Get("X-Amz-Signature")) {
		return s3err.ErrSignatureDoesNotMatch
	}
	return s3err.ErrNone
}

// Verify if extracted signed headers are not properly signed.
func extractSignedHeaders(signedHeaders []string, r *http.Request) (http.Header, s3err.ErrorCode) {
	reqHeaders := r.Header
	// If no signed headers are provided, then return an error.
	if len(signedHeaders) == 0 {
		return nil, s3err.ErrMissingFields
	}
	extractedSignedHeaders := make(http.Header)
	for _, header := range signedHeaders {
		// `host` is not a case-sensitive header, unlike other headers such as `x-amz-date`.
		if header == "host" {
			// Get host value.
			hostHeaderValue := extractHostHeader(r)
			extractedSignedHeaders[header] = []string{hostHeaderValue}
			continue
		}
		// For all other headers we need to find them in the HTTP headers and copy them over.
		// We skip non-existent headers to be compatible with AWS signatures.
		if values, ok := reqHeaders[http.CanonicalHeaderKey(header)]; ok {
			extractedSignedHeaders[header] = values
		}
	}
	return extractedSignedHeaders, s3err.ErrNone
}

// extractHostHeader returns the value of host header if available.
func extractHostHeader(r *http.Request) string {
	// Check for X-Forwarded-Host header first, which is set by reverse proxies
	if forwardedHost := r.Header.Get("X-Forwarded-Host"); forwardedHost != "" {
		// Check if reverse proxy also forwarded the port
		if forwardedPort := r.Header.Get("X-Forwarded-Port"); forwardedPort != "" {
			// Determine the protocol to check for standard ports
			proto := r.Header.Get("X-Forwarded-Proto")
			// Only add port if it's not the standard port for the protocol
			if (proto == "https" && forwardedPort != "443") || (proto != "https" && forwardedPort != "80") {
				return forwardedHost + ":" + forwardedPort
			}
		}
		// Using reverse proxy with X-Forwarded-Host (standard port or no port forwarded).
		return forwardedHost
	}

	hostHeaderValue := r.Host
	// For standard requests, this should be fine.
	if r.Host != "" {
		return hostHeaderValue
	}
	// If no host header is found, then check for host URL value.
	if r.URL.Host != "" {
		hostHeaderValue = r.URL.Host
	}
	return hostHeaderValue
}

// getScope generate a string of a specific date, an AWS region, and a service.
func getScope(t time.Time, region string, service string) string {
	scope := strings.Join([]string{
		t.Format(yyyymmdd),
		region,
		service,
		"aws4_request",
	}, "/")
	return scope
}

// getCanonicalRequest generate a canonical request of style
//
// canonicalRequest =
//
//	<HTTPMethod>\n
//	<CanonicalURI>\n
//	<CanonicalQueryString>\n
//	<CanonicalHeaders>\n
//	<SignedHeaders>\n
//	<HashedPayload>
func getCanonicalRequest(extractedSignedHeaders http.Header, payload, queryStr, urlPath, method string) string {
	rawQuery := strings.Replace(queryStr, "+", "%20", -1)
	encodedPath := encodePath(urlPath)
	canonicalRequest := strings.Join([]string{
		method,
		encodedPath,
		rawQuery,
		getCanonicalHeaders(extractedSignedHeaders),
		getSignedHeaders(extractedSignedHeaders),
		payload,
	}, "\n")
	return canonicalRequest
}

// getStringToSign a string based on selected query values.
func getStringToSign(canonicalRequest string, t time.Time, scope string) string {
	stringToSign := signV4Algorithm + "\n" + t.Format(iso8601Format) + "\n"
	stringToSign = stringToSign + scope + "\n"
	stringToSign = stringToSign + getSHA256Hash([]byte(canonicalRequest))
	return stringToSign
}

// getSHA256Hash returns hex-encoded SHA256 hash of the input data.
func getSHA256Hash(data []byte) string {
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

// sumHMAC calculate hmac between two input byte array.
func sumHMAC(key []byte, data []byte) []byte {
	hash := hmac.New(sha256.New, key)
	hash.Write(data)
	return hash.Sum(nil)
}

// getSigningKey hmac seed to calculate final signature.
func getSigningKey(secretKey string, time string, region string, service string) []byte {
	date := sumHMAC([]byte("AWS4"+secretKey), []byte(time))
	regionBytes := sumHMAC(date, []byte(region))
	serviceBytes := sumHMAC(regionBytes, []byte(service))
	signingKey := sumHMAC(serviceBytes, []byte("aws4_request"))
	return signingKey
}

// getCanonicalHeaders generate a list of request headers with their values
func getCanonicalHeaders(signedHeaders http.Header) string {
	var headers []string
	vals := make(http.Header)
	for k, vv := range signedHeaders {
		vals[strings.ToLower(k)] = vv
	}
	for k := range vals {
		headers = append(headers, k)
	}
	sort.Strings(headers)

	var buf bytes.Buffer
	for _, k := range headers {
		buf.WriteString(k)
		buf.WriteByte(':')
		for idx, v := range vals[k] {
			if idx > 0 {
				buf.WriteByte(',')
			}
			buf.WriteString(signV4TrimAll(v))
		}
		buf.WriteByte('\n')
	}
	return buf.String()
}

// signV4TrimAll trims leading and trailing spaces from each string in the slice, and trims sequential spaces.
func signV4TrimAll(input string) string {
	// Compress adjacent spaces (a space is determined by
	// unicode.IsSpace() internally here) to a single space and trim
	// leading and trailing spaces.
	return strings.Join(strings.Fields(input), " ")
}

// getSignedHeaders generate a string i.e alphabetically sorted, semicolon-separated list of lowercase request header names
func getSignedHeaders(signedHeaders http.Header) string {
	var headers []string
	for k := range signedHeaders {
		headers = append(headers, strings.ToLower(k))
	}
	sort.Strings(headers)
	return strings.Join(headers, ";")
}

// if object matches reserved string, no need to encode them
var reservedObjectNames = regexp.MustCompile("^[a-zA-Z0-9-_.~/]+$")

// encodePath encodes the strings from UTF-8 byte representations to HTML hex escape sequences
//
// This is necessary since regular url.Parse() and url.Encode() functions do not support UTF-8
// non english characters cannot be parsed due to the nature in which url.Encode() is written
//
// This function on the other hand is a direct replacement for url.Encode() technique to support
// pretty much every UTF-8 character.
func encodePath(pathName string) string {
	if reservedObjectNames.MatchString(pathName) {
		return pathName
	}
	var encodedPathname string
	for _, s := range pathName {
		if 'A' <= s && s <= 'Z' || 'a' <= s && s <= 'z' || '0' <= s && s <= '9' { // §2.3 Unreserved characters (mark)
			encodedPathname = encodedPathname + string(s)
		} else {
			switch s {
			case '-', '_', '.', '~', '/': // §2.3 Unreserved characters (mark)
				encodedPathname = encodedPathname + string(s)
			default:
				runeLen := utf8.RuneLen(s)
				if runeLen < 0 {
					return pathName
				}
				u := make([]byte, runeLen)
				utf8.EncodeRune(u, s)
				for _, r := range u {
					hex := hex.EncodeToString([]byte{r})
					encodedPathname = encodedPathname + "%" + strings.ToUpper(hex)
				}
			}
		}
	}
	return encodedPathname
}

// getSignature final signature in hexadecimal form.
func getSignature(signingKey []byte, stringToSign string) string {
	return hex.EncodeToString(sumHMAC(signingKey, []byte(stringToSign)))
}

// compareSignatureV4 returns true if and only if both signatures
// are equal. The signatures are expected to be hex-encoded strings
// according to the AWS S3 signature V4 spec.
func compareSignatureV4(sig1, sig2 string) bool {
	// The CTC using []byte(str) works because the hex encoding doesn't use
	// non-ASCII characters. Otherwise, we'd need to convert the strings to
	// a []rune of UTF-8 characters.
	return subtle.ConstantTimeCompare([]byte(sig1), []byte(sig2)) == 1
}
