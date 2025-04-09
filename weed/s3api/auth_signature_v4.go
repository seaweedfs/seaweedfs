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
	"hash"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/seaweedfs/seaweedfs/weed/glog"
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

// Streaming AWS Signature Version '4' constants.
const (
	emptySHA256            = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	streamingContentSHA256 = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	signV4ChunkedAlgorithm = "AWS4-HMAC-SHA256-PAYLOAD"

	// http Header "x-amz-content-sha256" == "UNSIGNED-PAYLOAD" or "STREAMING-UNSIGNED-PAYLOAD-TRAILER" indicates that the
	// client did not calculate sha256 of the payload.
	unsignedPayload          = "UNSIGNED-PAYLOAD"
	streamingUnsignedPayload = "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
)

// AWS S3 authentication headers that should be skipped when signing the request
// https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
var awsS3AuthHeaders = map[string]struct{}{
	"x-amz-content-sha256": {},
	"x-amz-security-token": {},
	"x-amz-algorithm":      {},
	"x-amz-date":           {},
	"x-amz-expires":        {},
	"x-amz-signedheaders":  {},
	"x-amz-credential":     {},
	"x-amz-signature":      {},
}

// Returns SHA256 for calculating canonical-request.
func getContentSha256Cksum(r *http.Request) string {
	var (
		defaultSha256Cksum string
		v                  []string
		ok                 bool
	)

	// For a presigned request we look at the query param for sha256.
	if isRequestPresignedSignatureV4(r) {
		// X-Amz-Content-Sha256, if not set in presigned requests, checksum
		// will default to 'UNSIGNED-PAYLOAD'.
		defaultSha256Cksum = unsignedPayload
		v, ok = r.URL.Query()["X-Amz-Content-Sha256"]
		if !ok {
			v, ok = r.Header["X-Amz-Content-Sha256"]
		}
	} else {
		// X-Amz-Content-Sha256, if not set in signed requests, checksum
		// will default to sha256([]byte("")).
		defaultSha256Cksum = emptySHA256
		v, ok = r.Header["X-Amz-Content-Sha256"]
	}

	// We found 'X-Amz-Content-Sha256' return the captured value.
	if ok {
		return v[0]
	}

	// We couldn't find 'X-Amz-Content-Sha256'.
	return defaultSha256Cksum
}

// Verify authorization header - http://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-authenticating-requests.html
func (iam *IdentityAccessManagement) doesSignatureMatch(hashedPayload string, r *http.Request) (*Identity, s3err.ErrorCode) {

	// Copy request.
	req := *r

	// Save authorization header.
	v4Auth := req.Header.Get("Authorization")

	// Parse signature version '4' header.
	signV4Values, err := parseSignV4(v4Auth)
	if err != s3err.ErrNone {
		return nil, err
	}

	// Extract all the signed headers along with its values.
	extractedSignedHeaders, errCode := extractSignedHeaders(signV4Values.SignedHeaders, r)
	if errCode != s3err.ErrNone {
		return nil, errCode
	}

	// Verify if the access key id matches.
	identity, cred, found := iam.lookupByAccessKey(signV4Values.Credential.accessKey)
	if !found {
		return nil, s3err.ErrInvalidAccessKeyID
	}

	// Extract date, if not present throw error.
	var date string
	if date = req.Header.Get(http.CanonicalHeaderKey("X-Amz-Date")); date == "" {
		if date = r.Header.Get("Date"); date == "" {
			return nil, s3err.ErrMissingDateHeader
		}
	}
	// Parse date header.
	t, e := time.Parse(iso8601Format, date)
	if e != nil {
		return nil, s3err.ErrMalformedDate
	}

	// Query string.
	queryStr := req.URL.Query().Encode()

	// Get hashed Payload
	if signV4Values.Credential.scope.service != "s3" && hashedPayload == emptySHA256 && r.Body != nil {
		buf, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewBuffer(buf))
		b, _ := io.ReadAll(bytes.NewBuffer(buf))
		if len(b) != 0 {
			bodyHash := sha256.Sum256(b)
			hashedPayload = hex.EncodeToString(bodyHash[:])
		}
	}

	if forwardedPrefix := r.Header.Get("X-Forwarded-Prefix"); forwardedPrefix != "" {
		// Handling usage of reverse proxy at prefix.
		// Trying with prefix before main path.

		// Get canonical request.
		glog.V(4).Infof("Forwarded Prefix: %s", forwardedPrefix)

		canonicalRequest := getCanonicalRequest(extractedSignedHeaders, hashedPayload, queryStr, forwardedPrefix+req.URL.Path, req.Method)
		errCode = iam.genAndCompareSignatureV4(canonicalRequest, cred.SecretKey, t, signV4Values)
		if errCode == s3err.ErrNone {
			return identity, errCode
		}
	}

	// Get canonical request.
	canonicalRequest := getCanonicalRequest(extractedSignedHeaders, hashedPayload, queryStr, req.URL.Path, req.Method)

	errCode = iam.genAndCompareSignatureV4(canonicalRequest, cred.SecretKey, t, signV4Values)

	if errCode == s3err.ErrNone {
		return identity, errCode
	}
	return nil, errCode
}

// Generate and compare signature for request.
func (iam *IdentityAccessManagement) genAndCompareSignatureV4(canonicalRequest, secretKey string, t time.Time, signV4Values signValues) s3err.ErrorCode {
	// Get string to sign from canonical request.
	stringToSign := getStringToSign(canonicalRequest, t, signV4Values.Credential.getScope())
	glog.V(4).Infof("String to Sign:\n%s", stringToSign)
	// Calculate signature.
	newSignature := iam.getSignature(
		secretKey,
		signV4Values.Credential.scope.date,
		signV4Values.Credential.scope.region,
		signV4Values.Credential.scope.service,
		stringToSign,
	)
	glog.V(4).Infof("Signature:\n%s", newSignature)

	// Verify if signature match.
	if !compareSignatureV4(newSignature, signV4Values.Signature) {
		return s3err.ErrSignatureDoesNotMatch
	}
	return s3err.ErrNone
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

// signValues data type represents structured form of AWS Signature V4 header.
type signValues struct {
	Credential    credentialHeader
	SignedHeaders []string
	Signature     string
}

// Return scope string.
func (c credentialHeader) getScope() string {
	return strings.Join([]string{
		c.scope.date.Format(yyyymmdd),
		c.scope.region,
		c.scope.service,
		c.scope.request,
	}, "/")
}

//	Authorization: algorithm Credential=accessKeyID/credScope, \
//	        SignedHeaders=signedHeaders, Signature=signature
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

// parse credentialHeader string into its structured form.
func parseCredentialHeader(credElement string) (ch credentialHeader, aec s3err.ErrorCode) {
	creds := strings.Split(strings.TrimSpace(credElement), "=")
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

// doesPolicySignatureV4Match - Verify query headers with post policy
//   - http://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-HTTPPOSTConstructPolicy.html
//
// returns ErrNone if the signature matches.
func (iam *IdentityAccessManagement) doesPolicySignatureV4Match(formValues http.Header) s3err.ErrorCode {

	// Parse credential tag.
	credHeader, err := parseCredentialHeader("Credential=" + formValues.Get("X-Amz-Credential"))
	if err != s3err.ErrNone {
		return s3err.ErrMissingFields
	}

	_, cred, found := iam.lookupByAccessKey(credHeader.accessKey)
	if !found {
		return s3err.ErrInvalidAccessKeyID
	}

	// Get signature.
	newSignature := iam.getSignature(
		cred.SecretKey,
		credHeader.scope.date,
		credHeader.scope.region,
		credHeader.scope.service,
		formValues.Get("Policy"),
	)

	// Verify signature.
	if !compareSignatureV4(newSignature, formValues.Get("X-Amz-Signature")) {
		return s3err.ErrSignatureDoesNotMatch
	}

	// Success.
	return s3err.ErrNone
}

// check query headers with presigned signature
//   - http://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
func (iam *IdentityAccessManagement) doesPresignedSignatureMatch(hashedPayload string, r *http.Request) (*Identity, s3err.ErrorCode) {

	// Copy request
	req := *r

	// Parse request query string.
	pSignValues, err := parsePreSignV4(req.URL.Query())
	if err != s3err.ErrNone {
		return nil, err
	}

	// Verify if the access key id matches.
	identity, cred, found := iam.lookupByAccessKey(pSignValues.Credential.accessKey)
	if !found {
		return nil, s3err.ErrInvalidAccessKeyID
	}

	// Extract all the signed headers along with its values.
	extractedSignedHeaders, errCode := extractSignedHeaders(pSignValues.SignedHeaders, r)
	if errCode != s3err.ErrNone {
		return nil, errCode
	}
	// Construct new query.
	query := make(url.Values)
	if req.URL.Query().Get("X-Amz-Content-Sha256") != "" {
		query.Set("X-Amz-Content-Sha256", hashedPayload)
	}

	query.Set("X-Amz-Algorithm", signV4Algorithm)

	now := time.Now().UTC()

	// If the host which signed the request is slightly ahead in time (by less than globalMaxSkewTime) the
	// request should still be allowed.
	if pSignValues.Date.After(now.Add(15 * time.Minute)) {
		return nil, s3err.ErrRequestNotReadyYet
	}

	if now.Sub(pSignValues.Date) > pSignValues.Expires {
		return nil, s3err.ErrExpiredPresignRequest
	}

	// Save the date and expires.
	t := pSignValues.Date
	expireSeconds := int(pSignValues.Expires / time.Second)

	// Construct the query.
	query.Set("X-Amz-Date", t.Format(iso8601Format))
	query.Set("X-Amz-Expires", strconv.Itoa(expireSeconds))
	query.Set("X-Amz-SignedHeaders", getSignedHeaders(extractedSignedHeaders))
	query.Set("X-Amz-Credential", cred.AccessKey+"/"+getScope(t, pSignValues.Credential.scope.region))

	// Save other headers available in the request parameters.
	for k, v := range req.URL.Query() {
		// Skip AWS S3 authentication headers
		if _, ok := awsS3AuthHeaders[strings.ToLower(k)]; ok {
			continue
		}

		query[k] = v
	}

	// Get the encoded query.
	encodedQuery := query.Encode()

	// Verify if date query is same.
	if req.URL.Query().Get("X-Amz-Date") != query.Get("X-Amz-Date") {
		return nil, s3err.ErrSignatureDoesNotMatch
	}
	// Verify if expires query is same.
	if req.URL.Query().Get("X-Amz-Expires") != query.Get("X-Amz-Expires") {
		return nil, s3err.ErrSignatureDoesNotMatch
	}
	// Verify if signed headers query is same.
	if req.URL.Query().Get("X-Amz-SignedHeaders") != query.Get("X-Amz-SignedHeaders") {
		return nil, s3err.ErrSignatureDoesNotMatch
	}
	// Verify if credential query is same.
	if req.URL.Query().Get("X-Amz-Credential") != query.Get("X-Amz-Credential") {
		return nil, s3err.ErrSignatureDoesNotMatch
	}
	// Verify if sha256 payload query is same.
	if req.URL.Query().Get("X-Amz-Content-Sha256") != "" {
		if req.URL.Query().Get("X-Amz-Content-Sha256") != query.Get("X-Amz-Content-Sha256") {
			return nil, s3err.ErrContentSHA256Mismatch
		}
	}

	// / Verify finally if signature is same.

	// Get canonical request.
	presignedCanonicalReq := getCanonicalRequest(extractedSignedHeaders, hashedPayload, encodedQuery, req.URL.Path, req.Method)

	// Get string to sign from canonical request.
	presignedStringToSign := getStringToSign(presignedCanonicalReq, t, pSignValues.Credential.getScope())

	// Get new signature.
	newSignature := iam.getSignature(
		cred.SecretKey,
		pSignValues.Credential.scope.date,
		pSignValues.Credential.scope.region,
		pSignValues.Credential.scope.service,
		presignedStringToSign,
	)

	// Verify signature.
	if !compareSignatureV4(req.URL.Query().Get("X-Amz-Signature"), newSignature) {
		return nil, s3err.ErrSignatureDoesNotMatch
	}
	return identity, s3err.ErrNone
}

func (iam *IdentityAccessManagement) getSignature(secretKey string, t time.Time, region string, service string, stringToSign string) string {
	pool := iam.getSignatureHashPool(secretKey, t, region, service)
	h := pool.Get().(hash.Hash)
	defer pool.Put(h)

	h.Reset()
	h.Write([]byte(stringToSign))
	sig := hex.EncodeToString(h.Sum(nil))

	return sig
}

func (iam *IdentityAccessManagement) getSignatureHashPool(secretKey string, t time.Time, region string, service string) *sync.Pool {
	// Build a caching key for the pool.
	date := t.Format(yyyymmdd)
	hashID := "AWS4" + secretKey + "/" + date + "/" + region + "/" + service + "/" + "aws4_request"

	// Try to find an existing pool and return it.
	iam.hashMu.RLock()
	pool, ok := iam.hashes[hashID]
	iam.hashMu.RUnlock()

	if !ok {
		iam.hashMu.Lock()
		defer iam.hashMu.Unlock()
		pool, ok = iam.hashes[hashID]
	}

	if ok {
		atomic.StoreInt32(iam.hashCounters[hashID], 1)
		return pool
	}

	// Create a pool that returns HMAC hashers for the requested parameters to avoid expensive re-initializing
	// of new instances on every request.
	iam.hashes[hashID] = &sync.Pool{
		New: func() any {
			signingKey := getSigningKey(secretKey, date, region, service)
			return hmac.New(sha256.New, signingKey)
		},
	}
	iam.hashCounters[hashID] = new(int32)

	// Clean up unused pools automatically after one hour of inactivity
	ticker := time.NewTicker(time.Hour)
	go func() {
		for range ticker.C {
			old := atomic.SwapInt32(iam.hashCounters[hashID], 0)
			if old == 0 {
				break
			}
		}

		ticker.Stop()
		iam.hashMu.Lock()
		delete(iam.hashes, hashID)
		delete(iam.hashCounters, hashID)
		iam.hashMu.Unlock()
	}()

	return iam.hashes[hashID]
}

func contains(list []string, elem string) bool {
	for _, t := range list {
		if t == elem {
			return true
		}
	}
	return false
}

// preSignValues data type represents structured form of AWS Signature V4 query string.
type preSignValues struct {
	signValues
	Date    time.Time
	Expires time.Duration
}

// Parses signature version '4' query string of the following form.
//
//	querystring = X-Amz-Algorithm=algorithm
//	querystring += &X-Amz-Credential= urlencode(accessKey + '/' + credential_scope)
//	querystring += &X-Amz-Date=date
//	querystring += &X-Amz-Expires=timeout interval
//	querystring += &X-Amz-SignedHeaders=signed_headers
//	querystring += &X-Amz-Signature=signature
//
// verifies if any of the necessary query params are missing in the presigned request.
func doesV4PresignParamsExist(query url.Values) s3err.ErrorCode {
	v4PresignQueryParams := []string{"X-Amz-Algorithm", "X-Amz-Credential", "X-Amz-Signature", "X-Amz-Date", "X-Amz-SignedHeaders", "X-Amz-Expires"}
	for _, v4PresignQueryParam := range v4PresignQueryParams {
		if _, ok := query[v4PresignQueryParam]; !ok {
			return s3err.ErrInvalidQueryParams
		}
	}
	return s3err.ErrNone
}

// Parses all the presigned signature values into separate elements.
func parsePreSignV4(query url.Values) (psv preSignValues, aec s3err.ErrorCode) {
	var err s3err.ErrorCode
	// verify whether the required query params exist.
	err = doesV4PresignParamsExist(query)
	if err != s3err.ErrNone {
		return psv, err
	}

	// Verify if the query algorithm is supported or not.
	if query.Get("X-Amz-Algorithm") != signV4Algorithm {
		return psv, s3err.ErrInvalidQuerySignatureAlgo
	}

	// Initialize signature version '4' structured header.
	preSignV4Values := preSignValues{}

	// Save credential.
	preSignV4Values.Credential, err = parseCredentialHeader("Credential=" + query.Get("X-Amz-Credential"))
	if err != s3err.ErrNone {
		return psv, err
	}

	var e error
	// Save date in native time.Time.
	preSignV4Values.Date, e = time.Parse(iso8601Format, query.Get("X-Amz-Date"))
	if e != nil {
		return psv, s3err.ErrMalformedPresignedDate
	}

	// Save expires in native time.Duration.
	preSignV4Values.Expires, e = time.ParseDuration(query.Get("X-Amz-Expires") + "s")
	if e != nil {
		return psv, s3err.ErrMalformedExpires
	}

	if preSignV4Values.Expires < 0 {
		return psv, s3err.ErrNegativeExpires
	}

	// Check if Expiry time is less than 7 days (value in seconds).
	if preSignV4Values.Expires.Seconds() > 604800 {
		return psv, s3err.ErrMaximumExpires
	}

	// Save signed headers.
	preSignV4Values.SignedHeaders, err = parseSignedHeader("SignedHeaders=" + query.Get("X-Amz-SignedHeaders"))
	if err != s3err.ErrNone {
		return psv, err
	}

	// Save signature.
	preSignV4Values.Signature, err = parseSignature("Signature=" + query.Get("X-Amz-Signature"))
	if err != s3err.ErrNone {
		return psv, err
	}

	// Return structured form of signature query string.
	return preSignV4Values, s3err.ErrNone
}

// extractSignedHeaders extract signed headers from Authorization header
func extractSignedHeaders(signedHeaders []string, r *http.Request) (http.Header, s3err.ErrorCode) {
	reqHeaders := r.Header
	// find whether "host" is part of list of signed headers.
	// if not return ErrUnsignedHeaders. "host" is mandatory.
	if !contains(signedHeaders, "host") {
		return nil, s3err.ErrUnsignedHeaders
	}
	extractedSignedHeaders := make(http.Header)
	for _, header := range signedHeaders {
		// `host` will not be found in the headers, can be found in r.Host.
		// but its alway necessary that the list of signed headers containing host in it.
		val, ok := reqHeaders[http.CanonicalHeaderKey(header)]
		if ok {
			for _, enc := range val {
				extractedSignedHeaders.Add(header, enc)
			}
			continue
		}
		switch header {
		case "expect":
			// Set the default value of the Expect header for compatibility.
			//
			// In NGINX v1.1, the Expect header is removed when handling 100-continue requests.
			//
			// `aws-cli` sets this as part of signed headers.
			//
			// According to
			// http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.20
			// Expect header is always of form:
			//
			//   Expect       =  "Expect" ":" 1#expectation
			//   expectation  =  "100-continue" | expectation-extension
			//
			// So it safe to assume that '100-continue' is what would
			// be sent, for the time being keep this work around.
			extractedSignedHeaders.Set(header, "100-continue")
		case "host":
			extractedHost := extractHostHeader(r)
			extractedSignedHeaders.Set(header, extractedHost)
		case "transfer-encoding":
			for _, enc := range r.TransferEncoding {
				extractedSignedHeaders.Add(header, enc)
			}
		case "content-length":
			// Signature-V4 spec excludes Content-Length from signed headers list for signature calculation.
			// But some clients deviate from this rule. Hence we consider Content-Length for signature
			// calculation to be compatible with such clients.
			extractedSignedHeaders.Set(header, strconv.FormatInt(r.ContentLength, 10))
		default:
			return nil, s3err.ErrUnsignedHeaders
		}
	}
	return extractedSignedHeaders, s3err.ErrNone
}

func extractHostHeader(r *http.Request) string {

	forwardedHost := r.Header.Get("X-Forwarded-Host")
	forwardedPort := r.Header.Get("X-Forwarded-Port")

	// If X-Forwarded-Host is set, use that as the host.
	// If X-Forwarded-Port is set, use that too to form the host.
	if forwardedHost != "" {
		extractedHost := forwardedHost
		host, port, err := net.SplitHostPort(extractedHost)
		if err == nil {
			extractedHost = host
			if forwardedPort == "" {
				forwardedPort = port
			}
		}
		if !isDefaultPort(r.URL.Scheme, forwardedPort) {
			extractedHost = net.JoinHostPort(extractedHost, forwardedPort)
		}
		return extractedHost
	} else {
		// Go http server removes "host" from Request.Header
		host := r.Host
		if host == "" {
			host = r.URL.Host
		}
		h, port, err := net.SplitHostPort(host)
		if err != nil {
			return host
		}
		if isDefaultPort(r.URL.Scheme, port) {
			return h
		}
		return host
	}
}

func isDefaultPort(scheme, port string) bool {
	if port == "" {
		return true
	}

	lowerCaseScheme := strings.ToLower(scheme)
	return (lowerCaseScheme == "http" && port == "80") ||
		(lowerCaseScheme == "https" && port == "443")
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

// getScope generate a string of a specific date, an AWS region, and a service.
func getScope(t time.Time, region string) string {
	scope := strings.Join([]string{
		t.Format(yyyymmdd),
		region,
		"s3",
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

	glog.V(4).Infof("Canonical Request:\n%s", canonicalRequest)
	return canonicalRequest
}

// getStringToSign a string based on selected query values.
func getStringToSign(canonicalRequest string, t time.Time, scope string) string {
	stringToSign := signV4Algorithm + "\n" + t.Format(iso8601Format) + "\n"
	stringToSign = stringToSign + scope + "\n"
	canonicalRequestBytes := sha256.Sum256([]byte(canonicalRequest))
	stringToSign = stringToSign + hex.EncodeToString(canonicalRequestBytes[:])
	return stringToSign
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
		headers = append(headers, strings.ToLower(k))
		vals[strings.ToLower(k)] = vv
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

// Trim leading and trailing spaces and replace sequential spaces with one space, following Trimall()
// in http://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html
func signV4TrimAll(input string) string {
	// Compress adjacent spaces (a space is determined by
	// unicode.IsSpace() internally here) to one space and return
	return strings.Join(strings.Fields(input), " ")
}

// if object matches reserved string, no need to encode them
var reservedObjectNames = regexp.MustCompile("^[a-zA-Z0-9-_.~/]+$")

// EncodePath encode the strings from UTF-8 byte representations to HTML hex escape sequences
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
			continue
		}
		switch s {
		case '-', '_', '.', '~', '/': // §2.3 Unreserved characters (mark)
			encodedPathname = encodedPathname + string(s)
			continue
		default:
			len := utf8.RuneLen(s)
			if len < 0 {
				// if utf8 cannot convert return the same string as is
				return pathName
			}
			u := make([]byte, len)
			utf8.EncodeRune(u, s)
			for _, r := range u {
				hex := hex.EncodeToString([]byte{r})
				encodedPathname = encodedPathname + "%" + strings.ToUpper(hex)
			}
		}
	}
	return encodedPathname
}

// compareSignatureV4 returns true if and only if both signatures
// are equal. The signatures are expected to be HEX encoded strings
// according to the AWS S3 signature V4 spec.
func compareSignatureV4(sig1, sig2 string) bool {
	// The CTC using []byte(str) works because the hex encoding
	// is unique for a sequence of bytes. See also compareSignatureV2.
	return subtle.ConstantTimeCompare([]byte(sig1), []byte(sig2)) == 1
}
