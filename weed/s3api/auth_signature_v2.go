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
	"crypto/hmac"
	"crypto/sha1"
	"crypto/subtle"
	"encoding/base64"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
)

// Whitelist resource list that will be used in query string for signature-V2 calculation.
// The list should be alphabetically sorted
var resourceList = []string{
	"acl",
	"delete",
	"lifecycle",
	"location",
	"logging",
	"notification",
	"partNumber",
	"policy",
	"requestPayment",
	"response-cache-control",
	"response-content-disposition",
	"response-content-encoding",
	"response-content-language",
	"response-content-type",
	"response-expires",
	"torrent",
	"uploadId",
	"uploads",
	"versionId",
	"versioning",
	"versions",
	"website",
}

// Verify if request has AWS Signature Version '2'.
func (iam *IdentityAccessManagement) isReqAuthenticatedV2(r *http.Request) (*Identity, s3err.ErrorCode) {
	if isRequestSignatureV2(r) {
		return iam.doesSignV2Match(r)
	}
	return iam.doesPresignV2SignatureMatch(r)
}

func (iam *IdentityAccessManagement) doesPolicySignatureV2Match(formValues http.Header) s3err.ErrorCode {

	accessKey := formValues.Get("AWSAccessKeyId")
	if accessKey == "" {
		return s3err.ErrMissingFields
	}

	identity, cred, found := iam.lookupByAccessKey(accessKey)
	if !found {
		return s3err.ErrInvalidAccessKeyID
	}

	bucket := formValues.Get("bucket")
	if !identity.canDo(s3_constants.ACTION_WRITE, bucket, "") {
		return s3err.ErrAccessDenied
	}

	policy := formValues.Get("Policy")
	if policy == "" {
		return s3err.ErrMissingFields
	}

	signature := formValues.Get("Signature")
	if signature == "" {
		return s3err.ErrMissingFields
	}

	if !compareSignatureV2(signature, calculateSignatureV2(policy, cred.SecretKey)) {
		return s3err.ErrSignatureDoesNotMatch
	}
	return s3err.ErrNone
}

// doesSignV2Match - Verify authorization header with calculated header in accordance with
//   - http://docs.aws.amazon.com/AmazonS3/latest/dev/auth-request-sig-v2.html
//
// returns ErrNone if the signature matches.
func (iam *IdentityAccessManagement) doesSignV2Match(r *http.Request) (*Identity, s3err.ErrorCode) {
	v2Auth := r.Header.Get("Authorization")
	accessKey, errCode := validateV2AuthHeader(v2Auth)
	if errCode != s3err.ErrNone {
		return nil, errCode
	}

	identity, cred, found := iam.lookupByAccessKey(accessKey)
	if !found {
		return nil, s3err.ErrInvalidAccessKeyID
	}

	expectedAuth := signatureV2(cred, r.Method, r.URL.Path, r.URL.Query().Encode(), r.Header)
	if v2Auth != expectedAuth {
		return nil, s3err.ErrSignatureDoesNotMatch
	}
	return identity, s3err.ErrNone
}

// doesPresignV2SignatureMatch - Verify query headers with calculated header in accordance with
//   - http://docs.aws.amazon.com/AmazonS3/latest/dev/auth-request-sig-v2.html
//
// returns ErrNone if the signature matches.
func (iam *IdentityAccessManagement) doesPresignV2SignatureMatch(r *http.Request) (*Identity, s3err.ErrorCode) {
	query := r.URL.Query()
	expires := query.Get("Expires")
	if expires == "" {
		return nil, s3err.ErrMissingFields
	}

	expireTimestamp, err := strconv.ParseInt(expires, 10, 64)
	if err != nil {
		return nil, s3err.ErrMalformedExpires
	}

	if time.Unix(expireTimestamp, 0).Before(time.Now().UTC()) {
		return nil, s3err.ErrExpiredPresignRequest
	}

	accessKey := query.Get("AWSAccessKeyId")
	if accessKey == "" {
		return nil, s3err.ErrInvalidAccessKeyID
	}

	signature := query.Get("Signature")
	if signature == "" {
		return nil, s3err.ErrMissingFields
	}

	identity, cred, found := iam.lookupByAccessKey(accessKey)
	if !found {
		return nil, s3err.ErrInvalidAccessKeyID
	}

	expectedSignature := preSignatureV2(cred, r.Method, r.URL.Path, r.URL.Query().Encode(), r.Header, expires)
	if !compareSignatureV2(signature, expectedSignature) {
		return nil, s3err.ErrSignatureDoesNotMatch
	}
	return identity, s3err.ErrNone
}

// validateV2AuthHeader validates AWS Signature Version '2' authentication header.
func validateV2AuthHeader(v2Auth string) (accessKey string, errCode s3err.ErrorCode) {
	if v2Auth == "" {
		return "", s3err.ErrAuthHeaderEmpty
	}

	// Signature V2 authorization header format:
	// Authorization: AWS AKIAIOSFODNN7EXAMPLE:frJIUN8DYpKDtOLCwo//yllqDzg=
	if !strings.HasPrefix(v2Auth, signV2Algorithm) {
		return "", s3err.ErrSignatureVersionNotSupported
	}

	// Strip off the Algorithm prefix.
	v2Auth = v2Auth[len(signV2Algorithm):]
	authFields := strings.Split(v2Auth, ":")
	if len(authFields) != 2 {
		return "", s3err.ErrMissingFields
	}

	// The first field is Access Key ID.
	if authFields[0] == "" {
		return "", s3err.ErrInvalidAccessKeyID
	}

	// The second field is signature.
	if authFields[1] == "" {
		return "", s3err.ErrMissingFields
	}

	return strings.TrimLeft(authFields[0], " "), s3err.ErrNone
}

// signatureV2 - calculates signature version 2 for request.
func signatureV2(cred *Credential, method string, encodedResource string, encodedQuery string, headers http.Header) string {
	stringToSign := getStringToSignV2(method, encodedResource, encodedQuery, headers, "")
	signature := calculateSignatureV2(stringToSign, cred.SecretKey)
	return signV2Algorithm + " " + cred.AccessKey + ":" + signature
}

// getStringToSignV2 - string to sign in accordance with
//   - http://docs.aws.amazon.com/AmazonS3/latest/dev/auth-request-sig-v2.html
func getStringToSignV2(method string, encodedResource, encodedQuery string, headers http.Header, expires string) string {
	canonicalHeaders := canonicalizedAmzHeadersV2(headers)
	if len(canonicalHeaders) > 0 {
		canonicalHeaders += "\n"
	}

	// From the Amazon docs:
	//
	// StringToSign = HTTP-Verb + "\n" +
	// 	 Content-MD5 + "\n" +
	//	 Content-Type + "\n" +
	//	 Date + "\n" +
	//	 CanonicalizedAmzHeaders +
	//	 CanonicalizedResource;
	stringToSign := method + "\n"
	stringToSign += headers.Get("Content-Md5") + "\n"
	stringToSign += headers.Get("Content-Type") + "\n"

	if expires != "" {
		stringToSign += expires + "\n"
	} else {
		stringToSign += headers.Get("Date") + "\n"
		if v := headers.Get("x-amz-date"); v != "" {
			stringToSign = strings.Replace(stringToSign, headers.Get("Date")+"\n", "\n", -1)
		}
	}
	stringToSign += canonicalHeaders
	stringToSign += canonicalizedResourceV2(encodedResource, encodedQuery)
	return stringToSign
}

// canonicalizedResourceV2 - canonicalize the resource string for signature V2.
func canonicalizedResourceV2(encodedResource, encodedQuery string) string {
	queries := strings.Split(encodedQuery, "&")
	keyval := make(map[string]string)
	for _, query := range queries {
		key := query
		val := ""
		index := strings.Index(query, "=")
		if index != -1 {
			key = query[:index]
			val = query[index+1:]
		}
		keyval[key] = val
	}

	var canonicalQueries []string
	for _, resource := range resourceList {
		if val, ok := keyval[resource]; ok {
			if val == "" {
				canonicalQueries = append(canonicalQueries, resource)
				continue
			}
			canonicalQueries = append(canonicalQueries, resource+"="+val)
		}
	}

	// The queries will be already sorted as resourceList is sorted.
	if len(canonicalQueries) == 0 {
		return encodedResource
	}

	// If queries are present then the canonicalized resource is set to encodedResource + "?" + strings.Join(canonicalQueries, "&")
	return encodedResource + "?" + strings.Join(canonicalQueries, "&")
}

// canonicalizedAmzHeadersV2 - canonicalize the x-amz-* headers for signature V2.
func canonicalizedAmzHeadersV2(headers http.Header) string {
	var keys []string
	keyval := make(map[string]string)
	for key := range headers {
		lkey := strings.ToLower(key)
		if !strings.HasPrefix(lkey, "x-amz-") {
			continue
		}
		keys = append(keys, lkey)
		keyval[lkey] = strings.Join(headers[key], ",")
	}
	sort.Strings(keys)

	var canonicalHeaders []string
	for _, key := range keys {
		canonicalHeaders = append(canonicalHeaders, key+":"+keyval[key])
	}
	return strings.Join(canonicalHeaders, "\n")
}

// calculateSignatureV2 - calculates signature version 2.
func calculateSignatureV2(stringToSign string, secret string) string {
	hm := hmac.New(sha1.New, []byte(secret))
	hm.Write([]byte(stringToSign))
	return base64.StdEncoding.EncodeToString(hm.Sum(nil))
}

// compareSignatureV2 returns true if and only if both signatures
// are equal. The signatures are expected to be base64 encoded strings
// according to the AWS S3 signature V2 spec.
func compareSignatureV2(sig1, sig2 string) bool {
	// Decode signature string to binary byte-sequence representation is required
	// as Base64 encoding of a value is not unique:
	// For example "aGVsbG8=" and "aGVsbG8=\r" will result in the same byte slice.
	signature1, err := base64.StdEncoding.DecodeString(sig1)
	if err != nil {
		return false
	}
	signature2, err := base64.StdEncoding.DecodeString(sig2)
	if err != nil {
		return false
	}
	return subtle.ConstantTimeCompare(signature1, signature2) == 1
}

// Return signature-v2 for the presigned request.
func preSignatureV2(cred *Credential, method string, encodedResource string, encodedQuery string, headers http.Header, expires string) string {
	stringToSign := getStringToSignV2(method, encodedResource, encodedQuery, headers, expires)
	return calculateSignatureV2(stringToSign, cred.SecretKey)
}
