package policy

/*
 * MinIO Cloud Storage, (C) 2016, 2017, 2018 MinIO, Inc.
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

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	iso8601DateFormat = "20060102T150405Z"
	iso8601TimeFormat = "2006-01-02T15:04:05.000Z" // Reply date format with nanosecond precision.
)

func newPostPolicyBytesV4WithContentRange(credential, bucketName, objectKey string, expiration time.Time) []byte {
	t := time.Now().UTC()
	// Add the expiration date.
	expirationStr := fmt.Sprintf(`"expiration": "%s"`, expiration.Format(iso8601TimeFormat))
	// Add the bucket condition, only accept buckets equal to the one passed.
	bucketConditionStr := fmt.Sprintf(`["eq", "$bucket", "%s"]`, bucketName)
	// Add the key condition, only accept keys equal to the one passed.
	keyConditionStr := fmt.Sprintf(`["eq", "$key", "%s/upload.txt"]`, objectKey)
	// Add content length condition, only accept content sizes of a given length.
	contentLengthCondStr := `["content-length-range", 1024, 1048576]`
	// Add the algorithm condition, only accept AWS SignV4 Sha256.
	algorithmConditionStr := `["eq", "$x-amz-algorithm", "AWS4-HMAC-SHA256"]`
	// Add the date condition, only accept the current date.
	dateConditionStr := fmt.Sprintf(`["eq", "$x-amz-date", "%s"]`, t.Format(iso8601DateFormat))
	// Add the credential string, only accept the credential passed.
	credentialConditionStr := fmt.Sprintf(`["eq", "$x-amz-credential", "%s"]`, credential)
	// Add the meta-uuid string, set to 1234
	uuidConditionStr := fmt.Sprintf(`["eq", "$x-amz-meta-uuid", "%s"]`, "1234")

	// Combine all conditions into one string.
	conditionStr := fmt.Sprintf(`"conditions":[%s, %s, %s, %s, %s, %s, %s]`, bucketConditionStr,
		keyConditionStr, contentLengthCondStr, algorithmConditionStr, dateConditionStr, credentialConditionStr, uuidConditionStr)
	retStr := "{"
	retStr = retStr + expirationStr + ","
	retStr = retStr + conditionStr
	retStr = retStr + "}"

	return []byte(retStr)
}

// newPostPolicyBytesV4 - creates a bare bones postpolicy string with key and bucket matches.
func newPostPolicyBytesV4(credential, bucketName, objectKey string, expiration time.Time) []byte {
	t := time.Now().UTC()
	// Add the expiration date.
	expirationStr := fmt.Sprintf(`"expiration": "%s"`, expiration.Format(iso8601TimeFormat))
	// Add the bucket condition, only accept buckets equal to the one passed.
	bucketConditionStr := fmt.Sprintf(`["eq", "$bucket", "%s"]`, bucketName)
	// Add the key condition, only accept keys equal to the one passed.
	keyConditionStr := fmt.Sprintf(`["eq", "$key", "%s/upload.txt"]`, objectKey)
	// Add the algorithm condition, only accept AWS SignV4 Sha256.
	algorithmConditionStr := `["eq", "$x-amz-algorithm", "AWS4-HMAC-SHA256"]`
	// Add the date condition, only accept the current date.
	dateConditionStr := fmt.Sprintf(`["eq", "$x-amz-date", "%s"]`, t.Format(iso8601DateFormat))
	// Add the credential string, only accept the credential passed.
	credentialConditionStr := fmt.Sprintf(`["eq", "$x-amz-credential", "%s"]`, credential)
	// Add the meta-uuid string, set to 1234
	uuidConditionStr := fmt.Sprintf(`["eq", "$x-amz-meta-uuid", "%s"]`, "1234")

	// Combine all conditions into one string.
	conditionStr := fmt.Sprintf(`"conditions":[%s, %s, %s, %s, %s, %s]`, bucketConditionStr, keyConditionStr, algorithmConditionStr, dateConditionStr, credentialConditionStr, uuidConditionStr)
	retStr := "{"
	retStr = retStr + expirationStr + ","
	retStr = retStr + conditionStr
	retStr = retStr + "}"

	return []byte(retStr)
}

// newPostPolicyBytesV2 - creates a bare bones postpolicy string with key and bucket matches.
func newPostPolicyBytesV2(bucketName, objectKey string, expiration time.Time) []byte {
	// Add the expiration date.
	expirationStr := fmt.Sprintf(`"expiration": "%s"`, expiration.Format(iso8601TimeFormat))
	// Add the bucket condition, only accept buckets equal to the one passed.
	bucketConditionStr := fmt.Sprintf(`["eq", "$bucket", "%s"]`, bucketName)
	// Add the key condition, only accept keys equal to the one passed.
	keyConditionStr := fmt.Sprintf(`["starts-with", "$key", "%s/upload.txt"]`, objectKey)

	// Combine all conditions into one string.
	conditionStr := fmt.Sprintf(`"conditions":[%s, %s]`, bucketConditionStr, keyConditionStr)
	retStr := "{"
	retStr = retStr + expirationStr + ","
	retStr = retStr + conditionStr
	retStr = retStr + "}"

	return []byte(retStr)
}

// Wrapper for calling TestPostPolicyBucketHandler tests for both Erasure multiple disks and single node setup.

// testPostPolicyBucketHandler - Tests validate post policy handler uploading objects.

// Wrapper for calling TestPostPolicyBucketHandlerRedirect tests for both Erasure multiple disks and single node setup.

// testPostPolicyBucketHandlerRedirect tests POST Object when success_action_redirect is specified

// postPresignSignatureV4 - presigned signature for PostPolicy requests.
func postPresignSignatureV4(policyBase64 string, t time.Time, secretAccessKey, location string) string {
	// Get signing key.
	signingkey := getSigningKey(secretAccessKey, t, location)
	// Calculate signature.
	signature := getSignature(signingkey, policyBase64)
	return signature
}

// copied from auth_signature_v4.go to break import loop
// sumHMAC calculate hmac between two input byte array.
func sumHMAC(key []byte, data []byte) []byte {
	hash := hmac.New(sha256.New, key)
	hash.Write(data)
	return hash.Sum(nil)
}

// copied from auth_signature_v4.go to break import loop
// getSigningKey hmac seed to calculate final signature.
func getSigningKey(secretKey string, t time.Time, region string) []byte {
	date := sumHMAC([]byte("AWS4"+secretKey), []byte(t.Format("20060102")))
	regionBytes := sumHMAC(date, []byte(region))
	service := sumHMAC(regionBytes, []byte("s3"))
	signingKey := sumHMAC(service, []byte("aws4_request"))
	return signingKey
}

// copied from auth_signature_v4.go to break import loop
// getSignature final signature in hexadecimal form.
func getSignature(signingKey []byte, stringToSign string) string {
	return hex.EncodeToString(sumHMAC(signingKey, []byte(stringToSign)))
}

// copied from auth_signature_v4.go to break import loop
func calculateSignatureV2(stringToSign string, secret string) string {
	hm := hmac.New(sha1.New, []byte(secret))
	hm.Write([]byte(stringToSign))
	return base64.StdEncoding.EncodeToString(hm.Sum(nil))
}

func newPostRequestV2(endPoint, bucketName, objectName string, accessKey, secretKey string) (*http.Request, error) {
	// Expire the request five minutes from now.
	expirationTime := time.Now().UTC().Add(time.Minute * 5)
	// Create a new post policy.
	policy := newPostPolicyBytesV2(bucketName, objectName, expirationTime)
	// Only need the encoding.
	encodedPolicy := base64.StdEncoding.EncodeToString(policy)

	// Presign with V4 signature based on the policy.
	signature := calculateSignatureV2(encodedPolicy, secretKey)

	formData := map[string]string{
		"AWSAccessKeyId": accessKey,
		"bucket":         bucketName,
		"key":            objectName + "/${filename}",
		"policy":         encodedPolicy,
		"signature":      signature,
	}

	// Create the multipart form.
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)

	// Set the normal formData
	for k, v := range formData {
		w.WriteField(k, v)
	}
	// Set the File formData
	writer, err := w.CreateFormFile("file", "upload.txt")
	if err != nil {
		// return nil, err
		return nil, err
	}
	writer.Write([]byte("hello world"))
	// Close before creating the new request.
	w.Close()

	// Set the body equal to the created policy.
	reader := bytes.NewReader(buf.Bytes())

	req, err := http.NewRequest(http.MethodPost, makeTestTargetURL(endPoint, bucketName, "", nil), reader)
	if err != nil {
		return nil, err
	}

	// Set form content-type.
	req.Header.Set("Content-Type", w.FormDataContentType())
	return req, nil
}

func buildGenericPolicy(t time.Time, accessKey, region, bucketName, objectName string, contentLengthRange bool) []byte {
	// Expire the request five minutes from now.
	expirationTime := t.Add(time.Minute * 5)

	credStr := getCredentialString(accessKey, region, t)
	// Create a new post policy.
	policy := newPostPolicyBytesV4(credStr, bucketName, objectName, expirationTime)
	if contentLengthRange {
		policy = newPostPolicyBytesV4WithContentRange(credStr, bucketName, objectName, expirationTime)
	}
	return policy
}

func newPostRequestV4Generic(endPoint, bucketName, objectName string, objData []byte, accessKey, secretKey string, region string,
	t time.Time, policy []byte, addFormData map[string]string, corruptedB64 bool, corruptedMultipart bool) (*http.Request, error) {
	// Get the user credential.
	credStr := getCredentialString(accessKey, region, t)

	// Only need the encoding.
	encodedPolicy := base64.StdEncoding.EncodeToString(policy)

	if corruptedB64 {
		encodedPolicy = "%!~&" + encodedPolicy
	}

	// Presign with V4 signature based on the policy.
	signature := postPresignSignatureV4(encodedPolicy, t, secretKey, region)

	formData := map[string]string{
		"bucket":           bucketName,
		"key":              objectName + "/${filename}",
		"x-amz-credential": credStr,
		"policy":           encodedPolicy,
		"x-amz-signature":  signature,
		"x-amz-date":       t.Format(iso8601DateFormat),
		"x-amz-algorithm":  "AWS4-HMAC-SHA256",
		"x-amz-meta-uuid":  "1234",
		"Content-Encoding": "gzip",
	}

	// Add form data
	for k, v := range addFormData {
		formData[k] = v
	}

	// Create the multipart form.
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)

	// Set the normal formData
	for k, v := range formData {
		w.WriteField(k, v)
	}
	// Set the File formData but don't if we want send an incomplete multipart request
	if !corruptedMultipart {
		writer, err := w.CreateFormFile("file", "upload.txt")
		if err != nil {
			// return nil, err
			return nil, err
		}
		writer.Write(objData)
		// Close before creating the new request.
		w.Close()
	}

	// Set the body equal to the created policy.
	reader := bytes.NewReader(buf.Bytes())

	req, err := http.NewRequest(http.MethodPost, makeTestTargetURL(endPoint, bucketName, "", nil), reader)
	if err != nil {
		return nil, err
	}

	// Set form content-type.
	req.Header.Set("Content-Type", w.FormDataContentType())
	return req, nil
}

func newPostRequestV4WithContentLength(endPoint, bucketName, objectName string, objData []byte, accessKey, secretKey string) (*http.Request, error) {
	t := time.Now().UTC()
	region := "us-east-1"
	policy := buildGenericPolicy(t, accessKey, region, bucketName, objectName, true)
	return newPostRequestV4Generic(endPoint, bucketName, objectName, objData, accessKey, secretKey, region, t, policy, nil, false, false)
}

func newPostRequestV4(endPoint, bucketName, objectName string, objData []byte, accessKey, secretKey string) (*http.Request, error) {
	t := time.Now().UTC()
	region := "us-east-1"
	policy := buildGenericPolicy(t, accessKey, region, bucketName, objectName, false)
	return newPostRequestV4Generic(endPoint, bucketName, objectName, objData, accessKey, secretKey, region, t, policy, nil, false, false)
}

// construct URL for http requests for bucket operations.
func makeTestTargetURL(endPoint, bucketName, objectName string, queryValues url.Values) string {
	urlStr := endPoint + "/"
	if bucketName != "" {
		urlStr = urlStr + bucketName + "/"
	}
	if objectName != "" {
		urlStr = urlStr + EncodePath(objectName)
	}
	if len(queryValues) > 0 {
		urlStr = urlStr + "?" + queryValues.Encode()
	}
	return urlStr
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
func EncodePath(pathName string) string {
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

// getCredentialString generate a credential string.
func getCredentialString(accessKeyID, location string, t time.Time) string {
	return accessKeyID + "/" + getScope(t, location)
}

// getScope generate a string of a specific date, an AWS region, and a service.
func getScope(t time.Time, region string) string {
	scope := strings.Join([]string{
		t.Format("20060102"),
		region,
		string("s3"),
		"aws4_request",
	}, "/")
	return scope
}
