package s3api

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/s3api/s3err"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

// TestIsRequestPresignedSignatureV4 - Test validates the logic for presign signature verision v4 detection.
func TestIsRequestPresignedSignatureV4(t *testing.T) {
	testCases := []struct {
		inputQueryKey   string
		inputQueryValue string
		expectedResult  bool
	}{
		// Test case - 1.
		// Test case with query key ""X-Amz-Credential" set.
		{"", "", false},
		// Test case - 2.
		{"X-Amz-Credential", "", true},
		// Test case - 3.
		{"X-Amz-Content-Sha256", "", false},
	}

	for i, testCase := range testCases {
		// creating an input HTTP request.
		// Only the query parameters are relevant for this particular test.
		inputReq, err := http.NewRequest("GET", "http://example.com", nil)
		if err != nil {
			t.Fatalf("Error initializing input HTTP request: %v", err)
		}
		q := inputReq.URL.Query()
		q.Add(testCase.inputQueryKey, testCase.inputQueryValue)
		inputReq.URL.RawQuery = q.Encode()

		actualResult := isRequestPresignedSignatureV4(inputReq)
		if testCase.expectedResult != actualResult {
			t.Errorf("Test %d: Expected the result to `%v`, but instead got `%v`", i+1, testCase.expectedResult, actualResult)
		}
	}
}

// Tests is requested authenticated function, tests replies for s3 errors.
func TestIsReqAuthenticated(t *testing.T) {
	option := S3ApiServerOption{}
	iam := NewIdentityAccessManagement(&option)
	iam.identities = []*Identity{
		{
			Name: "someone",
			Credentials: []*Credential{
				{
					AccessKey: "access_key_1",
					SecretKey: "secret_key_1",
				},
			},
			Actions: nil,
		},
	}

	// List of test cases for validating http request authentication.
	testCases := []struct {
		req     *http.Request
		s3Error s3err.ErrorCode
	}{
		// When request is unsigned, access denied is returned.
		{mustNewRequest("GET", "http://127.0.0.1:9000", 0, nil, t), s3err.ErrAccessDenied},
		// When request is properly signed, error is none.
		{mustNewSignedRequest("GET", "http://127.0.0.1:9000", 0, nil, t), s3err.ErrNone},
	}

	// Validates all testcases.
	for i, testCase := range testCases {
		if _, s3Error := iam.reqSignatureV4Verify(testCase.req); s3Error != testCase.s3Error {
			ioutil.ReadAll(testCase.req.Body)
			t.Fatalf("Test %d: Unexpected S3 error: want %d - got %d", i, testCase.s3Error, s3Error)
		}
	}
}

func TestCheckAdminRequestAuthType(t *testing.T) {
	option := S3ApiServerOption{}
	iam := NewIdentityAccessManagement(&option)
	iam.identities = []*Identity{
		{
			Name: "someone",
			Credentials: []*Credential{
				{
					AccessKey: "access_key_1",
					SecretKey: "secret_key_1",
				},
			},
			Actions: nil,
		},
	}

	testCases := []struct {
		Request *http.Request
		ErrCode s3err.ErrorCode
	}{
		{Request: mustNewRequest("GET", "http://127.0.0.1:9000", 0, nil, t), ErrCode: s3err.ErrAccessDenied},
		{Request: mustNewSignedRequest("GET", "http://127.0.0.1:9000", 0, nil, t), ErrCode: s3err.ErrNone},
		{Request: mustNewPresignedRequest("GET", "http://127.0.0.1:9000", 0, nil, t), ErrCode: s3err.ErrNone},
	}
	for i, testCase := range testCases {
		if _, s3Error := iam.reqSignatureV4Verify(testCase.Request); s3Error != testCase.ErrCode {
			t.Errorf("Test %d: Unexpected s3error returned wanted %d, got %d", i, testCase.ErrCode, s3Error)
		}
	}
}

// Provides a fully populated http request instance, fails otherwise.
func mustNewRequest(method string, urlStr string, contentLength int64, body io.ReadSeeker, t *testing.T) *http.Request {
	req, err := newTestRequest(method, urlStr, contentLength, body)
	if err != nil {
		t.Fatalf("Unable to initialize new http request %s", err)
	}
	return req
}

// This is similar to mustNewRequest but additionally the request
// is signed with AWS Signature V4, fails if not able to do so.
func mustNewSignedRequest(method string, urlStr string, contentLength int64, body io.ReadSeeker, t *testing.T) *http.Request {
	req := mustNewRequest(method, urlStr, contentLength, body, t)
	cred := &Credential{"access_key_1", "secret_key_1"}
	if err := signRequestV4(req, cred.AccessKey, cred.SecretKey); err != nil {
		t.Fatalf("Unable to inititalized new signed http request %s", err)
	}
	return req
}

// This is similar to mustNewRequest but additionally the request
// is presigned with AWS Signature V4, fails if not able to do so.
func mustNewPresignedRequest(method string, urlStr string, contentLength int64, body io.ReadSeeker, t *testing.T) *http.Request {
	req := mustNewRequest(method, urlStr, contentLength, body, t)
	cred := &Credential{"access_key_1", "secret_key_1"}
	if err := preSignV4(req, cred.AccessKey, cred.SecretKey, int64(10*time.Minute.Seconds())); err != nil {
		t.Fatalf("Unable to inititalized new signed http request %s", err)
	}
	return req
}

// Returns new HTTP request object.
func newTestRequest(method, urlStr string, contentLength int64, body io.ReadSeeker) (*http.Request, error) {
	if method == "" {
		method = "POST"
	}

	// Save for subsequent use
	var hashedPayload string
	var md5Base64 string
	switch {
	case body == nil:
		hashedPayload = getSHA256Hash([]byte{})
	default:
		payloadBytes, err := ioutil.ReadAll(body)
		if err != nil {
			return nil, err
		}
		hashedPayload = getSHA256Hash(payloadBytes)
		md5Base64 = getMD5HashBase64(payloadBytes)
	}
	// Seek back to beginning.
	if body != nil {
		body.Seek(0, 0)
	} else {
		body = bytes.NewReader([]byte(""))
	}
	req, err := http.NewRequest(method, urlStr, body)
	if err != nil {
		return nil, err
	}
	if md5Base64 != "" {
		req.Header.Set("Content-Md5", md5Base64)
	}
	req.Header.Set("x-amz-content-sha256", hashedPayload)

	// Add Content-Length
	req.ContentLength = contentLength

	return req, nil
}

// getSHA256Hash returns SHA-256 hash in hex encoding of given data.
func getSHA256Hash(data []byte) string {
	return hex.EncodeToString(getSHA256Sum(data))
}

// getMD5HashBase64 returns MD5 hash in base64 encoding of given data.
func getMD5HashBase64(data []byte) string {
	return base64.StdEncoding.EncodeToString(getMD5Sum(data))
}

// getSHA256Hash returns SHA-256 sum of given data.
func getSHA256Sum(data []byte) []byte {
	hash := sha256.New()
	hash.Write(data)
	return hash.Sum(nil)
}

// getMD5Sum returns MD5 sum of given data.
func getMD5Sum(data []byte) []byte {
	hash := md5.New()
	hash.Write(data)
	return hash.Sum(nil)
}

// getMD5Hash returns MD5 hash in hex encoding of given data.
func getMD5Hash(data []byte) string {
	return hex.EncodeToString(getMD5Sum(data))
}

var ignoredHeaders = map[string]bool{
	"Authorization":  true,
	"Content-Type":   true,
	"Content-Length": true,
	"User-Agent":     true,
}

// Sign given request using Signature V4.
func signRequestV4(req *http.Request, accessKey, secretKey string) error {
	// Get hashed payload.
	hashedPayload := req.Header.Get("x-amz-content-sha256")
	if hashedPayload == "" {
		return fmt.Errorf("Invalid hashed payload")
	}

	currTime := time.Now()

	// Set x-amz-date.
	req.Header.Set("x-amz-date", currTime.Format(iso8601Format))

	// Get header map.
	headerMap := make(map[string][]string)
	for k, vv := range req.Header {
		// If request header key is not in ignored headers, then add it.
		if _, ok := ignoredHeaders[http.CanonicalHeaderKey(k)]; !ok {
			headerMap[strings.ToLower(k)] = vv
		}
	}

	// Get header keys.
	headers := []string{"host"}
	for k := range headerMap {
		headers = append(headers, k)
	}
	sort.Strings(headers)

	region := "us-east-1"

	// Get canonical headers.
	var buf bytes.Buffer
	for _, k := range headers {
		buf.WriteString(k)
		buf.WriteByte(':')
		switch {
		case k == "host":
			buf.WriteString(req.URL.Host)
			fallthrough
		default:
			for idx, v := range headerMap[k] {
				if idx > 0 {
					buf.WriteByte(',')
				}
				buf.WriteString(v)
			}
			buf.WriteByte('\n')
		}
	}
	canonicalHeaders := buf.String()

	// Get signed headers.
	signedHeaders := strings.Join(headers, ";")

	// Get canonical query string.
	req.URL.RawQuery = strings.Replace(req.URL.Query().Encode(), "+", "%20", -1)

	// Get canonical URI.
	canonicalURI := EncodePath(req.URL.Path)

	// Get canonical request.
	// canonicalRequest =
	//  <HTTPMethod>\n
	//  <CanonicalURI>\n
	//  <CanonicalQueryString>\n
	//  <CanonicalHeaders>\n
	//  <SignedHeaders>\n
	//  <HashedPayload>
	//
	canonicalRequest := strings.Join([]string{
		req.Method,
		canonicalURI,
		req.URL.RawQuery,
		canonicalHeaders,
		signedHeaders,
		hashedPayload,
	}, "\n")

	// Get scope.
	scope := strings.Join([]string{
		currTime.Format(yyyymmdd),
		region,
		"s3",
		"aws4_request",
	}, "/")

	stringToSign := "AWS4-HMAC-SHA256" + "\n" + currTime.Format(iso8601Format) + "\n"
	stringToSign = stringToSign + scope + "\n"
	stringToSign = stringToSign + getSHA256Hash([]byte(canonicalRequest))

	date := sumHMAC([]byte("AWS4"+secretKey), []byte(currTime.Format(yyyymmdd)))
	regionHMAC := sumHMAC(date, []byte(region))
	service := sumHMAC(regionHMAC, []byte("s3"))
	signingKey := sumHMAC(service, []byte("aws4_request"))

	signature := hex.EncodeToString(sumHMAC(signingKey, []byte(stringToSign)))

	// final Authorization header
	parts := []string{
		"AWS4-HMAC-SHA256" + " Credential=" + accessKey + "/" + scope,
		"SignedHeaders=" + signedHeaders,
		"Signature=" + signature,
	}
	auth := strings.Join(parts, ", ")
	req.Header.Set("Authorization", auth)

	return nil
}

// preSignV4 presign the request, in accordance with
// http://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html.
func preSignV4(req *http.Request, accessKeyID, secretAccessKey string, expires int64) error {
	// Presign is not needed for anonymous credentials.
	if accessKeyID == "" || secretAccessKey == "" {
		return errors.New("Presign cannot be generated without access and secret keys")
	}

	region := "us-east-1"
	date := time.Now().UTC()
	scope := getScope(date, region)
	credential := fmt.Sprintf("%s/%s", accessKeyID, scope)

	// Set URL query.
	query := req.URL.Query()
	query.Set("X-Amz-Algorithm", signV4Algorithm)
	query.Set("X-Amz-Date", date.Format(iso8601Format))
	query.Set("X-Amz-Expires", strconv.FormatInt(expires, 10))
	query.Set("X-Amz-SignedHeaders", "host")
	query.Set("X-Amz-Credential", credential)
	query.Set("X-Amz-Content-Sha256", unsignedPayload)

	// "host" is the only header required to be signed for Presigned URLs.
	extractedSignedHeaders := make(http.Header)
	extractedSignedHeaders.Set("host", req.Host)

	queryStr := strings.Replace(query.Encode(), "+", "%20", -1)
	canonicalRequest := getCanonicalRequest(extractedSignedHeaders, unsignedPayload, queryStr, req.URL.Path, req.Method)
	stringToSign := getStringToSign(canonicalRequest, date, scope)
	signingKey := getSigningKey(secretAccessKey, date, region)
	signature := getSignature(signingKey, stringToSign)

	req.URL.RawQuery = query.Encode()

	// Add signature header to RawQuery.
	req.URL.RawQuery += "&X-Amz-Signature=" + url.QueryEscape(signature)

	// Construct the final presigned URL.
	return nil
}

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
