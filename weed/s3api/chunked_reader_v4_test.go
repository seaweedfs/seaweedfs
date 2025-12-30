package s3api

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"hash/crc32"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/stretchr/testify/assert"
)

// getDefaultTimestamp returns a current timestamp for tests
func getDefaultTimestamp() string {
	return time.Now().UTC().Format(iso8601Format)
}

const (
	defaultTimestamp       = "20130524T000000Z" // Legacy constant for reference
	defaultBucketName      = "examplebucket"
	defaultAccessKeyId     = UserAccessKeyPrefix + "IOSFODNN7EXAMPLE"
	defaultSecretAccessKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	defaultRegion          = "us-east-1"
)

func generateStreamingUnsignedPayloadTrailerPayload(includeFinalCRLF bool) string {
	// This test will implement the following scenario:
	// https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html

	chunk1 := "2000\r\n" + strings.Repeat("a", 8192) + "\r\n"
	chunk2 := "2000\r\n" + strings.Repeat("a", 8192) + "\r\n"
	chunk3 := "400\r\n" + strings.Repeat("a", 1024) + "\r\n"

	chunk4 := "0\r\n" /* the last chunk is empty */

	if includeFinalCRLF {
		// Some clients omit the final CRLF, so we need to test that case as well
		chunk4 += "\r\n"
	}

	data := strings.Repeat("a", 17408)
	writer := crc32.NewIEEE()
	_, err := writer.Write([]byte(data))

	if err != nil {
		fmt.Println("Error:", err)
	}
	checksum := writer.Sum(nil)
	base64EncodedChecksum := base64.StdEncoding.EncodeToString(checksum)
	trailer := "x-amz-checksum-crc32:" + base64EncodedChecksum + "\n\r\n\r\n\r\n"

	payload := chunk1 + chunk2 + chunk3 + chunk4 + trailer
	return payload
}

func NewRequestStreamingUnsignedPayloadTrailer(includeFinalCRLF bool) (*http.Request, error) {
	// This test will implement the following scenario:
	// https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html

	payload := generateStreamingUnsignedPayloadTrailerPayload(includeFinalCRLF)
	req, err := http.NewRequest("PUT", "http://amzn-s3-demo-bucket/Key+", bytes.NewReader([]byte(payload)))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Host", "amzn-s3-demo-bucket")
	req.Header.Set("x-amz-date", getDefaultTimestamp())
	req.Header.Set("Content-Encoding", "aws-chunked")
	req.Header.Set("x-amz-decoded-content-length", "17408")
	req.Header.Set("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
	req.Header.Set("x-amz-trailer", "x-amz-checksum-crc32")

	return req, nil
}

func TestNewSignV4ChunkedReaderStreamingUnsignedPayloadTrailer(t *testing.T) {
	// This test will implement the following scenario:
	// https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html
	iam := setupIam()

	req, err := NewRequestStreamingUnsignedPayloadTrailer(true)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}
	// The expected payload a long string of 'a's
	expectedPayload := strings.Repeat("a", 17408)

	runWithRequest(iam, req, t, expectedPayload)

	req, err = NewRequestStreamingUnsignedPayloadTrailer(false)
	if err != nil {
		t.Fatalf("Failed to create request: %v", err)
	}
	runWithRequest(iam, req, t, expectedPayload)
}

func runWithRequest(iam IdentityAccessManagement, req *http.Request, t *testing.T, expectedPayload string) {
	reader, errCode := iam.newChunkedReader(req)
	assert.NotNil(t, reader)
	assert.Equal(t, s3err.ErrNone, errCode)

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read data: %v", err)
	}

	assert.Equal(t, expectedPayload, string(data))
}

func setupIam() IdentityAccessManagement {
	// Create an IdentityAccessManagement instance
	// Add default access keys and secrets

	iam := IdentityAccessManagement{
		identities:        []*Identity{},
		accessKeyIdent:    map[string]*Identity{},
		accounts:          map[string]*Account{},
		emailAccount:      map[string]*Account{},
		hashes:            map[string]*sync.Pool{},
		hashCounters:      map[string]*int32{},
		identityAnonymous: nil,
		domain:            "",
		isAuthEnabled:     false,
	}

	iam.identities = append(iam.identities, &Identity{
		Name: "default",
		Credentials: []*Credential{
			{
				AccessKey: defaultAccessKeyId,
				SecretKey: defaultSecretAccessKey,
			},
		},
		Actions: []Action{
			"Read",
			"Write",
			"List",
		},
	})

	iam.accessKeyIdent[defaultAccessKeyId] = iam.identities[0]
	return iam
}

// TestSignedStreamingUpload tests streaming uploads with signed chunks
// This replaces the removed AWS example test with a dynamic signature generation approach
func TestSignedStreamingUpload(t *testing.T) {
	iam := setupIam()

	// Create a simple streaming upload with 2 chunks
	chunk1Data := strings.Repeat("a", 1024)
	chunk2Data := strings.Repeat("b", 512)

	// Use current time for signatures
	now := time.Now().UTC()
	amzDate := now.Format(iso8601Format)
	dateStamp := now.Format(yyyymmdd)

	// Calculate seed signature
	scope := dateStamp + "/" + defaultRegion + "/s3/aws4_request"

	// Build canonical request for seed signature
	hashedPayload := "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	canonicalHeaders := "content-encoding:aws-chunked\n" +
		"host:s3.amazonaws.com\n" +
		"x-amz-content-sha256:" + hashedPayload + "\n" +
		"x-amz-date:" + amzDate + "\n" +
		"x-amz-decoded-content-length:1536\n"
	signedHeaders := "content-encoding;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length"

	canonicalRequest := "PUT\n" +
		"/test-bucket/test-object\n" +
		"\n" +
		canonicalHeaders + "\n" +
		signedHeaders + "\n" +
		hashedPayload

	canonicalRequestHash := getSHA256Hash([]byte(canonicalRequest))
	stringToSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + canonicalRequestHash

	signingKey := getSigningKey(defaultSecretAccessKey, dateStamp, defaultRegion, "s3")
	seedSignature := getSignature(signingKey, stringToSign)

	// Calculate chunk signatures
	chunk1Hash := getSHA256Hash([]byte(chunk1Data))
	chunk1StringToSign := "AWS4-HMAC-SHA256-PAYLOAD\n" + amzDate + "\n" + scope + "\n" +
		seedSignature + "\n" + emptySHA256 + "\n" + chunk1Hash
	chunk1Signature := getSignature(signingKey, chunk1StringToSign)

	chunk2Hash := getSHA256Hash([]byte(chunk2Data))
	chunk2StringToSign := "AWS4-HMAC-SHA256-PAYLOAD\n" + amzDate + "\n" + scope + "\n" +
		chunk1Signature + "\n" + emptySHA256 + "\n" + chunk2Hash
	chunk2Signature := getSignature(signingKey, chunk2StringToSign)

	finalStringToSign := "AWS4-HMAC-SHA256-PAYLOAD\n" + amzDate + "\n" + scope + "\n" +
		chunk2Signature + "\n" + emptySHA256 + "\n" + emptySHA256
	finalSignature := getSignature(signingKey, finalStringToSign)

	// Build the chunked payload
	payload := fmt.Sprintf("400;chunk-signature=%s\r\n%s\r\n", chunk1Signature, chunk1Data) +
		fmt.Sprintf("200;chunk-signature=%s\r\n%s\r\n", chunk2Signature, chunk2Data) +
		fmt.Sprintf("0;chunk-signature=%s\r\n\r\n", finalSignature)

	// Create the request
	req, err := http.NewRequest("PUT", "http://s3.amazonaws.com/test-bucket/test-object",
		bytes.NewReader([]byte(payload)))
	assert.NoError(t, err)

	req.Header.Set("Host", "s3.amazonaws.com")
	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("x-amz-content-sha256", hashedPayload)
	req.Header.Set("Content-Encoding", "aws-chunked")
	req.Header.Set("x-amz-decoded-content-length", "1536")

	authHeader := fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		defaultAccessKeyId, scope, signedHeaders, seedSignature)
	req.Header.Set("Authorization", authHeader)

	// Test the chunked reader
	reader, errCode := iam.newChunkedReader(req)
	assert.Equal(t, s3err.ErrNone, errCode)
	assert.NotNil(t, reader)

	// Read and verify the payload
	data, err := io.ReadAll(reader)
	assert.NoError(t, err)
	assert.Equal(t, chunk1Data+chunk2Data, string(data))
}

// createTrailerStreamingRequest creates a streaming upload request with trailer for testing.
// If useValidTrailerSignature is true, uses a correctly calculated trailer signature;
// otherwise uses an intentionally wrong signature for negative testing.
func createTrailerStreamingRequest(t *testing.T, useValidTrailerSignature bool) (*http.Request, string) {
	chunk1Data := "hello world\n"
	chunk1DataLen := len(chunk1Data)
	chunk1DataLenHex := fmt.Sprintf("%x", chunk1DataLen)

	// Use current time for signatures
	now := time.Now().UTC()
	amzDate := now.Format(iso8601Format)
	dateStamp := now.Format(yyyymmdd)

	// Calculate seed signature
	scope := dateStamp + "/" + defaultRegion + "/s3/aws4_request"

	// Build canonical request for seed signature
	hashedPayload := "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"
	canonicalHeaders := "content-encoding:aws-chunked\n" +
		"host:s3.amazonaws.com\n" +
		"x-amz-content-sha256:" + hashedPayload + "\n" +
		"x-amz-date:" + amzDate + "\n" +
		fmt.Sprintf("x-amz-decoded-content-length:%d\n", chunk1DataLen) +
		"x-amz-trailer:x-amz-checksum-crc32\n"
	signedHeaders := "content-encoding;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length;x-amz-trailer"

	canonicalRequest := "PUT\n" +
		"/test-bucket/test-object\n" +
		"\n" +
		canonicalHeaders + "\n" +
		signedHeaders + "\n" +
		hashedPayload

	canonicalRequestHash := getSHA256Hash([]byte(canonicalRequest))
	stringToSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + canonicalRequestHash

	signingKey := getSigningKey(defaultSecretAccessKey, dateStamp, defaultRegion, "s3")
	seedSignature := getSignature(signingKey, stringToSign)

	// Calculate chunk signatures
	chunk1Hash := getSHA256Hash([]byte(chunk1Data))
	chunk1StringToSign := "AWS4-HMAC-SHA256-PAYLOAD\n" + amzDate + "\n" + scope + "\n" +
		seedSignature + "\n" + emptySHA256 + "\n" + chunk1Hash
	chunk1Signature := getSignature(signingKey, chunk1StringToSign)

	// Final chunk (0 bytes)
	finalStringToSign := "AWS4-HMAC-SHA256-PAYLOAD\n" + amzDate + "\n" + scope + "\n" +
		chunk1Signature + "\n" + emptySHA256 + "\n" + emptySHA256
	finalSignature := getSignature(signingKey, finalStringToSign)

	// Calculate CRC32 checksum for trailer
	crcWriter := crc32.NewIEEE()
	_, crcErr := crcWriter.Write([]byte(chunk1Data))
	assert.NoError(t, crcErr)
	checksum := crcWriter.Sum(nil)
	base64EncodedChecksum := base64.StdEncoding.EncodeToString(checksum)

	// The on-wire trailer format uses \r\n (HTTP/aws-chunked convention)
	trailerOnWire := "x-amz-checksum-crc32:" + base64EncodedChecksum + "\r\n"

	// Calculate or use wrong trailer signature
	var trailerSignature string
	if useValidTrailerSignature {
		// The canonical trailer content uses \n for signing (per AWS SigV4 spec)
		trailerCanonical := "x-amz-checksum-crc32:" + base64EncodedChecksum + "\n"
		trailerHash := getSHA256Hash([]byte(trailerCanonical))
		trailerStringToSign := "AWS4-HMAC-SHA256-TRAILER\n" + amzDate + "\n" + scope + "\n" +
			finalSignature + "\n" + trailerHash
		trailerSignature = getSignature(signingKey, trailerStringToSign)
	} else {
		// Intentionally wrong signature for negative testing
		trailerSignature = "0000000000000000000000000000000000000000000000000000000000000000"
	}

	// Build the chunked payload with trailer and trailer signature
	payload := fmt.Sprintf("%s;chunk-signature=%s\r\n%s\r\n", chunk1DataLenHex, chunk1Signature, chunk1Data) +
		fmt.Sprintf("0;chunk-signature=%s\r\n", finalSignature) +
		trailerOnWire +
		"x-amz-trailer-signature:" + trailerSignature + "\r\n" +
		"\r\n"

	// Create the request
	req, err := http.NewRequest("PUT", "http://s3.amazonaws.com/test-bucket/test-object",
		bytes.NewReader([]byte(payload)))
	assert.NoError(t, err)

	req.Header.Set("Host", "s3.amazonaws.com")
	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("x-amz-content-sha256", hashedPayload)
	req.Header.Set("Content-Encoding", "aws-chunked")
	req.Header.Set("x-amz-decoded-content-length", fmt.Sprintf("%d", chunk1DataLen))
	req.Header.Set("x-amz-trailer", "x-amz-checksum-crc32")

	authHeader := fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		defaultAccessKeyId, scope, signedHeaders, seedSignature)
	req.Header.Set("Authorization", authHeader)

	return req, chunk1Data
}

// TestSignedStreamingUploadWithTrailer tests streaming uploads with signed chunks and trailers
// This tests the STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER content-sha256 header value
// which is used by AWS SDK v2 when checksum validation is enabled
func TestSignedStreamingUploadWithTrailer(t *testing.T) {
	iam := setupIam()
	req, expectedData := createTrailerStreamingRequest(t, true)

	// Test the chunked reader
	reader, errCode := iam.newChunkedReader(req)
	assert.Equal(t, s3err.ErrNone, errCode)
	assert.NotNil(t, reader)

	// Read and verify the payload
	data, err := io.ReadAll(reader)
	assert.NoError(t, err)
	assert.Equal(t, expectedData, string(data))
}

// TestSignedStreamingUploadWithTrailerInvalidSignature tests behavior with invalid trailer signatures.
// This is a negative test case for trailer signature validation. It currently verifies that an invalid
// signature doesn't break content reading, and is prepared for when validation is implemented.
func TestSignedStreamingUploadWithTrailerInvalidSignature(t *testing.T) {
	iam := setupIam()
	req, expectedData := createTrailerStreamingRequest(t, false)

	// Test the chunked reader - it should be created successfully
	reader, errCode := iam.newChunkedReader(req)
	assert.Equal(t, s3err.ErrNone, errCode)
	assert.NotNil(t, reader)

	// Read the payload - currently trailer signature validation may not be implemented,
	// but this test documents the expected behavior and will catch regressions
	// if trailer signature validation is added in the future
	data, err := io.ReadAll(reader)
	// Note: If trailer signature validation is implemented, this should fail with an error
	// For now, we just verify the content is correctly extracted
	if err != nil {
		assert.Contains(t, err.Error(), "signature", "Error should indicate signature mismatch")
	} else {
		// If no error, content should still be correct (trailer sig validation not yet implemented)
		assert.Equal(t, expectedData, string(data))
	}
}

// TestSignedStreamingUploadInvalidSignature tests that invalid chunk signatures are rejected
// This is a negative test case to ensure signature validation is actually working
func TestSignedStreamingUploadInvalidSignature(t *testing.T) {
	iam := setupIam()

	// Create a simple streaming upload with 1 chunk
	chunk1Data := strings.Repeat("a", 1024)

	// Use current time for signatures
	now := time.Now().UTC()
	amzDate := now.Format(iso8601Format)
	dateStamp := now.Format(yyyymmdd)

	// Calculate seed signature
	scope := dateStamp + "/" + defaultRegion + "/s3/aws4_request"

	// Build canonical request for seed signature
	hashedPayload := "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	canonicalHeaders := "content-encoding:aws-chunked\n" +
		"host:s3.amazonaws.com\n" +
		"x-amz-content-sha256:" + hashedPayload + "\n" +
		"x-amz-date:" + amzDate + "\n" +
		"x-amz-decoded-content-length:1024\n"
	signedHeaders := "content-encoding;host;x-amz-content-sha256;x-amz-date;x-amz-decoded-content-length"

	canonicalRequest := "PUT\n" +
		"/test-bucket/test-object\n" +
		"\n" +
		canonicalHeaders + "\n" +
		signedHeaders + "\n" +
		hashedPayload

	canonicalRequestHash := getSHA256Hash([]byte(canonicalRequest))
	stringToSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + canonicalRequestHash

	signingKey := getSigningKey(defaultSecretAccessKey, dateStamp, defaultRegion, "s3")
	seedSignature := getSignature(signingKey, stringToSign)

	// Calculate chunk signature (correct)
	chunk1Hash := getSHA256Hash([]byte(chunk1Data))
	chunk1StringToSign := "AWS4-HMAC-SHA256-PAYLOAD\n" + amzDate + "\n" + scope + "\n" +
		seedSignature + "\n" + emptySHA256 + "\n" + chunk1Hash
	chunk1Signature := getSignature(signingKey, chunk1StringToSign)

	// Calculate final signature (correct)
	finalStringToSign := "AWS4-HMAC-SHA256-PAYLOAD\n" + amzDate + "\n" + scope + "\n" +
		chunk1Signature + "\n" + emptySHA256 + "\n" + emptySHA256
	finalSignature := getSignature(signingKey, finalStringToSign)

	// Build the chunked payload with INTENTIONALLY WRONG chunk signature
	// We'll use a modified signature to simulate a tampered request
	wrongChunkSignatureBytes := []byte(chunk1Signature)
	if len(wrongChunkSignatureBytes) > 0 {
		// Flip the first hex character to guarantee a different signature
		if wrongChunkSignatureBytes[0] == '0' {
			wrongChunkSignatureBytes[0] = '1'
		} else {
			wrongChunkSignatureBytes[0] = '0'
		}
	}
	wrongChunkSignature := string(wrongChunkSignatureBytes)
	payload := fmt.Sprintf("400;chunk-signature=%s\r\n%s\r\n", wrongChunkSignature, chunk1Data) +
		fmt.Sprintf("0;chunk-signature=%s\r\n\r\n", finalSignature)

	// Create the request
	req, err := http.NewRequest("PUT", "http://s3.amazonaws.com/test-bucket/test-object",
		bytes.NewReader([]byte(payload)))
	assert.NoError(t, err)

	req.Header.Set("Host", "s3.amazonaws.com")
	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("x-amz-content-sha256", hashedPayload)
	req.Header.Set("Content-Encoding", "aws-chunked")
	req.Header.Set("x-amz-decoded-content-length", "1024")

	authHeader := fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		defaultAccessKeyId, scope, signedHeaders, seedSignature)
	req.Header.Set("Authorization", authHeader)

	// Test the chunked reader - it should be created successfully
	reader, errCode := iam.newChunkedReader(req)
	assert.Equal(t, s3err.ErrNone, errCode)
	assert.NotNil(t, reader)

	// Try to read the payload - this should fail with signature validation error
	_, err = io.ReadAll(reader)
	assert.Error(t, err, "Expected error when reading chunk with invalid signature")
	assert.Contains(t, err.Error(), "chunk signature does not match", "Error should indicate chunk signature mismatch")
}
