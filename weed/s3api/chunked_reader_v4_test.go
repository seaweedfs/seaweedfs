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
	defaultAccessKeyId     = "AKIAIOSFODNN7EXAMPLE"
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
