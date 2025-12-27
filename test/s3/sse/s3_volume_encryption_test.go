//go:build integration
// +build integration

package sse

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// TestS3VolumeEncryptionRoundtrip tests that data uploaded with encryptVolumeData
// enabled can be read back correctly. This requires the S3 server to be started with
// -encryptVolumeData=true for the test to verify encryption is working.
//
// To run this test:
// 1. Start SeaweedFS: weed server -s3 -s3.encryptVolumeData=true
// 2. Run: go test -v -run TestS3VolumeEncryptionRoundtrip
func TestS3VolumeEncryptionRoundtrip(t *testing.T) {
	svc := getS3Client(t)
	bucket := fmt.Sprintf("volume-encryption-test-%d", time.Now().Unix())

	// Create bucket
	_, err := svc.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}
	defer cleanupBucket(t, svc, bucket)

	testCases := []struct {
		name     string
		key      string
		content  string
		rangeReq string // Optional range request
	}{
		{
			name:    "small file",
			key:     "small.txt",
			content: "Hello, encrypted world!",
		},
		{
			name:    "medium file",
			key:     "medium.txt",
			content: strings.Repeat("SeaweedFS volume encryption test content. ", 1000),
		},
		{
			name:    "binary content",
			key:     "binary.bin",
			content: string([]byte{0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD, 0x00, 0x80}),
		},
		{
			name:     "range request",
			key:      "range-test.txt",
			content:  "0123456789ABCDEFGHIJ",
			rangeReq: "bytes=5-10",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Upload
			_, err := svc.PutObject(&s3.PutObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(tc.key),
				Body:   strings.NewReader(tc.content),
			})
			if err != nil {
				t.Fatalf("PutObject failed: %v", err)
			}

			// Download
			getInput := &s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(tc.key),
			}
			if tc.rangeReq != "" {
				getInput.Range = aws.String(tc.rangeReq)
			}

			result, err := svc.GetObject(getInput)
			if err != nil {
				t.Fatalf("GetObject failed: %v", err)
			}
			defer result.Body.Close()

			data, err := io.ReadAll(result.Body)
			if err != nil {
				t.Fatalf("Failed to read response body: %v", err)
			}

			// Verify content
			expected := tc.content
			if tc.rangeReq != "" {
				// For "bytes=5-10", we expect characters at positions 5-10 (inclusive)
				expected = tc.content[5:11]
			}

			if string(data) != expected {
				t.Errorf("Content mismatch:\n  expected: %q\n  got: %q", expected, string(data))
			} else {
				t.Logf("Successfully uploaded and downloaded %s (%d bytes)", tc.key, len(data))
			}
		})
	}
}

// TestS3VolumeEncryptionMultiChunk tests large files that span multiple chunks
// to ensure encryption works correctly across chunk boundaries.
func TestS3VolumeEncryptionMultiChunk(t *testing.T) {
	svc := getS3Client(t)
	bucket := fmt.Sprintf("volume-encryption-multichunk-%d", time.Now().Unix())

	// Create bucket
	_, err := svc.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}
	defer cleanupBucket(t, svc, bucket)

	// Create a file larger than default chunk size (8MB)
	// Use 10MB to ensure multiple chunks
	largeContent := make([]byte, 10*1024*1024)
	for i := range largeContent {
		largeContent[i] = byte(i % 256)
	}

	key := "large-file.bin"

	// Upload
	_, err = svc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(largeContent),
	})
	if err != nil {
		t.Fatalf("PutObject failed for large file: %v", err)
	}

	// Download full file
	result, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer result.Body.Close()

	downloadedData, err := io.ReadAll(result.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}

	if len(downloadedData) != len(largeContent) {
		t.Errorf("Size mismatch: expected %d, got %d", len(largeContent), len(downloadedData))
	}

	if !bytes.Equal(downloadedData, largeContent) {
		t.Errorf("Content mismatch in multi-chunk file")
		// Find first mismatch
		for i := 0; i < len(downloadedData) && i < len(largeContent); i++ {
			if downloadedData[i] != largeContent[i] {
				t.Errorf("First mismatch at byte %d: expected %02x, got %02x", i, largeContent[i], downloadedData[i])
				break
			}
		}
	} else {
		t.Logf("Successfully uploaded and downloaded %d byte multi-chunk file", len(downloadedData))
	}

	// Test range request spanning chunk boundary (around 8MB)
	rangeStart := int64(8*1024*1024 - 1000)
	rangeEnd := int64(8*1024*1024 + 1000)
	rangeResult, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", rangeStart, rangeEnd)),
	})
	if err != nil {
		t.Fatalf("Range GetObject failed: %v", err)
	}
	defer rangeResult.Body.Close()

	rangeData, err := io.ReadAll(rangeResult.Body)
	if err != nil {
		t.Fatalf("Failed to read range response: %v", err)
	}

	expectedRange := largeContent[rangeStart : rangeEnd+1]
	if !bytes.Equal(rangeData, expectedRange) {
		t.Errorf("Range request content mismatch at chunk boundary")
	} else {
		t.Logf("Successfully retrieved range spanning chunk boundary (%d bytes)", len(rangeData))
	}
}

// TestS3VolumeEncryptionMultiChunkRangeRead tests range reads that span multiple chunks:
// - Part of chunk 1
// - Whole chunk 2
// - Part of chunk 3
// This is critical for verifying that cipher decryption works correctly when
// reading across chunk boundaries with the cipherKey from each chunk.
func TestS3VolumeEncryptionMultiChunkRangeRead(t *testing.T) {
	svc := getS3Client(t)
	bucket := fmt.Sprintf("volume-encryption-multirange-%d", time.Now().Unix())

	// Create bucket
	_, err := svc.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}
	defer cleanupBucket(t, svc, bucket)

	// Default chunk size is 8MB. Create a file with 3+ chunks (25MB)
	// to ensure we have multiple complete chunks
	const chunkSize = 8 * 1024 * 1024 // 8MB
	const fileSize = 25 * 1024 * 1024 // 25MB = 3 chunks + partial 4th

	largeContent := make([]byte, fileSize)
	for i := range largeContent {
		// Use recognizable pattern: each byte encodes its position
		largeContent[i] = byte(i % 256)
	}

	key := "multi-chunk-range-test.bin"

	// Upload
	_, err = svc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(largeContent),
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	t.Logf("Uploaded %d byte file (%d chunks of %d bytes)", fileSize, (fileSize+chunkSize-1)/chunkSize, chunkSize)

	// Test cases for range reads spanning multiple chunks
	testCases := []struct {
		name       string
		rangeStart int64
		rangeEnd   int64
		desc       string
	}{
		{
			name:       "part_chunk1_whole_chunk2_part_chunk3",
			rangeStart: chunkSize - 1000,   // 1000 bytes before end of chunk 1
			rangeEnd:   2*chunkSize + 1000, // 1000 bytes into chunk 3
			desc:       "Spans from end of chunk 1 through entire chunk 2 into beginning of chunk 3",
		},
		{
			name:       "last_byte_chunk1_through_first_byte_chunk3",
			rangeStart: chunkSize - 1, // Last byte of chunk 1
			rangeEnd:   2 * chunkSize, // First byte of chunk 3
			desc:       "Minimal span: last byte of chunk 1, entire chunk 2, first byte of chunk 3",
		},
		{
			name:       "middle_chunk1_to_middle_chunk3",
			rangeStart: chunkSize / 2,             // Middle of chunk 1
			rangeEnd:   2*chunkSize + chunkSize/2, // Middle of chunk 3
			desc:       "From middle of chunk 1 to middle of chunk 3",
		},
		{
			name:       "half_chunk1_whole_chunk2_half_chunk3",
			rangeStart: chunkSize / 2,                 // Half of chunk 1
			rangeEnd:   2*chunkSize + chunkSize/2 - 1, // Half of chunk 3
			desc:       "Half of each boundary chunk, whole middle chunk",
		},
		{
			name:       "single_byte_each_boundary",
			rangeStart: chunkSize - 1, // 1 byte from chunk 1
			rangeEnd:   2 * chunkSize, // 1 byte from chunk 3
			desc:       "1 byte from chunk 1, entire chunk 2, 1 byte from chunk 3",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Logf("Test: %s", tc.desc)
			t.Logf("Range: bytes=%d-%d (spanning bytes at offsets across chunk boundaries)", tc.rangeStart, tc.rangeEnd)

			result, err := svc.GetObject(&s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
				Range:  aws.String(fmt.Sprintf("bytes=%d-%d", tc.rangeStart, tc.rangeEnd)),
			})
			if err != nil {
				t.Fatalf("Range GetObject failed: %v", err)
			}
			defer result.Body.Close()

			rangeData, err := io.ReadAll(result.Body)
			if err != nil {
				t.Fatalf("Failed to read range response: %v", err)
			}

			expectedLen := tc.rangeEnd - tc.rangeStart + 1
			if int64(len(rangeData)) != expectedLen {
				t.Errorf("Size mismatch: expected %d bytes, got %d", expectedLen, len(rangeData))
			}

			expectedRange := largeContent[tc.rangeStart : tc.rangeEnd+1]
			if !bytes.Equal(rangeData, expectedRange) {
				t.Errorf("Content mismatch in multi-chunk range read")
				// Find first mismatch for debugging
				for i := 0; i < len(rangeData) && i < len(expectedRange); i++ {
					if rangeData[i] != expectedRange[i] {
						globalOffset := tc.rangeStart + int64(i)
						chunkNum := globalOffset / chunkSize
						offsetInChunk := globalOffset % chunkSize
						t.Errorf("First mismatch at byte %d (chunk %d, offset %d in chunk): expected %02x, got %02x",
							i, chunkNum, offsetInChunk, expectedRange[i], rangeData[i])
						break
					}
				}
			} else {
				t.Logf("✓ Successfully read %d bytes spanning chunks (offsets %d-%d)", len(rangeData), tc.rangeStart, tc.rangeEnd)
			}
		})
	}
}

// TestS3VolumeEncryptionCopy tests that copying encrypted objects works correctly.
func TestS3VolumeEncryptionCopy(t *testing.T) {
	svc := getS3Client(t)
	bucket := fmt.Sprintf("volume-encryption-copy-%d", time.Now().Unix())

	// Create bucket
	_, err := svc.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Fatalf("Failed to create bucket: %v", err)
	}
	defer cleanupBucket(t, svc, bucket)

	srcKey := "source-object.txt"
	dstKey := "copied-object.txt"
	content := "Content to be copied with volume-level encryption"

	// Upload source
	_, err = svc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(srcKey),
		Body:   strings.NewReader(content),
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Copy object
	_, err = svc.CopyObject(&s3.CopyObjectInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String(dstKey),
		CopySource: aws.String(fmt.Sprintf("%s/%s", bucket, srcKey)),
	})
	if err != nil {
		t.Fatalf("CopyObject failed: %v", err)
	}

	// Read copied object
	result, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(dstKey),
	})
	if err != nil {
		t.Fatalf("GetObject failed for copied object: %v", err)
	}
	defer result.Body.Close()

	data, err := io.ReadAll(result.Body)
	if err != nil {
		t.Fatalf("Failed to read copied object: %v", err)
	}

	if string(data) != content {
		t.Errorf("Copied content mismatch:\n  expected: %q\n  got: %q", content, string(data))
	} else {
		t.Logf("Successfully copied encrypted object")
	}
}

// Helper functions

func getS3Client(t *testing.T) *s3.S3 {
	// Use credentials that match the Makefile configuration
	// ACCESS_KEY ?= some_access_key1
	// SECRET_KEY ?= some_secret_key1
	sess, err := session.NewSession(&aws.Config{
		Region:           aws.String("us-east-1"),
		Endpoint:         aws.String("http://localhost:8333"),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
		Credentials:      credentials.NewStaticCredentials("some_access_key1", "some_secret_key1", ""),
	})
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}
	return s3.New(sess)
}

func cleanupBucket(t *testing.T, svc *s3.S3, bucket string) {
	ctx := context.Background()
	_ = ctx

	// List and delete all objects
	listResult, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Logf("Warning: failed to list objects for cleanup: %v", err)
		return
	}

	for _, obj := range listResult.Contents {
		_, err := svc.DeleteObject(&s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    obj.Key,
		})
		if err != nil {
			t.Logf("Warning: failed to delete object %s: %v", *obj.Key, err)
		}
	}

	// Delete bucket
	_, err = svc.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		t.Logf("Warning: failed to delete bucket %s: %v", bucket, err)
	}
}
