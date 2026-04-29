package sse_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"
)

// TestSSES3UploadPartCopyIntegration pins the fix for issue #8908.
//
// Docker Registry's S3 storage driver finalizes blob uploads via a server-side
// "Move" pattern: a streaming PUT/multipart upload to a temporary key, then
// CreateMultipartUpload + UploadPartCopy(s) + CompleteMultipartUpload to put
// the bytes at the final blob path. Under bucket-default SSE-S3, every push
// goes through this UploadPartCopy step.
//
// Before the fix, copyChunksForRange did a raw byte copy that left the
// destination's part chunks SseType=NONE. Then completedMultipartChunk
// (PR #9224) saw NONE chunks in an SSE-S3 multipart upload and "backfilled"
// SSE-S3 metadata with IVs derived from the destination upload's baseIV. But
// the bytes on disk had been encrypted with the SOURCE upload's key+baseIV,
// so the read path decrypted with the wrong IV — yielding deterministic byte
// corruption on GET (the "Digest did not match" symptom kubelet surfaces).
//
// This test reproduces the exact shape: a 39MB plaintext source (single
// PutObject — auto-chunked into multiple internal SSE-S3 chunks on disk
// because of bucket-default SSE-S3), then a fresh multipart upload at a new
// destination key with two UploadPartCopy parts (32MB + 7MB) and Complete.
// The full GET must SHA back to what was uploaded.
//
// The function name ends in "Integration" so it is matched by the existing
// `TestSSE.*Integration` pattern in test/s3/sse/Makefile and the
// `.*Multipart.*Integration` pattern in .github/workflows/s3-sse-tests.yml.
func TestSSES3UploadPartCopyIntegration(t *testing.T) {
	ctx := context.Background()
	client, err := createS3Client(ctx, defaultConfig)
	require.NoError(t, err, "Failed to create S3 client")

	bucketName, err := createTestBucket(ctx, client, defaultConfig.BucketPrefix+"sse-s3-uploadpartcopy-")
	require.NoError(t, err, "Failed to create test bucket")
	defer cleanupTestBucket(ctx, client, bucketName)

	// Bucket-default SSE-S3 — same setup Docker Registry uses.
	_, err = client.PutBucketEncryption(ctx, &s3.PutBucketEncryptionInput{
		Bucket: aws.String(bucketName),
		ServerSideEncryptionConfiguration: &types.ServerSideEncryptionConfiguration{
			Rules: []types.ServerSideEncryptionRule{
				{
					ApplyServerSideEncryptionByDefault: &types.ServerSideEncryptionByDefault{
						SSEAlgorithm: types.ServerSideEncryptionAes256,
					},
				},
			},
		},
	})
	require.NoError(t, err, "Failed to set bucket default SSE-S3")

	// Source: 39MB single PutObject (auto-chunked internally into 5 SSE-S3 chunks).
	const sourceSize = 39 * 1024 * 1024
	sourceData := generateTestData(sourceSize)
	expectedSHA := sha256.Sum256(sourceData)

	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String("source-blob"),
		Body:   bytes.NewReader(sourceData),
	})
	require.NoError(t, err, "Failed to upload source object")

	// Sanity check: the source itself must round-trip correctly.
	{
		resp, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String("source-blob"),
		})
		require.NoError(t, err, "Failed to GET source")
		got, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		require.NoError(t, err)
		require.Equal(t, expectedSHA, sha256.Sum256(got), "source object must round-trip")
	}

	cases := []struct {
		name string
		// part definitions: each entry is (start, end) byte range to copy.
		parts [][2]int64
	}{
		{
			// Docker Registry's typical Move shape for blobs around 40MB:
			// one 32MB part + one tail part. This is exactly the metadata
			// shape the user reported (5 dst chunks across 2 multipart parts).
			name: "DockerRegistry_32MB_Plus_Tail",
			parts: [][2]int64{
				{0, 32*1024*1024 - 1},
				{32 * 1024 * 1024, sourceSize - 1},
			},
		},
		{
			// Single full-object UploadPartCopy.
			name: "Single_Full_Object_Copy",
			parts: [][2]int64{
				{0, sourceSize - 1},
			},
		},
		{
			// Many small range-copies — exercises the per-part-local-offset
			// IV math under varied chunk-overlap shapes.
			name: "Many_5MB_Ranges",
			parts: [][2]int64{
				{0, 5*1024*1024 - 1},
				{5 * 1024 * 1024, 10*1024*1024 - 1},
				{10 * 1024 * 1024, 15*1024*1024 - 1},
				{15 * 1024 * 1024, 20*1024*1024 - 1},
				{20 * 1024 * 1024, sourceSize - 1},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dstKey := "dest-" + strings.ToLower(strings.ReplaceAll(tc.name, "_", "-"))

			createResp, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(dstKey),
			})
			require.NoError(t, err, "CreateMultipartUpload")
			uploadID := aws.ToString(createResp.UploadId)

			completedParts := make([]types.CompletedPart, 0, len(tc.parts))
			for i, rng := range tc.parts {
				partNumber := int32(i + 1)
				resp, err := client.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
					Bucket:          aws.String(bucketName),
					Key:             aws.String(dstKey),
					PartNumber:      aws.Int32(partNumber),
					UploadId:        aws.String(uploadID),
					CopySource:      aws.String(bucketName + "/source-blob"),
					CopySourceRange: aws.String(fmt.Sprintf("bytes=%d-%d", rng[0], rng[1])),
				})
				require.NoErrorf(t, err, "UploadPartCopy part %d range=[%d,%d]", partNumber, rng[0], rng[1])
				completedParts = append(completedParts, types.CompletedPart{
					ETag:       resp.CopyPartResult.ETag,
					PartNumber: aws.Int32(partNumber),
				})
			}

			_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
				Bucket:          aws.String(bucketName),
				Key:             aws.String(dstKey),
				UploadId:        aws.String(uploadID),
				MultipartUpload: &types.CompletedMultipartUpload{Parts: completedParts},
			})
			require.NoError(t, err, "CompleteMultipartUpload")

			// Two-pass verification: full GET (Docker Registry / Kubelet shape).
			resp, err := client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(dstKey),
			})
			require.NoError(t, err, "GetObject")
			defer resp.Body.Close()

			h := sha256.New()
			n, err := io.Copy(h, resp.Body)
			require.NoError(t, err, "stream GET body")
			require.Equal(t, int64(sourceSize), n, "GET length")

			var actual [32]byte
			copy(actual[:], h.Sum(nil))

			require.Equalf(t, expectedSHA, actual,
				"UploadPartCopy SHA mismatch (#8908):\n  expected sha256:%s\n  got      sha256:%s\n  parts=%d  size=%d",
				hex.EncodeToString(expectedSHA[:]),
				hex.EncodeToString(actual[:]),
				len(tc.parts), n)
		})
	}
}
