package sse_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"
)

// TestSSES3ConcurrentMultipartDigestIntegration mirrors the production
// scenario reported in issue #8908: Docker Registry pushes several large
// container images in parallel, each push being itself a multipart upload of
// many ~5MB parts under bucket-default SSE-S3. Registry pulls fail with
// "Digest did not match" because the SHA-256 of the streamed GET body does
// not match the SHA-256 of what was uploaded.
//
// Coverage that the existing TestSSEMultipartManyChunksIntegration does NOT
// have:
//   - bucket-default SSE-S3 (not explicit per-request headers)
//   - multiple objects uploaded concurrently
//   - parts within each object uploaded concurrently
//   - both full-body GET (Docker Registry) and chunked range GETs (kubelet/CRI)
//
// If the SHA over the streamed body ever differs from the SHA over the
// uploaded bytes, the test fails with the exact bucket/key, expected SHA,
// and actual SHA — the same shape Docker Registry surfaces on pull.
//
// The function name ends in "Integration" so it is matched by the existing
// `TestSSE.*Integration` pattern in test/s3/sse/Makefile and the
// `.*Multipart.*Integration` pattern in .github/workflows/s3-sse-tests.yml,
// so the regression coverage runs automatically in CI.
func TestSSES3ConcurrentMultipartDigestIntegration(t *testing.T) {
	ctx := context.Background()
	client, err := createS3Client(ctx, defaultConfig)
	require.NoError(t, err, "Failed to create S3 client")

	bucketName, err := createTestBucket(ctx, client, defaultConfig.BucketPrefix+"sse-s3-concurrent-")
	require.NoError(t, err, "Failed to create test bucket")
	defer cleanupTestBucket(ctx, client, bucketName)

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
	require.NoError(t, err, "Failed to set bucket default SSE-S3 encryption")

	const (
		numObjects     = 5               // mirrors "5 images at a time"
		partsPerObject = 12              // 12 parts × 5MB = 60MB per blob; total 5×60=300MB
		partSize       = 5 * 1024 * 1024 // S3 minimum part size
		uploadParallel = 8               // mirror typical S3 SDK transfer manager concurrency
		iterations     = 2               // run twice to flush out flakiness without bloating CI
	)

	type blob struct {
		key     string
		parts   [][]byte
		full    []byte
		fullSHA [32]byte
	}

	makeBlobs := func(iter int) []blob {
		blobs := make([]blob, numObjects)
		for i := range blobs {
			parts := make([][]byte, partsPerObject)
			for j := range parts {
				parts[j] = generateTestData(partSize)
			}
			full := bytes.Join(parts, nil)
			blobs[i] = blob{
				key:     fmt.Sprintf("iter%02d-blob-%02d", iter, i),
				parts:   parts,
				full:    full,
				fullSHA: sha256.Sum256(full),
			}
		}
		return blobs
	}

	uploadOne := func(b blob) error {
		createResp, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(b.key),
		})
		if err != nil {
			return fmt.Errorf("create multipart for %s: %w", b.key, err)
		}
		uploadID := aws.ToString(createResp.UploadId)

		completedParts := make([]types.CompletedPart, partsPerObject)
		var (
			wg      sync.WaitGroup
			partErr error
			partMu  sync.Mutex
		)
		sem := make(chan struct{}, uploadParallel)
		for i := 0; i < partsPerObject; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()

				resp, err := client.UploadPart(ctx, &s3.UploadPartInput{
					Bucket:     aws.String(bucketName),
					Key:        aws.String(b.key),
					PartNumber: aws.Int32(int32(i + 1)),
					UploadId:   aws.String(uploadID),
					Body:       bytes.NewReader(b.parts[i]),
				})
				if err != nil {
					partMu.Lock()
					if partErr == nil {
						partErr = fmt.Errorf("upload part %d of %s: %w", i+1, b.key, err)
					}
					partMu.Unlock()
					return
				}
				completedParts[i] = types.CompletedPart{
					ETag:       resp.ETag,
					PartNumber: aws.Int32(int32(i + 1)),
				}
			}(i)
		}
		wg.Wait()
		if partErr != nil {
			_, _ = client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
				Bucket: aws.String(bucketName), Key: aws.String(b.key), UploadId: aws.String(uploadID),
			})
			return partErr
		}

		_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket:          aws.String(bucketName),
			Key:             aws.String(b.key),
			UploadId:        aws.String(uploadID),
			MultipartUpload: &types.CompletedMultipartUpload{Parts: completedParts},
		})
		if err != nil {
			return fmt.Errorf("complete multipart for %s: %w", b.key, err)
		}
		return nil
	}

	type result struct {
		key      string
		expected string
		actual   string
		gotSize  int64
		wantSize int64
		readErr  error
	}

	verifyFullGET := func(b blob) result {
		resp, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(b.key),
		})
		if err != nil {
			return result{key: "FULL " + b.key, readErr: fmt.Errorf("GetObject: %w", err)}
		}
		defer resp.Body.Close()
		h := sha256.New()
		n, copyErr := io.Copy(h, resp.Body)
		var actual [32]byte
		copy(actual[:], h.Sum(nil))
		return result{
			key:      "FULL " + b.key,
			expected: hex.EncodeToString(b.fullSHA[:]),
			actual:   hex.EncodeToString(actual[:]),
			gotSize:  n,
			wantSize: int64(len(b.full)),
			readErr:  copyErr,
		}
	}

	verifyRangeGET := func(b blob) result {
		// 1MB windows. Includes a window that crosses the 8MB internal-chunk
		// boundary (chunkSize in putToFiler) — the historical danger zone for
		// SSE-S3 multipart range reads.
		const window = 1024 * 1024
		h := sha256.New()
		var totalN int64
		objectSize := int64(len(b.full))
		for offset := int64(0); offset < objectSize; offset += window {
			end := offset + window - 1
			if end >= objectSize {
				end = objectSize - 1
			}
			resp, err := client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(b.key),
				Range:  aws.String(fmt.Sprintf("bytes=%d-%d", offset, end)),
			})
			if err != nil {
				return result{key: "RANGE " + b.key, readErr: fmt.Errorf("Range GetObject [%d-%d]: %w", offset, end, err)}
			}
			n, copyErr := io.Copy(h, resp.Body)
			resp.Body.Close()
			totalN += n
			if copyErr != nil {
				return result{key: "RANGE " + b.key, readErr: fmt.Errorf("Range copy [%d-%d]: %w", offset, end, copyErr)}
			}
		}
		var actual [32]byte
		copy(actual[:], h.Sum(nil))
		return result{
			key:      "RANGE " + b.key,
			expected: hex.EncodeToString(b.fullSHA[:]),
			actual:   hex.EncodeToString(actual[:]),
			gotSize:  totalN,
			wantSize: objectSize,
		}
	}

	var allFailures []string
	for iter := 0; iter < iterations; iter++ {
		blobs := makeBlobs(iter)

		// Upload all N blobs in parallel.
		{
			var (
				wg     sync.WaitGroup
				errMu  sync.Mutex
				errors []error
			)
			for i := range blobs {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					if err := uploadOne(blobs[i]); err != nil {
						errMu.Lock()
						errors = append(errors, err)
						errMu.Unlock()
					}
				}(i)
			}
			wg.Wait()
			require.Empty(t, errors, "iter %d: concurrent multipart uploads must succeed", iter)
		}

		// Two passes (full body + range), all blobs verified concurrently.
		results := make([]result, 0, 2*numObjects)
		var resultMu sync.Mutex
		var wg sync.WaitGroup
		for i := range blobs {
			wg.Add(2)
			go func(b blob) {
				defer wg.Done()
				r := verifyFullGET(b)
				resultMu.Lock()
				results = append(results, r)
				resultMu.Unlock()
			}(blobs[i])
			go func(b blob) {
				defer wg.Done()
				r := verifyRangeGET(b)
				resultMu.Lock()
				results = append(results, r)
				resultMu.Unlock()
			}(blobs[i])
		}
		wg.Wait()

		for _, r := range results {
			if r.readErr != nil {
				allFailures = append(allFailures, fmt.Sprintf("[iter %d %s] read error: %v (got %d / want %d bytes)",
					iter, r.key, r.readErr, r.gotSize, r.wantSize))
				continue
			}
			if r.expected != r.actual {
				allFailures = append(allFailures, fmt.Sprintf(
					"[iter %d %s] DIGEST MISMATCH: expected sha256:%s, got sha256:%s (got %d / want %d bytes)",
					iter, r.key, r.expected, r.actual, r.gotSize, r.wantSize))
			}
		}
	}
	if len(allFailures) > 0 {
		t.Fatalf("%d concurrent SSE-S3 multipart blob digest mismatches across %d iterations:\n  %s",
			len(allFailures), iterations, joinFailures(allFailures))
	}
}

func joinFailures(ss []string) string {
	out := ""
	for i, s := range ss {
		if i > 0 {
			out += "\n  "
		}
		out += s
	}
	return out
}
