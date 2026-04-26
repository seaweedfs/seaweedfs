package sse_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// internalChunkSize mirrors the constant in weed/s3api/s3api_object_handlers_put.go
// (the size at which auto-chunking splits a single PUT or part body inside the
// volume-server layer). Range-read coverage that's interesting for SSE has to
// straddle this boundary, since each internal chunk is encrypted with its own
// adjusted IV (SSE-S3 / SSE-KMS) or its own random IV + PartOffset (SSE-C),
// and the read path has to stitch keystreams across chunks correctly.
const internalChunkSize = 8 * 1024 * 1024

// TestSSERangeReadCoverageMatrix is the canonical end-to-end coverage matrix
// for HTTP range GETs across SSE modes, object size classes, and range
// patterns. It supplements the per-SSE-mode TestSSExxxRangeRequests tests
// (which are scoped to small single-chunk objects, ≤1MB) by also exercising
// MEDIUM single-PUT objects that cross one internal 8MB chunk boundary AND
// LARGE multipart objects whose content spans many internal chunks. The
// many-chunk case is the path that broke in #8908 for full-object GETs;
// pinning range correctness here protects against any future regression in
// per-chunk IV / PartOffset plumbing for partial reads.
//
// For SSE-KMS, the test probes once with a 1-byte SSE-KMS PUT and skips the
// SSE-KMS subtests with a clear message if the local server has no KMS
// provider configured (the default `weed mini` setup does not include one;
// the Makefile's `test-with-kms` target does).
func TestSSERangeReadCoverageMatrix(t *testing.T) {
	ctx := context.Background()
	client, err := createS3Client(ctx, defaultConfig)
	require.NoError(t, err, "create S3 client")

	bucketName, err := createTestBucket(ctx, client, defaultConfig.BucketPrefix+"range-matrix-")
	require.NoError(t, err, "create test bucket")
	defer cleanupTestBucket(ctx, client, bucketName)

	modes := []sseRangeMode{
		newRangeModeNone(),
		newRangeModeSSEC(),
		newRangeModeSSEKMS("test-range-coverage-key"),
		newRangeModeSSES3(),
	}

	// Size classes. Sizes are chosen to stress specific boundaries:
	//   small  : single internal chunk, no boundary
	//   medium : one internal chunk boundary (8MB+arbitrary tail)
	//   large  : multipart with parts > 8MB, so each part itself spans
	//            multiple internal chunks AND the object spans multiple
	//            parts -- this is the shape the #8908 fix targets.
	sizes := []sseRangeSize{
		{
			name:           "small_256KB_single_chunk",
			singlePutBytes: 256 * 1024,
		},
		{
			name:           "medium_12MB_one_internal_boundary",
			singlePutBytes: internalChunkSize + 4*1024*1024,
		},
		{
			name: "large_multipart_5x9MB_many_internal_boundaries",
			// 5 parts of 9MB each: 45MB total. Every part exceeds the 8MB
			// internal chunk size, so every part is split into 2 internal
			// chunks (8MB + 1MB), giving ~10 internal chunks across the
			// object. With AWS's 5MB minimum part size, this is the
			// smallest realistic shape that exercises both inter-part
			// stitching and intra-part chunk-boundary crossing.
			multipartParts: []int{
				9 * 1024 * 1024,
				9 * 1024 * 1024,
				9 * 1024 * 1024,
				9 * 1024 * 1024,
				9 * 1024 * 1024,
			},
		},
	}

	for _, mode := range modes {
		mode := mode
		t.Run(mode.name(), func(t *testing.T) {
			if reason := mode.probe(t, ctx, client, bucketName); reason != "" {
				t.Skipf("%s unsupported in this test environment: %s", mode.name(), reason)
			}

			for _, sz := range sizes {
				sz := sz
				t.Run(sz.name, func(t *testing.T) {
					objectKey := fmt.Sprintf("%s/%s", mode.name(), sz.name)
					var data []byte
					if len(sz.multipartParts) > 0 {
						parts := make([][]byte, len(sz.multipartParts))
						for i, n := range sz.multipartParts {
							parts[i] = generateTestData(n)
						}
						mode.uploadMultipart(t, ctx, client, bucketName, objectKey, parts)
						data = bytes.Join(parts, nil)
					} else {
						data = generateTestData(sz.singlePutBytes)
						mode.uploadSingle(t, ctx, client, bucketName, objectKey, data)
					}

					for _, rc := range rangeCasesFor(int64(len(data))) {
						rc := rc
						t.Run(rc.name, func(t *testing.T) {
							verifyRangeRead(t, ctx, client, mode, bucketName, objectKey, data, rc)
						})
					}
				})
			}
		})
	}
}

// rangeCasesFor returns the set of range patterns to exercise on an object of
// the given total length. Some patterns are skipped automatically when the
// object is too small for them to be meaningful (e.g. the many-chunk-spanning
// case requires the object to actually span many internal chunks).
func rangeCasesFor(totalLen int64) []sseRangeCase {
	cases := []sseRangeCase{
		{
			name:        "single_byte_at_zero",
			rangeHeader: "bytes=0-0",
			start:       0,
			end:         0,
		},
		{
			name:        "prefix_512_bytes",
			rangeHeader: "bytes=0-511",
			start:       0,
			end:         511,
		},
		{
			name:        "single_byte_at_last",
			rangeHeader: fmt.Sprintf("bytes=%d-%d", totalLen-1, totalLen-1),
			start:       totalLen - 1,
			end:         totalLen - 1,
		},
		{
			name:        "suffix_last_100_bytes",
			rangeHeader: "bytes=-100",
			start:       totalLen - 100,
			end:         totalLen - 1,
		},
		{
			name:        "open_ended_from_middle",
			rangeHeader: fmt.Sprintf("bytes=%d-", totalLen/2),
			start:       totalLen / 2,
			end:         totalLen - 1,
		},
		{
			name:        "whole_object_as_range",
			rangeHeader: fmt.Sprintf("bytes=0-%d", totalLen-1),
			start:       0,
			end:         totalLen - 1,
		},
	}

	if totalLen >= 64 {
		cases = append(cases, sseRangeCase{
			// AES block-boundary stress: a 17-byte range starting at byte 15
			// crosses the AES block boundary at byte 16. SSE-C historically
			// has the most fragile offset arithmetic here, so this is worth
			// pinning across all modes.
			name:        "mid_chunk_crosses_aes_block_boundary",
			rangeHeader: "bytes=15-31",
			start:       15,
			end:         31,
		})
	}

	if totalLen > internalChunkSize+128 {
		// Range straddles one internal 8MB chunk boundary by 64 bytes on
		// each side. Decryption has to fetch two distinct chunks and stitch
		// the keystreams together correctly -- the path that exposed #8908's
		// SSE-KMS double-IV bug (fixed in #9224 commit 4) and is the most
		// sensitive single test for chunk-boundary stitching.
		cases = append(cases, sseRangeCase{
			name:        "mid_straddles_one_internal_boundary",
			rangeHeader: fmt.Sprintf("bytes=%d-%d", internalChunkSize-64, internalChunkSize+63),
			start:       internalChunkSize - 64,
			end:         internalChunkSize + 63,
		})
	}

	if totalLen > 3*internalChunkSize+128 {
		// Range that spans more than three internal 8MB chunks.  This is
		// the regression path for #8908's read-side issue: the eager
		// multipart reader opened all chunks at once and could close
		// later ones via keepalive while earlier ones were still being
		// drained; range path uses the per-chunk view helpers (always
		// lazy) but a generous-size cross-many-chunks range is still the
		// best end-to-end pin that the per-chunk IV plumbing is correct
		// across part and chunk boundaries.
		start := int64(internalChunkSize/2) + 5
		end := start + 3*internalChunkSize + 17
		if end >= totalLen {
			end = totalLen - 1
		}
		cases = append(cases, sseRangeCase{
			name:        "mid_spans_many_internal_boundaries",
			rangeHeader: fmt.Sprintf("bytes=%d-%d", start, end),
			start:       start,
			end:         end,
		})
	}

	return cases
}

// sseRangeCase is one (start,end) range to GET, with the literal Range header
// to send so we cover both `bytes=N-M`, `bytes=N-`, and `bytes=-N` forms.
type sseRangeCase struct {
	name        string
	rangeHeader string
	start, end  int64 // inclusive byte offsets in the source data
}

type sseRangeSize struct {
	name           string
	singlePutBytes int   // if >0, upload via PutObject of this many random bytes
	multipartParts []int // if non-empty, multipart upload with these part sizes (in bytes)
}

// sseRangeMode is the per-SSE-type behavior plug for the matrix: how to
// configure CreateBucket / PutObject / CreateMultipartUpload / UploadPart /
// GetObject, and what to assert on GET responses.
type sseRangeMode interface {
	name() string
	// probe attempts a 1-byte upload and returns "" on success or a short
	// reason string if the test environment doesn't support this mode (used
	// to t.Skip the SSE-KMS subtests when no KMS provider is configured).
	probe(t *testing.T, ctx context.Context, client *s3.Client, bucket string) string
	uploadSingle(t *testing.T, ctx context.Context, client *s3.Client, bucket, key string, data []byte)
	uploadMultipart(t *testing.T, ctx context.Context, client *s3.Client, bucket, key string, parts [][]byte)
	configureGet(in *s3.GetObjectInput)
	verifyGet(t *testing.T, resp *s3.GetObjectOutput)
}

func newRangeModeNone() sseRangeMode { return &rangeModeNone{} }

type rangeModeNone struct{}

func (m *rangeModeNone) name() string { return "no_sse" }
func (m *rangeModeNone) probe(t *testing.T, ctx context.Context, client *s3.Client, bucket string) string {
	return ""
}
func (m *rangeModeNone) uploadSingle(t *testing.T, ctx context.Context, client *s3.Client, bucket, key string, data []byte) {
	t.Helper()
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	require.NoError(t, err, "PutObject")
}
func (m *rangeModeNone) uploadMultipart(t *testing.T, ctx context.Context, client *s3.Client, bucket, key string, parts [][]byte) {
	t.Helper()
	multipartUpload(t, ctx, client, bucket, key, parts, nil, nil)
}
func (m *rangeModeNone) configureGet(in *s3.GetObjectInput) {}
func (m *rangeModeNone) verifyGet(t *testing.T, resp *s3.GetObjectOutput) {
	t.Helper()
	assert.Empty(t, string(resp.ServerSideEncryption), "no SSE response header expected for plaintext object")
}

func newRangeModeSSEC() sseRangeMode {
	return &rangeModeSSEC{key: generateSSECKey()}
}

type rangeModeSSEC struct {
	key *SSECKey
}

func (m *rangeModeSSEC) name() string { return "sse_c" }
func (m *rangeModeSSEC) probe(t *testing.T, ctx context.Context, client *s3.Client, bucket string) string {
	return ""
}
func (m *rangeModeSSEC) uploadSingle(t *testing.T, ctx context.Context, client *s3.Client, bucket, key string, data []byte) {
	t.Helper()
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:               aws.String(bucket),
		Key:                  aws.String(key),
		Body:                 bytes.NewReader(data),
		SSECustomerAlgorithm: aws.String("AES256"),
		SSECustomerKey:       aws.String(m.key.KeyB64),
		SSECustomerKeyMD5:    aws.String(m.key.KeyMD5),
	})
	require.NoError(t, err, "PutObject SSE-C")
}
func (m *rangeModeSSEC) uploadMultipart(t *testing.T, ctx context.Context, client *s3.Client, bucket, key string, parts [][]byte) {
	t.Helper()
	multipartUpload(t, ctx, client, bucket, key, parts,
		func(in *s3.CreateMultipartUploadInput) {
			in.SSECustomerAlgorithm = aws.String("AES256")
			in.SSECustomerKey = aws.String(m.key.KeyB64)
			in.SSECustomerKeyMD5 = aws.String(m.key.KeyMD5)
		},
		func(in *s3.UploadPartInput) {
			in.SSECustomerAlgorithm = aws.String("AES256")
			in.SSECustomerKey = aws.String(m.key.KeyB64)
			in.SSECustomerKeyMD5 = aws.String(m.key.KeyMD5)
		},
	)
}
func (m *rangeModeSSEC) configureGet(in *s3.GetObjectInput) {
	in.SSECustomerAlgorithm = aws.String("AES256")
	in.SSECustomerKey = aws.String(m.key.KeyB64)
	in.SSECustomerKeyMD5 = aws.String(m.key.KeyMD5)
}
func (m *rangeModeSSEC) verifyGet(t *testing.T, resp *s3.GetObjectOutput) {
	t.Helper()
	assert.Equal(t, "AES256", aws.ToString(resp.SSECustomerAlgorithm))
	assert.Equal(t, m.key.KeyMD5, aws.ToString(resp.SSECustomerKeyMD5))
}

func newRangeModeSSEKMS(keyID string) sseRangeMode {
	return &rangeModeSSEKMS{keyID: keyID}
}

type rangeModeSSEKMS struct {
	keyID string
}

func (m *rangeModeSSEKMS) name() string { return "sse_kms" }
func (m *rangeModeSSEKMS) probe(t *testing.T, ctx context.Context, client *s3.Client, bucket string) string {
	t.Helper()
	probeKey := "__probe__" + m.name()
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:               aws.String(bucket),
		Key:                  aws.String(probeKey),
		Body:                 bytes.NewReader([]byte{0}),
		ServerSideEncryption: types.ServerSideEncryptionAwsKms,
		SSEKMSKeyId:          aws.String(m.keyID),
	})
	if err != nil {
		// Treat any 5xx / "InternalError" / connection-level failure as
		// "no KMS configured here". The test environments where SSE-KMS is
		// expected to work (Makefile target `test-with-kms`) configure a
		// provider; default `weed mini` does not.
		var apiErr *smithyhttp.ResponseError
		if errors.As(err, &apiErr) && apiErr.HTTPStatusCode() >= 500 {
			return fmt.Sprintf("KMS provider not configured (PutObject returned %d)", apiErr.HTTPStatusCode())
		}
		return fmt.Sprintf("KMS PutObject probe failed: %v", err)
	}
	// Best-effort cleanup of the probe object.
	_, _ = client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(probeKey),
	})
	return ""
}
func (m *rangeModeSSEKMS) uploadSingle(t *testing.T, ctx context.Context, client *s3.Client, bucket, key string, data []byte) {
	t.Helper()
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:               aws.String(bucket),
		Key:                  aws.String(key),
		Body:                 bytes.NewReader(data),
		ServerSideEncryption: types.ServerSideEncryptionAwsKms,
		SSEKMSKeyId:          aws.String(m.keyID),
	})
	require.NoError(t, err, "PutObject SSE-KMS")
}
func (m *rangeModeSSEKMS) uploadMultipart(t *testing.T, ctx context.Context, client *s3.Client, bucket, key string, parts [][]byte) {
	t.Helper()
	multipartUpload(t, ctx, client, bucket, key, parts,
		func(in *s3.CreateMultipartUploadInput) {
			in.ServerSideEncryption = types.ServerSideEncryptionAwsKms
			in.SSEKMSKeyId = aws.String(m.keyID)
		},
		// SSE-KMS does not require per-part headers (server reuses upload-init key).
		nil,
	)
}
func (m *rangeModeSSEKMS) configureGet(in *s3.GetObjectInput) {}
func (m *rangeModeSSEKMS) verifyGet(t *testing.T, resp *s3.GetObjectOutput) {
	t.Helper()
	assert.Equal(t, types.ServerSideEncryptionAwsKms, resp.ServerSideEncryption)
	assert.Equal(t, m.keyID, aws.ToString(resp.SSEKMSKeyId))
}

func newRangeModeSSES3() sseRangeMode { return &rangeModeSSES3{} }

type rangeModeSSES3 struct{}

func (m *rangeModeSSES3) name() string { return "sse_s3" }
func (m *rangeModeSSES3) probe(t *testing.T, ctx context.Context, client *s3.Client, bucket string) string {
	return ""
}
func (m *rangeModeSSES3) uploadSingle(t *testing.T, ctx context.Context, client *s3.Client, bucket, key string, data []byte) {
	t.Helper()
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:               aws.String(bucket),
		Key:                  aws.String(key),
		Body:                 bytes.NewReader(data),
		ServerSideEncryption: types.ServerSideEncryptionAes256,
	})
	require.NoError(t, err, "PutObject SSE-S3")
}
func (m *rangeModeSSES3) uploadMultipart(t *testing.T, ctx context.Context, client *s3.Client, bucket, key string, parts [][]byte) {
	t.Helper()
	multipartUpload(t, ctx, client, bucket, key, parts,
		func(in *s3.CreateMultipartUploadInput) {
			in.ServerSideEncryption = types.ServerSideEncryptionAes256
		},
		// SSE-S3 multipart parts inherit encryption from the upload init.
		nil,
	)
}
func (m *rangeModeSSES3) configureGet(in *s3.GetObjectInput) {}
func (m *rangeModeSSES3) verifyGet(t *testing.T, resp *s3.GetObjectOutput) {
	t.Helper()
	assert.Equal(t, types.ServerSideEncryptionAes256, resp.ServerSideEncryption)
}

// multipartUpload is a small helper shared across SSE modes that need to
// assemble the test object via Create / UploadPart / Complete with optional
// per-mode header injection. It registers a t.Cleanup that aborts the upload
// if Complete didn't run successfully, so a test failure mid-way doesn't
// leave an orphan upload behind.
func multipartUpload(t *testing.T, ctx context.Context, client *s3.Client, bucket, key string, parts [][]byte,
	configCreate func(*s3.CreateMultipartUploadInput),
	configPart func(*s3.UploadPartInput)) {
	t.Helper()
	createIn := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	if configCreate != nil {
		configCreate(createIn)
	}
	createResp, err := client.CreateMultipartUpload(ctx, createIn)
	require.NoError(t, err, "CreateMultipartUpload")
	uploadID := aws.ToString(createResp.UploadId)

	completed := false
	t.Cleanup(func() {
		if completed {
			return
		}
		_, _ = client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(bucket),
			Key:      aws.String(key),
			UploadId: aws.String(uploadID),
		})
	})

	completedParts := make([]types.CompletedPart, 0, len(parts))
	for i, part := range parts {
		partNumber := int32(i + 1)
		in := &s3.UploadPartInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String(key),
			PartNumber: aws.Int32(partNumber),
			UploadId:   aws.String(uploadID),
			Body:       bytes.NewReader(part),
		}
		if configPart != nil {
			configPart(in)
		}
		resp, err := client.UploadPart(ctx, in)
		require.NoError(t, err, "UploadPart %d", partNumber)
		completedParts = append(completedParts, types.CompletedPart{
			ETag:       resp.ETag,
			PartNumber: aws.Int32(partNumber),
		})
	}

	_, err = client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          aws.String(bucket),
		Key:             aws.String(key),
		UploadId:        aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{Parts: completedParts},
	})
	require.NoError(t, err, "CompleteMultipartUpload")
	completed = true
}

// verifyRangeRead does the actual GET + assertions for one (mode, object,
// range case). It checks: the body bytes match the source slice; the
// Content-Length header matches the range length; the Content-Range header
// matches the resolved byte range; the SSE response headers match the mode.
func verifyRangeRead(t *testing.T, ctx context.Context, client *s3.Client, mode sseRangeMode,
	bucket, key string, source []byte, rc sseRangeCase) {
	t.Helper()

	totalLen := int64(len(source))
	in := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Range:  aws.String(rc.rangeHeader),
	}
	mode.configureGet(in)

	resp, err := client.GetObject(ctx, in)
	require.NoError(t, err, "GetObject %s range=%s", key, rc.rangeHeader)
	defer resp.Body.Close()

	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "read range body")

	expected := source[rc.start : rc.end+1]
	expectedLen := rc.end - rc.start + 1
	assert.Equal(t, len(expected), len(got),
		"body length mismatch for %s range=%s (source size=%d)", key, rc.rangeHeader, totalLen)
	assert.Equal(t, expectedLen, aws.ToInt64(resp.ContentLength),
		"Content-Length header mismatch for %s range=%s", key, rc.rangeHeader)

	// Content-Range: bytes start-end/total
	wantContentRange := fmt.Sprintf("bytes %d-%d/%d", rc.start, rc.end, totalLen)
	assert.Equal(t, wantContentRange, aws.ToString(resp.ContentRange),
		"Content-Range header mismatch for %s range=%s", key, rc.rangeHeader)

	// Compare bytes with a hash-only assertion to keep failure output small
	// (the actual byte content is random and unhelpful printed verbatim).
	assertDataEqual(t, expected, got, "Range body mismatch for %s range=%s", key, rc.rangeHeader)

	mode.verifyGet(t, resp)

	// Sanity: never accidentally truncate. The bug fixed in #8908 shows up
	// as a fully-readable body that's shorter than expected -- so an
	// explicit "did we get fewer bytes than asked?" check, even though the
	// body-equal assertion above subsumes it, makes regressions far easier
	// to recognize in CI logs than a long byte-diff.
	if int64(len(got)) != expectedLen {
		t.Fatalf("range %s of %s returned %d bytes, expected %d (truncation regression)",
			rc.rangeHeader, key, len(got), expectedLen)
	}
}
