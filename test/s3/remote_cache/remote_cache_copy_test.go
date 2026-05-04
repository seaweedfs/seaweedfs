package remote_cache

import (
	"bytes"
	"crypto/md5"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRemoteCacheCopyObject exercises the bug pattern that #9304 originally
// reported and that #7817's tests couldn't catch: CopyObject reads a source
// whose data lives only in remote storage (not yet cached locally), so its
// resolved entry has FileSize > 0 but no chunks. Pre-fix, the handler wrote a
// destination with that same shape, and any GET on the destination returned
// 500 "data integrity error: size N reported but no content".
func TestRemoteCacheCopyObject(t *testing.T) {
	checkServersRunning(t)

	srcKey := fmt.Sprintf("copy-src-%d.bin", time.Now().UnixNano())
	dstKey := fmt.Sprintf("copy-dst-%d.bin", time.Now().UnixNano())

	// 5 MiB so the source has multiple chunks and is large enough to exercise
	// the streaming copy path, not just an inline-content shortcut.
	srcData := make([]byte, 5*1024*1024)
	for i := range srcData {
		srcData[i] = byte(i % 256)
	}
	srcSum := md5.Sum(srcData)

	t.Log("Uploading source object to primary (local)")
	uploadToPrimary(t, srcKey, srcData)

	t.Log("Uncaching source: pushes data to remote and removes local chunks")
	uncacheLocal(t, srcKey)

	// At this point the source entry on the primary has FileSize > 0,
	// no local chunks, and a RemoteEntry pointing at the secondary.
	// CopyObject must cache the source before persisting the destination.
	t.Log("Issuing CopyObject from remote-only source to a new local destination")
	_, err := getPrimaryClient().CopyObject(&s3.CopyObjectInput{
		Bucket:     aws.String(testBucket),
		Key:        aws.String(dstKey),
		CopySource: aws.String(testBucket + "/" + srcKey),
	})
	require.NoError(t, err, "CopyObject from remote-only source must succeed")

	// HEAD before GET so an obvious size mismatch surfaces with a useful error
	// instead of being masked by a body-stream failure.
	head, err := getPrimaryClient().HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String(dstKey),
	})
	require.NoError(t, err, "HEAD on copied destination must succeed")
	assert.Equal(t, int64(len(srcData)), aws.Int64Value(head.ContentLength),
		"destination ContentLength must match source")

	// The original bug: GET would return 500 with "data integrity error".
	// Now the destination is a real local object with chunks, so GET reads
	// it back byte-for-byte.
	t.Log("Reading destination back; pre-fix this returned 500 'data integrity error'")
	resp, err := getPrimaryClient().GetObject(&s3.GetObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String(dstKey),
	})
	require.NoError(t, err, "GET on copied destination must succeed")
	defer resp.Body.Close()

	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, len(srcData), len(got), "destination size must match source")
	gotSum := md5.Sum(got)
	assert.Equal(t, srcSum, gotSum, "destination bytes must match source bytes")
}

// TestRemoteCacheCopyObjectPart exercises the same bug pattern via the
// multipart upload-part-copy path (CopyObjectPartHandler), which iterates
// the source entry's chunks just like CopyObjectHandler. A remote-only
// source previously produced a part with size > 0 and no data; pre-fix
// the completed multipart upload was unreadable.
func TestRemoteCacheCopyObjectPart(t *testing.T) {
	checkServersRunning(t)

	srcKey := fmt.Sprintf("copypart-src-%d.bin", time.Now().UnixNano())
	dstKey := fmt.Sprintf("copypart-dst-%d.bin", time.Now().UnixNano())

	// 6 MiB so a single 0-5MiB part covers the lower half and exercises a
	// real range-read against the cached chunks (parts must be >= 5 MiB
	// per S3's MultipartUpload rules, except the last).
	srcData := make([]byte, 6*1024*1024)
	for i := range srcData {
		srcData[i] = byte((i * 7) % 256)
	}

	t.Log("Uploading source object to primary (local)")
	uploadToPrimary(t, srcKey, srcData)

	t.Log("Uncaching source: pushes data to remote and removes local chunks")
	uncacheLocal(t, srcKey)

	t.Log("Initiating multipart upload on the destination")
	create, err := getPrimaryClient().CreateMultipartUpload(&s3.CreateMultipartUploadInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String(dstKey),
	})
	require.NoError(t, err)
	uploadID := aws.StringValue(create.UploadId)
	defer func() {
		// Best-effort abort if the test fails mid-flight.
		_, _ = getPrimaryClient().AbortMultipartUpload(&s3.AbortMultipartUploadInput{
			Bucket:   aws.String(testBucket),
			Key:      aws.String(dstKey),
			UploadId: aws.String(uploadID),
		})
	}()

	t.Log("UploadPartCopy from remote-only source range")
	part1Range := fmt.Sprintf("bytes=0-%d", 5*1024*1024-1)
	cp1, err := getPrimaryClient().UploadPartCopy(&s3.UploadPartCopyInput{
		Bucket:          aws.String(testBucket),
		Key:             aws.String(dstKey),
		UploadId:        aws.String(uploadID),
		PartNumber:      aws.Int64(1),
		CopySource:      aws.String(testBucket + "/" + srcKey),
		CopySourceRange: aws.String(part1Range),
	})
	require.NoError(t, err, "UploadPartCopy from remote-only source must succeed")
	require.NotNil(t, cp1.CopyPartResult)

	part2Range := fmt.Sprintf("bytes=%d-%d", 5*1024*1024, 6*1024*1024-1)
	cp2, err := getPrimaryClient().UploadPartCopy(&s3.UploadPartCopyInput{
		Bucket:          aws.String(testBucket),
		Key:             aws.String(dstKey),
		UploadId:        aws.String(uploadID),
		PartNumber:      aws.Int64(2),
		CopySource:      aws.String(testBucket + "/" + srcKey),
		CopySourceRange: aws.String(part2Range),
	})
	require.NoError(t, err, "second UploadPartCopy must succeed")
	require.NotNil(t, cp2.CopyPartResult)

	t.Log("Completing multipart upload")
	_, err = getPrimaryClient().CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(testBucket),
		Key:      aws.String(dstKey),
		UploadId: aws.String(uploadID),
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: []*s3.CompletedPart{
				{ETag: cp1.CopyPartResult.ETag, PartNumber: aws.Int64(1)},
				{ETag: cp2.CopyPartResult.ETag, PartNumber: aws.Int64(2)},
			},
		},
	})
	require.NoError(t, err, "CompleteMultipartUpload must succeed")

	t.Log("Reading destination back; pre-fix this returned 500 'data integrity error'")
	resp, err := getPrimaryClient().GetObject(&s3.GetObjectInput{
		Bucket: aws.String(testBucket),
		Key:    aws.String(dstKey),
	})
	require.NoError(t, err)
	defer resp.Body.Close()

	got, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(srcData, got),
		"destination bytes must match source after multipart copy from remote-only source")
}
