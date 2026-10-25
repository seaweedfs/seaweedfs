package checksum_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
)

// newWhenRequiredChecksumClient returns a client that only sends checksum
// headers when the request asks for them, so tests can upload parts with no
// checksum headers at all.
func newWhenRequiredChecksumClient(t *testing.T) *s3.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(defaultConfig.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			defaultConfig.AccessKey, defaultConfig.SecretKey, "")),
	)
	require.NoError(t, err)
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String(defaultConfig.Endpoint)
		o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
	})
}

// UploadPart without checksum headers inherits the algorithm declared at
// CreateMultipartUpload, so the part still gets a checksum and the upload can
// be completed with it (AWS behavior).
func TestMultipartPartInheritsChecksumAlgorithm(t *testing.T) {
	client := newWhenRequiredChecksumClient(t)

	bucket := uniqueBucket()
	createBucket(t, client, bucket)
	defer cleanupBucket(t, client, bucket)

	body := bytes.Repeat([]byte("y"), 1024)
	key := "inherit-algo"

	create, err := client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String(key),
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
	})
	require.NoError(t, err)

	part, err := client.UploadPart(context.Background(), &s3.UploadPartInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String(key),
		UploadId:   create.UploadId,
		PartNumber: aws.Int32(1),
		Body:       bytes.NewReader(body),
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(part.ChecksumSHA256))

	done, err := client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: create.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{
			ETag:           part.ETag,
			PartNumber:     aws.Int32(1),
			ChecksumSHA256: part.ChecksumSHA256,
		}}},
	})
	require.NoError(t, err)
	require.Equal(t, types.ChecksumTypeComposite, done.ChecksumType)
	require.NotEmpty(t, aws.ToString(done.ChecksumSHA256))
}

// Mimics the .NET repro in https://github.com/seaweedfs/seaweedfs/issues/11401:
// UploadPart carries an explicit client-computed x-amz-checksum-sha256 value,
// CompleteMultipartUpload echoes it back per part, and HeadObject returns the
// object checksum when x-amz-checksum-mode: ENABLED is sent (same as AWS).
func TestIssue11401(t *testing.T) {
	client := getS3Client(t)

	bucket := uniqueBucket()
	createBucket(t, client, bucket)
	defer cleanupBucket(t, client, bucket)

	body := bytes.Repeat([]byte("x"), 1024)
	checksum := base64.StdEncoding.EncodeToString(func() []byte { s := sha256.Sum256(body); return s[:] }())
	key := "issue-11401"

	create, err := client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String(key),
		ContentType:       aws.String("application/octet-stream"),
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
	})
	require.NoError(t, err)

	part, err := client.UploadPart(context.Background(), &s3.UploadPartInput{
		Bucket:         aws.String(bucket),
		Key:            aws.String(key),
		UploadId:       create.UploadId,
		PartNumber:     aws.Int32(1),
		Body:           bytes.NewReader(body),
		ChecksumSHA256: aws.String(checksum),
	})
	require.NoError(t, err)
	require.Equal(t, checksum, aws.ToString(part.ChecksumSHA256))

	done, err := client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: create.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{
			ETag:           part.ETag,
			PartNumber:     aws.Int32(1),
			ChecksumSHA256: part.ChecksumSHA256,
		}}},
	})
	require.NoError(t, err)
	require.Equal(t, types.ChecksumTypeComposite, done.ChecksumType)
	require.NotEmpty(t, aws.ToString(done.ChecksumSHA256))

	head, err := client.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(key),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	require.NoError(t, err)
	require.Equal(t, aws.ToString(done.ChecksumSHA256), aws.ToString(head.ChecksumSHA256))

	headNoMode, err := client.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	require.Empty(t, aws.ToString(headNoMode.ChecksumSHA256))
}

// CompleteMultipartUpload validates the per-part checksums it is given, like
// AWS: missing checksums fail with InvalidRequest and wrong ones with
// BadDigest.
func TestCompleteMultipartUploadValidatesPartChecksums(t *testing.T) {
	client := newWhenRequiredChecksumClient(t)

	bucket := uniqueBucket()
	createBucket(t, client, bucket)
	defer cleanupBucket(t, client, bucket)

	body := bytes.Repeat([]byte("z"), 1024)
	key := "validate-parts"

	create, err := client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String(key),
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
	})
	require.NoError(t, err)

	part, err := client.UploadPart(context.Background(), &s3.UploadPartInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String(key),
		UploadId:   create.UploadId,
		PartNumber: aws.Int32(1),
		Body:       bytes.NewReader(body),
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(part.ChecksumSHA256))

	complete := func(checksum *string) error {
		_, err := client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(bucket),
			Key:      aws.String(key),
			UploadId: create.UploadId,
			MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{
				ETag:           part.ETag,
				PartNumber:     aws.Int32(1),
				ChecksumSHA256: checksum,
			}}},
		})
		return err
	}

	var apiErr smithy.APIError

	err = complete(nil)
	require.Error(t, err)
	require.True(t, errors.As(err, &apiErr))
	require.Equal(t, "InvalidRequest", apiErr.ErrorCode())

	wrong := base64.StdEncoding.EncodeToString(func() []byte { s := sha256.Sum256([]byte("other")); return s[:] }())
	err = complete(aws.String(wrong))
	require.Error(t, err)
	require.True(t, errors.As(err, &apiErr))
	require.Equal(t, "BadDigest", apiErr.ErrorCode())

	require.NoError(t, complete(part.ChecksumSHA256))
}

// FULL_OBJECT uploads need no per-part checksums in the complete request; the
// whole-object checksum travels in a request header and is validated against
// the computed value (BadDigest on mismatch), as on AWS.
func TestCompleteMultipartUploadFullObjectChecksum(t *testing.T) {
	client := newWhenRequiredChecksumClient(t)

	bucket := uniqueBucket()
	createBucket(t, client, bucket)
	defer cleanupBucket(t, client, bucket)

	body := bytes.Repeat([]byte("w"), 1024)
	key := "full-object"

	create, err := client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String(key),
		ChecksumAlgorithm: types.ChecksumAlgorithmCrc64nvme,
	})
	require.NoError(t, err)
	require.Equal(t, types.ChecksumTypeFullObject, create.ChecksumType)

	part, err := client.UploadPart(context.Background(), &s3.UploadPartInput{
		Bucket:     aws.String(bucket),
		Key:        aws.String(key),
		UploadId:   create.UploadId,
		PartNumber: aws.Int32(1),
		Body:       bytes.NewReader(body),
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(part.ChecksumCRC64NVME))

	complete := func(objectChecksum *string) error {
		_, err := client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
			Bucket:            aws.String(bucket),
			Key:               aws.String(key),
			UploadId:          create.UploadId,
			ChecksumCRC64NVME: objectChecksum,
			MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{{
				ETag:       part.ETag,
				PartNumber: aws.Int32(1),
			}}},
		})
		return err
	}

	var apiErr smithy.APIError
	wrong := base64.StdEncoding.EncodeToString(func() []byte { s := sha256.Sum256(body); return s[:] }())
	err = complete(aws.String(wrong))
	require.Error(t, err)
	require.True(t, errors.As(err, &apiErr))
	require.Equal(t, "BadDigest", apiErr.ErrorCode())

	require.NoError(t, complete(part.ChecksumCRC64NVME))
}

// UploadPart with a checksum algorithm that conflicts with the one declared at
// CreateMultipartUpload is rejected, as on AWS.
func TestMultipartPartConflictingAlgorithm(t *testing.T) {
	client := newWhenRequiredChecksumClient(t)

	bucket := uniqueBucket()
	createBucket(t, client, bucket)
	defer cleanupBucket(t, client, bucket)

	create, err := client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String("conflict"),
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
	})
	require.NoError(t, err)

	_, err = client.UploadPart(context.Background(), &s3.UploadPartInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String("conflict"),
		UploadId:          create.UploadId,
		PartNumber:        aws.Int32(1),
		Body:              bytes.NewReader([]byte("data")),
		ChecksumAlgorithm: types.ChecksumAlgorithmCrc32,
	})
	var apiErr smithy.APIError
	require.Error(t, err)
	require.True(t, errors.As(err, &apiErr))
	require.Equal(t, "InvalidRequest", apiErr.ErrorCode())
}
