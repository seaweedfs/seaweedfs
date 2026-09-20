package checksum_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
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
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, _ ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL:               defaultConfig.Endpoint,
					SigningRegion:     defaultConfig.Region,
					HostnameImmutable: true,
				}, nil
			})),
	)
	require.NoError(t, err)
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
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
