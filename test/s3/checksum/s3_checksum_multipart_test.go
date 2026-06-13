package checksum_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"
)

func TestMultipartCopyPreservesCRC64NVME(t *testing.T) {
	// aws-sdk-go-v2 sends CRC64NVME as an unsigned streaming trailer, which it
	// refuses over plain HTTP, so front the HTTP endpoint with a TLS proxy.
	target, err := url.Parse(defaultConfig.Endpoint)
	require.NoError(t, err)

	proxy := httputil.NewSingleHostReverseProxy(target)
	server := httptest.NewTLSServer(proxy)
	defer server.Close()

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(defaultConfig.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			defaultConfig.AccessKey, defaultConfig.SecretKey, "")),
		config.WithHTTPClient(server.Client()),
	)
	require.NoError(t, err)
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(server.URL)
		o.UsePathStyle = true
	})

	bucket := uniqueBucket()
	createBucket(t, client, bucket)
	defer cleanupBucket(t, client, bucket)

	sourceKey := "source"
	source, err := client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String(sourceKey),
		Body:              bytes.NewBufferString("trailing checksum"),
		ChecksumAlgorithm: types.ChecksumAlgorithmCrc64nvme,
	})
	require.NoError(t, err)

	key := "multipart-copy-crc64nvme"
	create, err := client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
		Bucket:            aws.String(bucket),
		Key:               aws.String(key),
		ChecksumAlgorithm: types.ChecksumAlgorithmCrc64nvme,
	})
	require.NoError(t, err)

	part, err := client.UploadPartCopy(context.Background(), &s3.UploadPartCopyInput{
		Bucket:     aws.String(bucket),
		CopySource: aws.String(fmt.Sprintf("%s/%s", bucket, sourceKey)),
		Key:        aws.String(key),
		UploadId:   create.UploadId,
		PartNumber: aws.Int32(1),
	})
	require.NoError(t, err)
	require.Equal(t, aws.ToString(source.ChecksumCRC64NVME), aws.ToString(part.CopyPartResult.ChecksumCRC64NVME))

	_, err = client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: create.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{{
				ETag:              part.CopyPartResult.ETag,
				PartNumber:        aws.Int32(1),
				ChecksumCRC64NVME: part.CopyPartResult.ChecksumCRC64NVME,
			}},
		},
	})
	require.NoError(t, err)
}
