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

func TestMultipartCopyPreservesChecksum(t *testing.T) {
	// aws-sdk-go-v2 sends flexible checksums as unsigned streaming trailers, which
	// it refuses over plain HTTP, so front the HTTP endpoint with a TLS proxy.
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

	cases := []struct {
		algorithm types.ChecksumAlgorithm
		srcSum    func(*s3.PutObjectOutput) *string
		partSum   func(*types.CopyPartResult) *string
		setPart   func(*types.CompletedPart, *string)
	}{
		{
			algorithm: types.ChecksumAlgorithmCrc32,
			srcSum:    func(o *s3.PutObjectOutput) *string { return o.ChecksumCRC32 },
			partSum:   func(r *types.CopyPartResult) *string { return r.ChecksumCRC32 },
			setPart:   func(p *types.CompletedPart, v *string) { p.ChecksumCRC32 = v },
		},
		{
			algorithm: types.ChecksumAlgorithmCrc32c,
			srcSum:    func(o *s3.PutObjectOutput) *string { return o.ChecksumCRC32C },
			partSum:   func(r *types.CopyPartResult) *string { return r.ChecksumCRC32C },
			setPart:   func(p *types.CompletedPart, v *string) { p.ChecksumCRC32C = v },
		},
		{
			algorithm: types.ChecksumAlgorithmCrc64nvme,
			srcSum:    func(o *s3.PutObjectOutput) *string { return o.ChecksumCRC64NVME },
			partSum:   func(r *types.CopyPartResult) *string { return r.ChecksumCRC64NVME },
			setPart:   func(p *types.CompletedPart, v *string) { p.ChecksumCRC64NVME = v },
		},
		{
			algorithm: types.ChecksumAlgorithmSha1,
			srcSum:    func(o *s3.PutObjectOutput) *string { return o.ChecksumSHA1 },
			partSum:   func(r *types.CopyPartResult) *string { return r.ChecksumSHA1 },
			setPart:   func(p *types.CompletedPart, v *string) { p.ChecksumSHA1 = v },
		},
		{
			algorithm: types.ChecksumAlgorithmSha256,
			srcSum:    func(o *s3.PutObjectOutput) *string { return o.ChecksumSHA256 },
			partSum:   func(r *types.CopyPartResult) *string { return r.ChecksumSHA256 },
			setPart:   func(p *types.CompletedPart, v *string) { p.ChecksumSHA256 = v },
		},
	}

	for _, tc := range cases {
		t.Run(string(tc.algorithm), func(t *testing.T) {
			sourceKey := "source-" + string(tc.algorithm)
			source, err := client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket:            aws.String(bucket),
				Key:               aws.String(sourceKey),
				Body:              bytes.NewBufferString("trailing checksum"),
				ChecksumAlgorithm: tc.algorithm,
			})
			require.NoError(t, err)

			key := "multipart-copy-" + string(tc.algorithm)
			create, err := client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
				Bucket:            aws.String(bucket),
				Key:               aws.String(key),
				ChecksumAlgorithm: tc.algorithm,
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
			require.Equal(t, aws.ToString(tc.srcSum(source)), aws.ToString(tc.partSum(part.CopyPartResult)))

			completed := types.CompletedPart{ETag: part.CopyPartResult.ETag, PartNumber: aws.Int32(1)}
			tc.setPart(&completed, tc.partSum(part.CopyPartResult))
			_, err = client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
				Bucket:          aws.String(bucket),
				Key:             aws.String(key),
				UploadId:        create.UploadId,
				MultipartUpload: &types.CompletedMultipartUpload{Parts: []types.CompletedPart{completed}},
			})
			require.NoError(t, err)
		})
	}
}
