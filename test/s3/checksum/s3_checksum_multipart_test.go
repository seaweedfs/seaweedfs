package checksum_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"
)

// newTrailerChecksumClient returns a client that can send flexible checksums.
// aws-sdk-go-v2 sends them as unsigned streaming trailers, which it refuses over
// plain HTTP, so front the HTTP endpoint with a TLS proxy.
func newTrailerChecksumClient(t *testing.T) *s3.Client {
	t.Helper()

	target, err := url.Parse(defaultConfig.Endpoint)
	require.NoError(t, err)

	proxy := httputil.NewSingleHostReverseProxy(target)
	server := httptest.NewTLSServer(proxy)
	t.Cleanup(server.Close)

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(defaultConfig.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			defaultConfig.AccessKey, defaultConfig.SecretKey, "")),
		config.WithHTTPClient(server.Client()),
	)
	require.NoError(t, err)
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(server.URL)
		o.UsePathStyle = true
	})
}

func TestMultipartCopyPreservesChecksum(t *testing.T) {
	client := newTrailerChecksumClient(t)

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

// A multipart upload that asked for a checksum must report it in the
// CompleteMultipartUpload response, alongside the algorithm and type that
// CreateMultipartUpload echoed back.
func TestMultipartUploadReturnsObjectChecksum(t *testing.T) {
	client := newTrailerChecksumClient(t)

	bucket := uniqueBucket()
	createBucket(t, client, bucket)
	defer cleanupBucket(t, client, bucket)

	cases := []struct {
		algorithm    types.ChecksumAlgorithm
		expectedType types.ChecksumType
		completeSum  func(*s3.CompleteMultipartUploadOutput) *string
		headSum      func(*s3.HeadObjectOutput) *string
		partSum      func(*s3.UploadPartOutput) *string
		setPart      func(*types.CompletedPart, *string)
	}{
		{
			algorithm:    types.ChecksumAlgorithmCrc32,
			expectedType: types.ChecksumTypeComposite,
			completeSum:  func(o *s3.CompleteMultipartUploadOutput) *string { return o.ChecksumCRC32 },
			headSum:      func(o *s3.HeadObjectOutput) *string { return o.ChecksumCRC32 },
			partSum:      func(o *s3.UploadPartOutput) *string { return o.ChecksumCRC32 },
			setPart:      func(p *types.CompletedPart, v *string) { p.ChecksumCRC32 = v },
		},
		{
			algorithm:    types.ChecksumAlgorithmCrc32c,
			expectedType: types.ChecksumTypeComposite,
			completeSum:  func(o *s3.CompleteMultipartUploadOutput) *string { return o.ChecksumCRC32C },
			headSum:      func(o *s3.HeadObjectOutput) *string { return o.ChecksumCRC32C },
			partSum:      func(o *s3.UploadPartOutput) *string { return o.ChecksumCRC32C },
			setPart:      func(p *types.CompletedPart, v *string) { p.ChecksumCRC32C = v },
		},
		{
			algorithm:    types.ChecksumAlgorithmCrc64nvme,
			expectedType: types.ChecksumTypeFullObject,
			completeSum:  func(o *s3.CompleteMultipartUploadOutput) *string { return o.ChecksumCRC64NVME },
			headSum:      func(o *s3.HeadObjectOutput) *string { return o.ChecksumCRC64NVME },
			partSum:      func(o *s3.UploadPartOutput) *string { return o.ChecksumCRC64NVME },
			setPart:      func(p *types.CompletedPart, v *string) { p.ChecksumCRC64NVME = v },
		},
		{
			algorithm:    types.ChecksumAlgorithmSha1,
			expectedType: types.ChecksumTypeComposite,
			completeSum:  func(o *s3.CompleteMultipartUploadOutput) *string { return o.ChecksumSHA1 },
			headSum:      func(o *s3.HeadObjectOutput) *string { return o.ChecksumSHA1 },
			partSum:      func(o *s3.UploadPartOutput) *string { return o.ChecksumSHA1 },
			setPart:      func(p *types.CompletedPart, v *string) { p.ChecksumSHA1 = v },
		},
		{
			algorithm:    types.ChecksumAlgorithmSha256,
			expectedType: types.ChecksumTypeComposite,
			completeSum:  func(o *s3.CompleteMultipartUploadOutput) *string { return o.ChecksumSHA256 },
			headSum:      func(o *s3.HeadObjectOutput) *string { return o.ChecksumSHA256 },
			partSum:      func(o *s3.UploadPartOutput) *string { return o.ChecksumSHA256 },
			setPart:      func(p *types.CompletedPart, v *string) { p.ChecksumSHA256 = v },
		},
	}

	// Every part but the last has to reach the 5MB multipart minimum.
	parts := [][]byte{bytes.Repeat([]byte("a"), 5*1024*1024), []byte("tail")}

	for _, tc := range cases {
		t.Run(string(tc.algorithm), func(t *testing.T) {
			key := "multipart-" + string(tc.algorithm)
			create, err := client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
				Bucket:            aws.String(bucket),
				Key:               aws.String(key),
				ChecksumAlgorithm: tc.algorithm,
			})
			require.NoError(t, err)
			require.Equal(t, tc.algorithm, create.ChecksumAlgorithm)
			require.Equal(t, tc.expectedType, create.ChecksumType)

			var completed []types.CompletedPart
			for i, data := range parts {
				part, err := client.UploadPart(context.Background(), &s3.UploadPartInput{
					Bucket:            aws.String(bucket),
					Key:               aws.String(key),
					UploadId:          create.UploadId,
					PartNumber:        aws.Int32(int32(i + 1)),
					Body:              bytes.NewReader(data),
					ChecksumAlgorithm: tc.algorithm,
				})
				require.NoError(t, err)
				require.NotEmpty(t, aws.ToString(tc.partSum(part)))

				entry := types.CompletedPart{ETag: part.ETag, PartNumber: aws.Int32(int32(i + 1))}
				tc.setPart(&entry, tc.partSum(part))
				completed = append(completed, entry)
			}

			done, err := client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
				Bucket:          aws.String(bucket),
				Key:             aws.String(key),
				UploadId:        create.UploadId,
				MultipartUpload: &types.CompletedMultipartUpload{Parts: completed},
			})
			require.NoError(t, err)
			require.NotEmpty(t, aws.ToString(tc.completeSum(done)))
			require.Equal(t, tc.expectedType, done.ChecksumType)
			if tc.expectedType == types.ChecksumTypeComposite {
				require.True(t, strings.HasSuffix(aws.ToString(tc.completeSum(done)), fmt.Sprintf("-%d", len(parts))))
			}

			head, err := client.HeadObject(context.Background(), &s3.HeadObjectInput{
				Bucket:       aws.String(bucket),
				Key:          aws.String(key),
				ChecksumMode: types.ChecksumModeEnabled,
			})
			require.NoError(t, err)
			require.Equal(t, aws.ToString(tc.completeSum(done)), aws.ToString(tc.headSum(head)))
		})
	}
}
