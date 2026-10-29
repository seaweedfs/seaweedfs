package main

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func main() {
	client := s3.New(s3.Options{
		BaseEndpoint: aws.String("http://127.0.0.1:8333"),
		Region:       "us-east-1",
		Credentials:  credentials.NewStaticCredentialsProvider("k", "s", ""),
		UsePathStyle: true,
	})
	ctx := context.Background()
	bucket, key := "racebucket", "obj"
	client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &bucket})

	const N = 8
	uploadIds := make([]string, N)
	parts := make([]*types.CompletedPart, N)
	for i := 0; i < N; i++ {
		out, err := client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{Bucket: &bucket, Key: &key})
		if err != nil {
			panic(err)
		}
		uploadIds[i] = *out.UploadId
		p, err := client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket: &bucket, Key: &key, UploadId: &uploadIds[i], PartNumber: aws.Int32(1),
			Body: strings.NewReader(fmt.Sprintf("%05d-part-%d", i, i)),
		})
		if err != nil {
			panic(err)
		}
		parts[i] = &types.CompletedPart{PartNumber: aws.Int32(1), ETag: p.ETag}
	}

	var wg sync.WaitGroup
	start := make(chan struct{})
	results := make([]string, N)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, err := client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
				Bucket: &bucket, Key: &key, UploadId: &uploadIds[i],
				IfNoneMatch: aws.String("*"),
				MultipartUpload: &types.CompletedMultipartUpload{
					Parts: []types.CompletedPart{*parts[i]},
				},
			})
			if err != nil {
				results[i] = fmt.Sprintf("FAIL %v", err)
			} else {
				results[i] = "OK"
			}
		}(i)
	}
	close(start)
	wg.Wait()
	ok := 0
	for i, r := range results {
		fmt.Printf("%d: %s\n", i, r)
		if r == "OK" {
			ok++
		}
	}
	fmt.Printf("== %d of %d succeeded (want exactly 1)\n", ok, N)
}
