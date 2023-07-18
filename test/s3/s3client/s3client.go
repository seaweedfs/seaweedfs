package main

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"time"
)

func main() {
	cfg := MyConfig{
		Key:    "any",
		Secret: "any",
		Region: "US",
		Endpoint: MyEndpoint{
			URL: "http://localhost:8333",
		},
		Bucket: MyBucketConfig{
			Name:       "newbucket",
			Versioning: false,
		},
		MaxBackoffDelay:  aws.Int(int(time.Second * 5)),
		MaxRetryAttempts: aws.Int(1),
	}

	awsCfg, err := MyAwsConfig(cfg)
	if err != nil {
		panic(err)
	}
	svc := s3.NewFromConfig(*awsCfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	// Use the S3 client to interact with SeaweedFS
	// ...
	// Example: List all buckets
	result, err := svc.ListBuckets(context.Background(), &s3.ListBucketsInput{})
	// no errors - got list of buckets
	if err != nil {
		panic(err)
	}

	// Print the list of buckets
	for _, bucket := range result.Buckets {
		println(*bucket.Name)
	}

	bucket := "bucket1"
	_, err = svc.HeadBucket(context.Background(), &s3.HeadBucketInput{Bucket: &bucket})
	// ERROR HERE
	if err != nil {
		println(err)
	}

}

// === helpers

func MyAwsConfig(cfg MyConfig) (*aws.Config, error) {

	cred := aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(cfg.Key, cfg.Secret, ""))
	customResolver := aws.EndpointResolverWithOptionsFunc(
		func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				URL:           cfg.Endpoint.URL,
				SigningRegion: cfg.Region,
			}, nil
		})

	awsCfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(cfg.Region),
		config.WithCredentialsProvider(cred),
		config.WithEndpointResolverWithOptions(customResolver),
		config.WithRetryer(func() aws.Retryer {
			r := retry.AddWithMaxAttempts(retry.NewStandard(), *cfg.MaxRetryAttempts)
			return retry.AddWithMaxBackoffDelay(r, time.Duration(*cfg.MaxBackoffDelay*1000*1000))
		}))
	return &awsCfg, err
}

type MyConfig struct {
	// Access key of S3 AWS.
	Key string
	// Access secret of S3 AWS.
	Secret string
	// Region.
	Region string
	// AWS endpoint.
	Endpoint MyEndpoint
	// Bucket configuration.
	Bucket MyBucketConfig
	// File access.
	FileAccess MyFileAccessType
	// Maximum backoff delay (ms, default: 20 sec).
	MaxBackoffDelay *int
	// Maximum attempts to retry operation on error (default: 5).
	MaxRetryAttempts *int
}

type MyBucketConfig struct {
	// Name of bucket
	Name string
	// Enable or not versioning
	Versioning bool
}

type MyEndpoint struct {
	URL string
}

type MyFileAccessType byte
