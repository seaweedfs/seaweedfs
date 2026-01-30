package s3tables

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"
)

// TestCluster manages the weed mini instance for integration testing
type TestCluster struct {
	t          *testing.T
	dataDir    string
	ctx        context.Context
	cancel     context.CancelFunc
	isRunning  bool
	startOnce  sync.Once
	wg         sync.WaitGroup
	masterPort int
	volumePort int
	filerPort  int
	s3Port     int
	s3Endpoint string
}

// S3TablesClient is a simple client for S3 Tables API
type S3TablesClient struct {
	endpoint  string
	region    string
	accessKey string
	secretKey string
	client    *http.Client
}

// NewS3TablesClient creates a new S3 Tables client
func NewS3TablesClient(endpoint, region, accessKey, secretKey string) *S3TablesClient {
	return &S3TablesClient{
		endpoint:  endpoint,
		region:    region,
		accessKey: accessKey,
		secretKey: secretKey,
		client:    &http.Client{Timeout: 30 * time.Second},
	}
}

// Test configuration constants
const (
	testRegion    = "us-west-2"
	testAccessKey = "admin"
	testSecretKey = "admin"
	testAccountID = "111122223333"
)
