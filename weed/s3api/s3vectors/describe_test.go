package s3vectors

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"
)

func TestDescribeFillsTableLocation(t *testing.T) {
	fs := s3tablestest.Start(t)
	engine := &fakeVectors{}
	s := NewServer(s3tables.NewManagerClient(fs.Client), allowAllAuth{}, engine)
	var described [][]string
	s.SetDescribe(func(_ context.Context, id []string) (string, map[string]string, error) {
		described = append(described, id)
		return "s3://" + strings.Join(id, "/"), map[string]string{"aws_endpoint": "http://gateway:8333"}, nil
	})

	if w := postJSON(t, s.handleCreateVectorBucket, &CreateVectorBucketRequest{VectorBucketName: "vec-bucket"}); w.Code != http.StatusOK {
		t.Fatalf("create bucket: %d %s", w.Code, w.Body.String())
	}
	if w := postJSON(t, s.handleCreateIndex, &CreateIndexRequest{
		VectorBucketName: "vec-bucket",
		IndexName:        "idx",
		DataType:         "float32",
		Dimension:        3,
		DistanceMetric:   "euclidean",
	}); w.Code != http.StatusOK {
		t.Fatalf("create index: %d %s", w.Code, w.Body.String())
	}
	if w := postJSON(t, s.handlePutVectors, &PutVectorsRequest{
		VectorBucketName: "vec-bucket",
		IndexName:        "idx",
		Vectors:          []*PutInputVector{{Key: "a", Data: &VectorData{Float32: []float32{1, 2, 3}}}},
	}); w.Code != http.StatusOK {
		t.Fatalf("put vectors: %d %s", w.Code, w.Body.String())
	}

	if len(engine.created) != 1 || len(engine.puts) != 1 {
		t.Fatalf("engine calls: %d creates, %d puts", len(engine.created), len(engine.puts))
	}
	for _, table := range []interface{ GetLocation() string }{engine.created[0].Table, engine.puts[0].Table} {
		if table.GetLocation() != "s3://vec-bucket/vector/idx" {
			t.Fatalf("table location = %q", table.GetLocation())
		}
	}
	if engine.puts[0].Table.SseS3 {
		t.Fatal("a bucket without encryption was sent as SSE-S3")
	}
	if len(described) != 2 || strings.Join(described[1], "/") != "vec-bucket/vector/idx" {
		t.Fatalf("described ids = %v", described)
	}
}

// A bucket created with SSE-S3 sends its tables as encrypted.
func TestDescribeCarriesTheBucketsEncryption(t *testing.T) {
	fs := s3tablestest.Start(t)
	engine := &fakeVectors{}
	s := NewServer(s3tables.NewManagerClient(fs.Client), allowAllAuth{}, engine)
	s.SetEncryptionChecker(func() bool { return true })
	s.SetDescribe(func(_ context.Context, id []string) (string, map[string]string, error) {
		return "s3://" + strings.Join(id, "/"), nil, nil
	})
	if w := postJSON(t, s.handleCreateVectorBucket, &CreateVectorBucketRequest{
		VectorBucketName:        "sealed",
		EncryptionConfiguration: map[string]interface{}{"sseType": "AES256"},
	}); w.Code != http.StatusOK {
		t.Fatalf("create bucket: %d %s", w.Code, w.Body.String())
	}
	if w := postJSON(t, s.handleCreateIndex, &CreateIndexRequest{
		VectorBucketName: "sealed",
		IndexName:        "idx",
		DataType:         "float32",
		Dimension:        3,
		DistanceMetric:   "euclidean",
	}); w.Code != http.StatusOK {
		t.Fatalf("create index: %d %s", w.Code, w.Body.String())
	}
	if len(engine.created) != 1 || !engine.created[0].Table.SseS3 {
		t.Fatalf("engine CreateIndex calls = %v, want one SSE-S3 table", engine.created)
	}
}

func TestDescribeErrorsReachTheCaller(t *testing.T) {
	fs := s3tablestest.Start(t)
	cases := []struct {
		err  error
		code int
		body string
	}{
		{&s3tables.S3TablesError{Type: s3tables.ErrCodeNoSuchTable, Message: "gone"}, http.StatusNotFound, "NotFoundException"},
		{&s3tables.S3TablesError{Type: s3tables.ErrCodeAccessDenied, Message: "no"}, http.StatusForbidden, "AccessDeniedException"},
		{fmt.Errorf("lance catalog is not running"), http.StatusInternalServerError, "InternalFailure"},
	}
	engine := &fakeVectors{}
	s := NewServer(s3tables.NewManagerClient(fs.Client), allowAllAuth{}, engine)
	var describeErr error
	s.SetDescribe(func(context.Context, []string) (string, map[string]string, error) {
		return "", nil, describeErr
	})
	if w := postJSON(t, s.handleCreateVectorBucket, &CreateVectorBucketRequest{VectorBucketName: "vec-bucket"}); w.Code != http.StatusOK {
		t.Fatalf("create bucket: %d %s", w.Code, w.Body.String())
	}
	for _, tc := range cases {
		describeErr = tc.err
		w := postJSON(t, s.handleCreateIndex, &CreateIndexRequest{
			VectorBucketName: "vec-bucket",
			IndexName:        "idx",
			DataType:         "float32",
			Dimension:        3,
			DistanceMetric:   "euclidean",
		})
		if w.Code != tc.code || !strings.Contains(w.Body.String(), tc.body) {
			t.Fatalf("%v: %d %s, want %d %s", tc.err, w.Code, w.Body.String(), tc.code, tc.body)
		}
		if len(engine.created) != 0 {
			t.Fatalf("%v: the engine was called", tc.err)
		}
	}
}
