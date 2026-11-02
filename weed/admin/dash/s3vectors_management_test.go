package dash

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3_constants"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3vectors"
)

func newS3VectorsTestServer(t *testing.T) (*AdminServer, *s3tables.ManagerClient) {
	t.Helper()
	fs := s3tablestest.Start(t)
	client := s3tables.NewManagerClient(fs.Client)
	server := s3vectors.NewServer(client, nil, nil)
	server.SetRegion(s3tables.DefaultRegion)
	server.SetAccountID(s3tables.DefaultAccountID)
	return &AdminServer{s3VectorsServer: server}, client
}

// createS3VectorsIndexRecord writes the catalog record CreateIndex leaves
// behind; the admin server has no engine to run CreateIndex itself.
func createS3VectorsIndexRecord(t *testing.T, client *s3tables.ManagerClient, bucket, index string, metadata map[string]string) {
	t.Helper()
	m := s3tables.NewManager()
	m.SetTrusted(true)
	arn := "arn:aws:s3tables:us-east-1:000000000000:bucket/" + bucket
	ctx := context.Background()
	err := m.Execute(ctx, client, "CreateNamespace", &s3tables.CreateNamespaceRequest{TableBucketARN: arn, Namespace: []string{s3tables.VectorIndexNamespace}}, nil, s3_constants.AccountAdminId)
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("create namespace: %v", err)
	}
	full, _ := json.Marshal(metadata)
	if err := m.Execute(ctx, client, "CreateTable", &s3tables.CreateTableRequest{
		TableBucketARN: arn, Namespace: []string{s3tables.VectorIndexNamespace}, Name: index,
		Format: s3tables.FormatLance, Metadata: &s3tables.TableMetadata{FullMetadata: full},
	}, nil, s3_constants.AccountAdminId); err != nil {
		t.Fatalf("create index record: %v", err)
	}
}

func s3VectorsRequest(t *testing.T, handler http.HandlerFunc, method, target, body string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	handler(w, httptest.NewRequest(method, target, strings.NewReader(body)))
	return w
}

func TestS3VectorsBucketsListsOnlyVectorBuckets(t *testing.T) {
	s, client := newS3VectorsTestServer(t)
	ctx := context.Background()
	if w := s3VectorsRequest(t, s.CreateS3VectorsBucket, http.MethodPost, "/api/s3vectors/buckets", `{"name":"media"}`); w.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", w.Code, w.Body)
	}
	if err := s.executeS3VectorsOperation(ctx, "CreateVectorBucket", &s3vectors.CreateVectorBucketRequest{
		VectorBucketName: "sealed", EncryptionConfiguration: map[string]interface{}{"sseType": "AES256"},
	}, nil); err != nil {
		t.Fatalf("create sealed: %v", err)
	}
	if err := s3tables.NewManager().Execute(ctx, client, "CreateTableBucket", &s3tables.CreateTableBucketRequest{
		Name: "warehouse", Format: s3tables.FormatIceberg,
	}, nil, s3_constants.AccountAdminId); err != nil {
		t.Fatalf("create iceberg bucket: %v", err)
	}

	data, err := s.GetS3VectorsBucketsData(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, b := range data.Buckets {
		got[b.Name] = b.Encryption
		if want := "arn:aws:s3vectors:us-east-1:000000000000:bucket/" + b.Name; b.ARN != want || b.CreatedAt.IsZero() {
			t.Errorf("%s: arn %q created %v, want %q and a time", b.Name, b.ARN, b.CreatedAt, want)
		}
	}
	if len(got) != 2 || got["media"] != "" || got["sealed"] != "AES256" || data.TotalBuckets != 2 || data.Truncated {
		t.Fatalf("buckets %v (total %d), want media and sealed (AES256) without the iceberg bucket", got, data.TotalBuckets)
	}
}

func TestS3VectorsBucketShowsIndexesAndPolicy(t *testing.T) {
	s, client := newS3VectorsTestServer(t)
	ctx := context.Background()
	if err := s.executeS3VectorsOperation(ctx, "CreateVectorBucket", &s3vectors.CreateVectorBucketRequest{VectorBucketName: "media"}, nil); err != nil {
		t.Fatal(err)
	}
	createS3VectorsIndexRecord(t, client, "media", "movies", map[string]string{
		"dimension": "3", "distanceMetric": "cosine", "nonFilterableMetadataKeys": `["summary"]`,
	})
	policy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3vectors:QueryVectors","Resource":"*"}]}`
	if err := s.executeS3VectorsOperation(ctx, "PutVectorBucketPolicy", &s3vectors.PutVectorBucketPolicyRequest{VectorBucketName: "media", Policy: policy}, nil); err != nil {
		t.Fatal(err)
	}

	w := s3VectorsRequest(t, s.GetS3VectorsBucketAPI, http.MethodGet, "/api/s3vectors/indexes?bucket=media", "")
	if w.Code != http.StatusOK {
		t.Fatalf("get: %d %s", w.Code, w.Body)
	}
	var data S3VectorsBucketData
	if err := json.Unmarshal(w.Body.Bytes(), &data); err != nil {
		t.Fatal(err)
	}
	if data.Policy != policy {
		t.Errorf("policy %q, want %q", data.Policy, policy)
	}
	if data.TotalIndexes != 1 {
		t.Fatalf("indexes %+v", data.Indexes)
	}
	idx := data.Indexes[0]
	if idx.IndexName != "movies" || idx.Dimension != 3 || idx.DistanceMetric != "cosine" || idx.DataType != "float32" ||
		idx.MetadataConfiguration == nil || len(idx.MetadataConfiguration.NonFilterableMetadataKeys) != 1 {
		t.Fatalf("index %+v", idx)
	}

	if w := s3VectorsRequest(t, s.GetS3VectorsBucketAPI, http.MethodGet, "/api/s3vectors/indexes?bucket=missing", ""); w.Code != http.StatusNotFound {
		t.Fatalf("missing bucket: %d, want 404", w.Code)
	}
}

func TestS3VectorsDeleteIndexThenBucket(t *testing.T) {
	s, client := newS3VectorsTestServer(t)
	ctx := context.Background()
	if err := s.executeS3VectorsOperation(ctx, "CreateVectorBucket", &s3vectors.CreateVectorBucketRequest{VectorBucketName: "media"}, nil); err != nil {
		t.Fatal(err)
	}
	createS3VectorsIndexRecord(t, client, "media", "movies", map[string]string{"dimension": "3", "distanceMetric": "cosine"})

	if w := s3VectorsRequest(t, s.DeleteS3VectorsBucket, http.MethodDelete, "/api/s3vectors/buckets?bucket=media", ""); w.Code != http.StatusConflict {
		t.Fatalf("delete a bucket with an index: %d %s, want 409", w.Code, w.Body)
	}
	if w := s3VectorsRequest(t, s.DeleteS3VectorsIndex, http.MethodDelete, "/api/s3vectors/indexes?bucket=media&index=movies", ""); w.Code != http.StatusOK {
		t.Fatalf("delete index: %d %s", w.Code, w.Body)
	}
	// The emptied index namespace does not keep the bucket alive.
	if w := s3VectorsRequest(t, s.DeleteS3VectorsBucket, http.MethodDelete, "/api/s3vectors/buckets?bucket=media", ""); w.Code != http.StatusOK {
		t.Fatalf("delete bucket: %d %s", w.Code, w.Body)
	}
	data, err := s.GetS3VectorsBucketsData(ctx)
	if err != nil || data.TotalBuckets != 0 {
		t.Fatalf("after delete: %+v, %v", data.Buckets, err)
	}
}

func TestS3VectorsBucketsStopAtTheListLimit(t *testing.T) {
	s, client := newS3VectorsTestServer(t)
	ctx := context.Background()
	// The ICEBERG bucket leaves the first page short, so the last page
	// would carry the list past the limit.
	if err := s3tables.NewManager().Execute(ctx, client, "CreateTableBucket", &s3tables.CreateTableBucketRequest{
		Name: "iceberg", Format: s3tables.FormatIceberg,
	}, nil, s3_constants.AccountAdminId); err != nil {
		t.Fatal(err)
	}
	for i := 0; i <= s3TablesAdminListLimit; i++ {
		if err := s.executeS3VectorsOperation(ctx, "CreateVectorBucket", &s3vectors.CreateVectorBucketRequest{VectorBucketName: fmt.Sprintf("media-%04d", i)}, nil); err != nil {
			t.Fatal(err)
		}
	}
	data, err := s.GetS3VectorsBucketsData(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if data.TotalBuckets != s3TablesAdminListLimit || !data.Truncated {
		t.Fatalf("listed %d buckets (truncated %v), want the limit %d and truncated", data.TotalBuckets, data.Truncated, s3TablesAdminListLimit)
	}
}
