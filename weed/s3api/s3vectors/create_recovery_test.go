package s3vectors

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/vector_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type failingCreateVectors struct {
	*fakeVectors
	err      error
	onCreate func()
}

func (f *failingCreateVectors) CreateIndex(_ context.Context, request *vector_pb.CreateIndexRequest, _ ...grpc.CallOption) (*vector_pb.CreateIndexResponse, error) {
	f.created = append(f.created, request)
	err := f.err
	if f.onCreate != nil {
		f.onCreate()
	}
	return nil, err
}

func TestCreateIndexPreservesAnUnknownOutcome(t *testing.T) {
	for _, code := range []codes.Code{codes.Unavailable, codes.DeadlineExceeded, codes.Canceled, codes.Internal, codes.InvalidArgument} {
		t.Run(code.String(), func(t *testing.T) {
			fs := s3tablestest.Start(t)
			engine := &failingCreateVectors{fakeVectors: &fakeVectors{}, err: status.Error(code, "response lost")}
			server := NewServer(s3tables.NewManagerClient(fs.Client), allowAllAuth{}, engine)
			postJSON(t, server.handleCreateVectorBucket, &CreateVectorBucketRequest{VectorBucketName: "vec-bucket"})
			result := postJSON(t, server.handleCreateIndex, &CreateIndexRequest{VectorBucketName: "vec-bucket", IndexName: "idx", DataType: "float32", Dimension: 3, DistanceMetric: "cosine"})
			if result.Code == http.StatusOK {
				t.Fatal("failed engine reported success")
			}
			got := postJSON(t, server.handleGetIndex, &GetIndexRequest{VectorBucketName: "vec-bucket", IndexName: "idx"})
			want := http.StatusOK
			if code == codes.InvalidArgument {
				want = http.StatusNotFound
			}
			if got.Code != want {
				t.Fatalf("catalog after %s: %d %s", code, got.Code, got.Body)
			}
		})
	}
}

func TestCreateIndexRetainsTheWriteErrorIfRediscoveryFails(t *testing.T) {
	engine := &failingCreateVectors{fakeVectors: &fakeVectors{}, err: status.Error(codes.Unavailable, "response lost after commit")}
	discovery := &vectorDiscovery{client: engine, nextResolve: time.Now().Add(time.Hour), discover: func([]pb.ServerAddress, grpc.DialOption) []string { return nil }}
	_, err := discovery.CreateIndex(t.Context(), &vector_pb.CreateIndexRequest{})
	if status.Convert(err).Message() != "response lost after commit" || len(engine.created) != 1 {
		t.Fatalf("ambiguous result replaced by lookup failure: %v", err)
	}
}

func TestCreateIndexResumesAnUnknownOutcome(t *testing.T) {
	fs := s3tablestest.Start(t)
	engine := &failingCreateVectors{fakeVectors: &fakeVectors{}, err: status.Error(codes.Unavailable, "request lost")}
	server := NewServer(s3tables.NewManagerClient(fs.Client), allowAllAuth{}, engine)
	postJSON(t, server.handleCreateVectorBucket, &CreateVectorBucketRequest{VectorBucketName: "vec-bucket"})
	request := &CreateIndexRequest{VectorBucketName: "vec-bucket", IndexName: "idx", DataType: "float32", Dimension: 3, DistanceMetric: "cosine",
		MetadataConfiguration: &MetadataConfiguration{NonFilterableMetadataKeys: []string{"b", "a"}}, Tags: map[string]string{"team": "search"}}
	if result := postJSON(t, server.handleCreateIndex, request); result.Code == http.StatusOK {
		t.Fatal("unavailable engine reported success")
	}
	before, err := server.indexRecord(t.Context(), "vec-bucket", "idx")
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*CreateIndexRequest){
		func(r *CreateIndexRequest) { r.Dimension++ },
		func(r *CreateIndexRequest) { r.DistanceMetric = "euclidean" },
		func(r *CreateIndexRequest) { r.MetadataConfiguration = nil },
	} {
		other := *request
		change(&other)
		if result := postJSON(t, server.handleCreateIndex, &other); result.Code != http.StatusConflict {
			t.Fatalf("incompatible retry: %d %s", result.Code, result.Body)
		}
	}
	if len(engine.created) != 1 {
		t.Fatal("incompatible retry reached the engine")
	}
	engine.err = status.Error(codes.InvalidArgument, "retry refused")
	postJSON(t, server.handleCreateIndex, request)
	if _, err := server.indexRecord(t.Context(), "vec-bucket", "idx"); err != nil {
		t.Fatalf("retry removed an uncertain dataset's catalog: %v", err)
	}
	engine.err = nil
	request.MetadataConfiguration.NonFilterableMetadataKeys = []string{"a", "b"}
	if result := postJSON(t, server.handleCreateIndex, request); result.Code != http.StatusOK {
		t.Fatalf("resume: %d %s", result.Code, result.Body)
	}
	after, err := server.indexRecord(t.Context(), "vec-bucket", "idx")
	if err != nil || recordNonce(before) != recordNonce(after) {
		t.Fatalf("resume replaced the catalog: %v", err)
	}
	if result := postJSON(t, server.handleCreateIndex, request); result.Code != http.StatusConflict {
		t.Fatalf("completed index was initialized again: %d %s", result.Code, result.Body)
	}
	result := postJSON(t, server.handleListTagsForResource, &ListTagsForResourceRequest{ResourceArn: server.indexArn("vec-bucket", "idx")})
	var tags ListTagsForResourceResponse
	if err := json.Unmarshal(result.Body.Bytes(), &tags); err != nil || tags.Tags["team"] != "search" {
		t.Fatalf("resume lost tags: %s (%v)", result.Body, err)
	}
}

func TestCreateIndexRollbackPreservesACompletedRetry(t *testing.T) {
	fs := s3tablestest.Start(t)
	engine := &failingCreateVectors{fakeVectors: &fakeVectors{}, err: status.Error(codes.InvalidArgument, "first attempt failed")}
	server := NewServer(s3tables.NewManagerClient(fs.Client), allowAllAuth{}, engine)
	postJSON(t, server.handleCreateVectorBucket, &CreateVectorBucketRequest{VectorBucketName: "vec-bucket"})
	request := &CreateIndexRequest{VectorBucketName: "vec-bucket", IndexName: "idx", DataType: "float32", Dimension: 3, DistanceMetric: "cosine"}
	engine.onCreate = func() {
		engine.onCreate = nil
		engine.err = nil
		if result := postJSON(t, server.handleCreateIndex, request); result.Code != http.StatusOK {
			t.Fatalf("concurrent retry: %d %s", result.Code, result.Body)
		}
	}
	if result := postJSON(t, server.handleCreateIndex, request); result.Code == http.StatusOK {
		t.Fatal("first attempt reported success")
	}
	if _, err := server.indexRecord(t.Context(), "vec-bucket", "idx"); err != nil {
		t.Fatalf("rollback removed the completed retry: %v", err)
	}
	if result := postJSON(t, server.handleCreateIndex, request); result.Code != http.StatusConflict {
		t.Fatalf("completed retry lost its state: %d %s", result.Code, result.Body)
	}
}

func TestCreateIndexRetainsAnUnknownOutcomeAfterARefusedRetry(t *testing.T) {
	for _, code := range []codes.Code{codes.PermissionDenied, codes.FailedPrecondition} {
		t.Run(code.String(), func(t *testing.T) {
			fs := s3tablestest.Start(t)
			first := &failingCreateVectors{fakeVectors: &fakeVectors{}, err: status.Error(codes.Unavailable, "response lost after commit")}
			second := &failingCreateVectors{fakeVectors: &fakeVectors{}, err: status.Error(code, "retry refused")}
			discovery := &vectorDiscovery{client: first, nextResolve: time.Now().Add(time.Hour)}
			discovery.discover = func([]pb.ServerAddress, grpc.DialOption) []string {
				discovery.mu.Lock()
				discovery.client = second
				discovery.mu.Unlock()
				return nil
			}
			defer discovery.Close()
			server := NewServer(s3tables.NewManagerClient(fs.Client), allowAllAuth{}, discovery)
			postJSON(t, server.handleCreateVectorBucket, &CreateVectorBucketRequest{VectorBucketName: "vec-bucket"})
			result := postJSON(t, server.handleCreateIndex, &CreateIndexRequest{VectorBucketName: "vec-bucket", IndexName: "idx", DataType: "float32", Dimension: 3, DistanceMetric: "cosine"})
			if result.Code != http.StatusServiceUnavailable || len(first.created) != 1 || len(second.created) != 1 {
				t.Fatalf("unknown outcome lost: %d %s (attempts %d, %d)", result.Code, result.Body, len(first.created), len(second.created))
			}
			if _, err := server.indexRecord(t.Context(), "vec-bucket", "idx"); err != nil {
				t.Fatalf("refused retry erased a possibly committed dataset: %v", err)
			}
		})
	}
}

type concurrentCreateVectors struct {
	vector_pb.VectorServiceClient
	create func() (*vector_pb.CreateIndexResponse, error)
}

func (f *concurrentCreateVectors) CreateIndex(context.Context, *vector_pb.CreateIndexRequest, ...grpc.CallOption) (*vector_pb.CreateIndexResponse, error) {
	return f.create()
}

func TestCreateIndexRollbackPreservesAnActiveRetry(t *testing.T) {
	fs := s3tablestest.Start(t)
	firstStarted, retryStarted, finishRetry := make(chan struct{}), make(chan struct{}), make(chan struct{})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var calls atomic.Int32
	engine := &concurrentCreateVectors{create: func() (*vector_pb.CreateIndexResponse, error) {
		if calls.Add(1) == 1 {
			close(firstStarted)
			select {
			case <-retryStarted:
				return nil, status.Error(codes.InvalidArgument, "first attempt refused")
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		close(retryStarted)
		select {
		case <-finishRetry:
			return &vector_pb.CreateIndexResponse{}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}}
	server := NewServer(s3tables.NewManagerClient(fs.Client), allowAllAuth{}, engine)
	postJSON(t, server.handleCreateVectorBucket, &CreateVectorBucketRequest{VectorBucketName: "vec-bucket"})
	request := &CreateIndexRequest{VectorBucketName: "vec-bucket", IndexName: "idx", DataType: "float32", Dimension: 3, DistanceMetric: "cosine"}
	first, retry := make(chan *httptest.ResponseRecorder, 1), make(chan *httptest.ResponseRecorder, 1)
	go func() { first <- postJSON(t, server.handleCreateIndex, request) }()
	select {
	case <-firstStarted:
	case <-ctx.Done():
		t.Fatal("first create did not reach the engine")
	}
	go func() { retry <- postJSON(t, server.handleCreateIndex, request) }()
	if result := <-first; result.Code == http.StatusOK {
		t.Fatal("first attempt reported success")
	}
	if _, err := server.indexRecord(t.Context(), "vec-bucket", "idx"); err != nil {
		t.Fatalf("rollback removed an active retry: %v", err)
	}
	close(finishRetry)
	if result := <-retry; result.Code != http.StatusOK {
		t.Fatalf("retry could not finish: %d %s", result.Code, result.Body)
	}
}

func TestCreateIndexResumesWithTheDefaultMetric(t *testing.T) {
	fs := s3tablestest.Start(t)
	engine := &failingCreateVectors{fakeVectors: &fakeVectors{}, err: status.Error(codes.Unavailable, "request lost")}
	server := NewServer(s3tables.NewManagerClient(fs.Client), allowAllAuth{}, engine)
	postJSON(t, server.handleCreateVectorBucket, &CreateVectorBucketRequest{VectorBucketName: "vec-bucket"})
	request := &CreateIndexRequest{VectorBucketName: "vec-bucket", IndexName: "idx", DataType: "float32", Dimension: 3}
	postJSON(t, server.handleCreateIndex, request)
	engine.err = nil
	if result := postJSON(t, server.handleCreateIndex, request); result.Code != http.StatusOK {
		t.Fatalf("default-metric recovery: %d %s", result.Code, result.Body)
	}
	if len(engine.created) != 2 || engine.created[1].DistanceMetric != vector_pb.DistanceMetric_L2 {
		t.Fatal("recovery changed the default metric")
	}
}

func TestCreateIndexRetryPreservesDuplicateMetadataKeys(t *testing.T) {
	fs := s3tablestest.Start(t)
	engine := &failingCreateVectors{fakeVectors: &fakeVectors{}, err: status.Error(codes.Unavailable, "response lost")}
	server := NewServer(s3tables.NewManagerClient(fs.Client), allowAllAuth{}, engine)
	postJSON(t, server.handleCreateVectorBucket, &CreateVectorBucketRequest{VectorBucketName: "vec-bucket"})
	request := &CreateIndexRequest{VectorBucketName: "vec-bucket", IndexName: "idx", DataType: "float32", Dimension: 3, DistanceMetric: "cosine",
		MetadataConfiguration: &MetadataConfiguration{NonFilterableMetadataKeys: []string{"b", "a", "a"}}}
	postJSON(t, server.handleCreateIndex, request)
	engine.err = nil
	if result := postJSON(t, server.handleCreateIndex, request); result.Code != http.StatusOK {
		t.Fatalf("duplicate-key recovery: %d %s", result.Code, result.Body)
	}
	if len(engine.created) != 2 {
		t.Fatalf("engine calls: %d", len(engine.created))
	}
	for _, call := range engine.created {
		keys := slices.Clone(call.NonFilterableMetadataKeys)
		slices.Sort(keys)
		if !slices.Equal(keys, []string{"a", "a", "b"}) {
			t.Fatalf("initialization changed metadata keys: %q", keys)
		}
	}
}

func TestCreateIndexDoesNotInitializeAConcurrentCatalogCreate(t *testing.T) {
	fs := s3tablestest.Start(t)
	engine := &fakeVectors{}
	server := NewServer(s3tables.NewManagerClient(fs.Client), allowAllAuth{}, engine)
	postJSON(t, server.handleCreateVectorBucket, &CreateVectorBucketRequest{VectorBucketName: "vec-bucket"})
	postJSON(t, server.handleCreateIndex, &CreateIndexRequest{VectorBucketName: "vec-bucket", IndexName: "seed", DataType: "float32", Dimension: 7, DistanceMetric: "cosine"})
	dir, name := splitEntryPath(s3tables.GetTablePath("vec-bucket", indexNamespace, "idx"))
	seed := fs.Get(dir, "seed")
	var lookups int
	fs.OnLookup = func(d, n string) {
		if d == dir && n == name {
			lookups++
			if lookups == 2 {
				fs.Put(dir, name, seed.Extended)
			}
		}
	}
	result := postJSON(t, server.handleCreateIndex, &CreateIndexRequest{VectorBucketName: "vec-bucket", IndexName: "idx", DataType: "float32", Dimension: 3, DistanceMetric: "cosine"})
	if result.Code != http.StatusConflict || len(engine.created) != 1 {
		t.Fatalf("initialized a competing record: %d %s (%d engine calls)", result.Code, result.Body, len(engine.created))
	}
	record, err := server.indexRecord(t.Context(), "vec-bucket", "idx")
	if err != nil || indexFromRecord(server, "vec-bucket", "idx", record).Dimension != 7 {
		t.Fatalf("changed the competing record: %v", err)
	}
}
