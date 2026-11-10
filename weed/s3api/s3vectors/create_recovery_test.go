package s3vectors

import (
	"context"
	"net/http"
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
	err error
}

func (f *failingCreateVectors) CreateIndex(_ context.Context, request *vector_pb.CreateIndexRequest, _ ...grpc.CallOption) (*vector_pb.CreateIndexResponse, error) {
	f.created = append(f.created, request)
	return nil, f.err
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
