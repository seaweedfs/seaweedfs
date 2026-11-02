package s3vectors

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/vector_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"
	"google.golang.org/grpc"
)

type queryVectorsFake struct{ *fakeVectors }

func (queryVectorsFake) QueryVectors(ctx context.Context, req *vector_pb.QueryVectorsRequest, opts ...grpc.CallOption) (*vector_pb.QueryVectorsResponse, error) {
	return &vector_pb.QueryVectorsResponse{}, nil
}

// A metric sent in any case is stored, and returned by GetIndex and
// QueryVectors, as the lowercase AWS enum value.
func TestCreateIndexStoresLowercaseMetric(t *testing.T) {
	fs := s3tablestest.Start(t)
	engine := queryVectorsFake{newFakeVectors(0)}
	s := NewServer(s3tables.NewManagerClient(fs.Client), allowAllAuth{}, engine)
	if w := postJSON(t, s.handleCreateVectorBucket, &CreateVectorBucketRequest{VectorBucketName: "vec-bucket"}); w.Code != http.StatusOK {
		t.Fatalf("create bucket: %d %s", w.Code, w.Body.String())
	}
	index := &GetIndexRequest{VectorBucketName: "vec-bucket", IndexName: "idx"}
	if w := postJSON(t, s.handleCreateIndex, &CreateIndexRequest{
		VectorBucketName: index.VectorBucketName,
		IndexName:        index.IndexName,
		DataType:         "float32",
		Dimension:        2,
		DistanceMetric:   "COSINE",
	}); w.Code != http.StatusOK {
		t.Fatalf("create index: %d %s", w.Code, w.Body.String())
	}
	if len(engine.created) != 1 || engine.created[0].DistanceMetric != vector_pb.DistanceMetric_COSINE {
		t.Fatalf("engine CreateIndex calls = %v, want one cosine index", engine.created)
	}

	record, err := s.indexRecord(context.Background(), index.VectorBucketName, index.IndexName)
	if err != nil {
		t.Fatal(err)
	}
	if got := record["metadata"].(map[string]interface{})["fullMetadata"].(map[string]interface{})["distanceMetric"]; got != "cosine" {
		t.Fatalf("stored distanceMetric %v, want cosine", got)
	}

	w := postJSON(t, s.handleGetIndex, index)
	var got GetIndexResponse
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil || got.Index == nil || got.Index.DistanceMetric != "cosine" {
		t.Fatalf("GetIndex: %d %s", w.Code, w.Body.String())
	}

	w = postJSON(t, s.handleQueryVectors, &QueryVectorsRequest{
		VectorBucketName: index.VectorBucketName,
		IndexName:        index.IndexName,
		QueryVector:      &QueryVector{Float32: []float32{1, 0}},
		TopK:             1,
	})
	var query QueryVectorsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &query); err != nil || query.DistanceMetric != "cosine" {
		t.Fatalf("QueryVectors: %d %s", w.Code, w.Body.String())
	}
}

// Records stored before the metric was normalized render in lowercase too.
func TestIndexFromRecordLowercasesMetric(t *testing.T) {
	s := &Server{}
	for stored, want := range map[interface{}]string{
		"COSINE":    "cosine",
		"Euclidean": "euclidean",
		"":          "euclidean",
		nil:         "euclidean",
	} {
		record := map[string]interface{}{
			"metadata": map[string]interface{}{
				"fullMetadata": map[string]interface{}{"dimension": "2", "distanceMetric": stored},
			},
		}
		if got := indexFromRecord(s, "b", "i", record).DistanceMetric; got != want {
			t.Fatalf("stored %v: distanceMetric %q, want %q", stored, got, want)
		}
	}
}

type distancesFake struct {
	*fakeVectors
	distances []float32
}

func (f distancesFake) QueryVectors(ctx context.Context, req *vector_pb.QueryVectorsRequest, opts ...grpc.CallOption) (*vector_pb.QueryVectorsResponse, error) {
	resp := &vector_pb.QueryVectorsResponse{}
	for i, d := range f.distances {
		resp.Vectors = append(resp.Vectors, &vector_pb.QueryResult{Key: fmt.Sprint(i), Distance: d})
	}
	return resp, nil
}

// The engine returns Lance's squared L2 distance; a euclidean index reports
// its square root, the other metrics their engine value.
func TestQueryVectorsReportsEuclideanDistance(t *testing.T) {
	for metric, want := range map[string][]float32{
		"euclidean":  {5, 0.5, 0},
		"cosine":     {25, 0.25, -1e-7},
		"dotproduct": {25, 0.25, -1e-7},
	} {
		fs := s3tablestest.Start(t)
		engine := distancesFake{newFakeVectors(0), []float32{25, 0.25, -1e-7}}
		s := NewServer(s3tables.NewManagerClient(fs.Client), allowAllAuth{}, engine)
		if w := postJSON(t, s.handleCreateVectorBucket, &CreateVectorBucketRequest{VectorBucketName: "vec-bucket"}); w.Code != http.StatusOK {
			t.Fatalf("create bucket: %d %s", w.Code, w.Body.String())
		}
		if w := postJSON(t, s.handleCreateIndex, &CreateIndexRequest{
			VectorBucketName: "vec-bucket", IndexName: "idx", DataType: "float32", Dimension: 2, DistanceMetric: metric,
		}); w.Code != http.StatusOK {
			t.Fatalf("create index: %d %s", w.Code, w.Body.String())
		}
		w := postJSON(t, s.handleQueryVectors, &QueryVectorsRequest{
			VectorBucketName: "vec-bucket", IndexName: "idx",
			QueryVector: &QueryVector{Float32: []float32{1, 0}}, TopK: 3, ReturnDistance: true,
		})
		var query QueryVectorsResponse
		if err := json.Unmarshal(w.Body.Bytes(), &query); err != nil || len(query.Vectors) != len(want) {
			t.Fatalf("%s QueryVectors: %d %s", metric, w.Code, w.Body.String())
		}
		for i, v := range query.Vectors {
			if v.Distance == nil {
				t.Fatalf("%s hit %d has no distance", metric, i)
			}
			if *v.Distance != want[i] {
				t.Fatalf("%s hit %d distance %v, want %v", metric, i, *v.Distance, want[i])
			}
		}
	}
}
