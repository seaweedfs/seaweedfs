package s3vectors

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/vector_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"
	"google.golang.org/grpc"
)

// countingVectors counts engine ListVectors calls.
type countingVectors struct {
	*fakeVectors
	lists int
}

func (c *countingVectors) ListVectors(ctx context.Context, req *vector_pb.ListVectorsRequest, opts ...grpc.CallOption) (*vector_pb.ListVectorsResponse, error) {
	c.lists++
	return c.fakeVectors.ListVectors(ctx, req, opts...)
}

func listVectorsBody(fields string) json.RawMessage {
	return json.RawMessage(`{"vectorBucketName":"vec-bucket","indexName":"idx"` + fields + `}`)
}

// Out-of-range values are rejected before authorization (the server has no
// catalog) and never reach the engine.
func TestListVectorsBoundsRejectBeforeEngine(t *testing.T) {
	engine := &countingVectors{fakeVectors: newFakeVectors(50000)}
	s := &Server{vectors: engine}
	for name, fields := range map[string]string{
		"maxResults zero":               `,"maxResults":0`,
		"maxResults negative":           `,"maxResults":-1`,
		"maxResults over":               `,"maxResults":1001`,
		"segmentCount zero":             `,"segmentCount":0,"segmentIndex":0`,
		"segmentCount over":             `,"segmentCount":17,"segmentIndex":0`,
		"segmentCount huge":             `,"segmentCount":1000000,"segmentIndex":999999,"maxResults":1`,
		"segmentIndex negative":         `,"segmentCount":4,"segmentIndex":-1`,
		"segmentIndex at count":         `,"segmentCount":4,"segmentIndex":4`,
		"segmentIndex without count":    `,"segmentIndex":0`,
		"segmentCount without index":    `,"segmentCount":4`,
		"segment ok, maxResults over":   `,"segmentCount":4,"segmentIndex":0,"maxResults":1001`,
		"segment ok, maxResults zeroed": `,"segmentCount":4,"segmentIndex":0,"maxResults":0`,
	} {
		w := postJSON(t, s.handleListVectors, listVectorsBody(fields))
		if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "ValidationException") {
			t.Fatalf("%s: %d %s, want 400 ValidationException", name, w.Code, w.Body.String())
		}
	}
	if engine.lists != 0 {
		t.Fatalf("engine saw %d ListVectors calls, want none", engine.lists)
	}
}

// In-range values list normally; a segmented page at the top of the range
// costs one engine page, not a scan of the index.
func TestListVectorsBoundsAccepted(t *testing.T) {
	fs := s3tablestest.Start(t)
	engine := &countingVectors{fakeVectors: newFakeVectors(50000)}
	s := NewServer(s3tables.NewManagerClient(fs.Client), allowAllAuth{}, engine)
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
	for _, tc := range []struct {
		fields string
		rows   int
	}{
		{"", vectorsDefaultPage},
		{`,"maxResults":1`, 1},
		{`,"maxResults":1000`, vectorsMaxPage},
		{`,"segmentCount":1,"segmentIndex":0,"maxResults":1000`, vectorsMaxPage},
		{`,"segmentCount":16,"segmentIndex":15,"maxResults":1`, 1},
	} {
		engine.lists = 0
		w := postJSON(t, s.handleListVectors, listVectorsBody(tc.fields))
		if w.Code != http.StatusOK {
			t.Fatalf("%q: %d %s", tc.fields, w.Code, w.Body.String())
		}
		var out ListVectorsResponse
		if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
			t.Fatalf("%q: decode: %v", tc.fields, err)
		}
		if len(out.Vectors) != tc.rows || engine.lists != 1 {
			t.Fatalf("%q: %d rows in %d engine calls, want %d in 1", tc.fields, len(out.Vectors), engine.lists, tc.rows)
		}
	}
}
