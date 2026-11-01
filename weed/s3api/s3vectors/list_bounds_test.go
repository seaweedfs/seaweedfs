package s3vectors

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
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

// A segmented listing comes from the engine in one call per page; an engine
// that ignores segment_count falls back to paging here. Both cover every row
// once, in the same segments.
func TestListVectorsSegmentsFromTheEngine(t *testing.T) {
	fs := s3tablestest.Start(t)
	modern := &countingVectors{fakeVectors: newFakeVectors(2503)}
	legacy := &countingVectors{fakeVectors: newFakeVectors(2503)}
	legacy.legacy = true
	segments := map[bool][]string{}
	for _, engine := range []*countingVectors{modern, legacy} {
		s := NewServer(s3tables.NewManagerClient(fs.Client), allowAllAuth{}, engine)
		bucket := "vec-modern"
		if engine.legacy {
			bucket = "vec-legacy"
		}
		if w := postJSON(t, s.handleCreateVectorBucket, &CreateVectorBucketRequest{VectorBucketName: bucket}); w.Code != http.StatusOK {
			t.Fatalf("create bucket: %d %s", w.Code, w.Body.String())
		}
		if w := postJSON(t, s.handleCreateIndex, &CreateIndexRequest{
			VectorBucketName: bucket, IndexName: "idx", DataType: "float32", Dimension: 3, DistanceMetric: "euclidean",
		}); w.Code != http.StatusOK {
			t.Fatalf("create index: %d %s", w.Code, w.Body.String())
		}
		engine.lists = 0
		seen, pages := map[string]int{}, 0
		for index := 0; index < 3; index++ {
			token := ""
			for {
				fields := `,"segmentCount":3,"segmentIndex":` + strconv.Itoa(index) + `,"maxResults":100`
				if token != "" {
					fields += `,"nextToken":"` + token + `"`
				}
				body := json.RawMessage(`{"vectorBucketName":"` + bucket + `","indexName":"idx"` + fields + `}`)
				w := postJSON(t, s.handleListVectors, body)
				if w.Code != http.StatusOK {
					t.Fatalf("legacy=%v: %d %s", engine.legacy, w.Code, w.Body.String())
				}
				var out ListVectorsResponse
				if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
					t.Fatal(err)
				}
				pages++
				for _, v := range out.Vectors {
					seen[v.Key]++
					segments[engine.legacy] = append(segments[engine.legacy], strconv.Itoa(index)+"/"+v.Key)
				}
				if out.NextToken == "" {
					break
				}
				token = out.NextToken
			}
		}
		if len(seen) != len(engine.rows) {
			t.Fatalf("legacy=%v: saw %d distinct rows, want %d", engine.legacy, len(seen), len(engine.rows))
		}
		for key, n := range seen {
			if n != 1 {
				t.Fatalf("legacy=%v: row %s seen %d times", engine.legacy, key, n)
			}
		}
		if !engine.legacy && engine.lists != pages {
			t.Fatalf("modern engine took %d calls for %d pages", engine.lists, pages)
		}
		// The fallback starts from the page the engine already returned.
		engine.lists = 0
		body := json.RawMessage(`{"vectorBucketName":"` + bucket + `","indexName":"idx","segmentCount":2,"segmentIndex":0,"maxResults":1}`)
		w := postJSON(t, s.handleListVectors, body)
		var out ListVectorsResponse
		if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil || len(out.Vectors) != 1 || out.Vectors[0].Key != "k0" || out.NextToken != "1" {
			t.Fatalf("legacy=%v: one-row segment page %d %s", engine.legacy, w.Code, w.Body.String())
		}
		if engine.lists != 1 {
			t.Fatalf("legacy=%v: one-row segment page took %d engine calls", engine.legacy, engine.lists)
		}
	}
	if strings.Join(segments[false], ",") != strings.Join(segments[true], ",") {
		t.Fatal("legacy fallback and engine segments disagree")
	}
}
