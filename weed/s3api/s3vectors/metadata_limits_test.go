package s3vectors

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"
)

func vectorWithMetadata(key string, metadata map[string]interface{}) *PutInputVector {
	return &PutInputVector{Key: key, Data: &VectorData{Float32: []float32{1, 2, 3}}, Metadata: metadata}
}

func TestToPutVectorMetadataLimits(t *testing.T) {
	cfg := &MetadataConfiguration{NonFilterableMetadataKeys: []string{"body"}}
	manyKeys := func(n int) map[string]interface{} {
		m := make(map[string]interface{}, n)
		for i := range n {
			m[fmt.Sprintf("k%d", i)] = i
		}
		return m
	}
	for _, tc := range []struct {
		name string
		v    *PutInputVector
		ok   bool
	}{
		{"no metadata", vectorWithMetadata("a", nil), true},
		{"key at limit", vectorWithMetadata(strings.Repeat("k", vectorKeyMaxLength), nil), true},
		{"key over limit", vectorWithMetadata(strings.Repeat("k", vectorKeyMaxLength+1), nil), false},
		{"multibyte key at limit", vectorWithMetadata(strings.Repeat("é", vectorKeyMaxLength), nil), true},
		{"keys at limit", vectorWithMetadata("a", manyKeys(vectorMetadataMaxKeys)), true},
		{"keys over limit", vectorWithMetadata("a", manyKeys(vectorMetadataMaxKeys+1)), false},
		{"small filterable", vectorWithMetadata("a", map[string]interface{}{"genre": "jazz"}), true},
		{"filterable over 2KB", vectorWithMetadata("a", map[string]interface{}{"genre": strings.Repeat("x", vectorFilterableMetadataMaxBytes)}), false},
		{"non-filterable over 2KB", vectorWithMetadata("a", map[string]interface{}{"body": strings.Repeat("x", 30*1024)}), true},
		{"non-filterable over the CLASSIC 40KB", vectorWithMetadata("a", map[string]interface{}{"body": strings.Repeat("x", 60*1024)}), true},
		{"total over 100KB", vectorWithMetadata("a", map[string]interface{}{"body": strings.Repeat("x", vectorMetadataMaxBytes)}), false},
		{"total over 100KB across keys", vectorWithMetadata("a", map[string]interface{}{
			"body": strings.Repeat("x", 60*1024), "genre": "jazz", "k2": strings.Repeat("y", 1024),
			"k3": strings.Repeat("z", 45*1024),
		}), false},
		{"nested filterable counted", vectorWithMetadata("a", map[string]interface{}{
			"tags": []interface{}{strings.Repeat("x", 1500), strings.Repeat("y", 1500)},
		}), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := toPutVector(tc.v, 3, cfg); (err == nil) != tc.ok {
				t.Fatalf("toPutVector err = %v, want ok %v", err, tc.ok)
			}
		})
	}
}

// The filterable limit applies to every key when the index declares no
// non-filterable keys.
func TestToPutVectorFilterableWithoutConfiguration(t *testing.T) {
	v := vectorWithMetadata("a", map[string]interface{}{"body": strings.Repeat("x", 4096)})
	if _, err := toPutVector(v, 3, nil); err == nil {
		t.Fatal("4KB metadata on an index without non-filterable keys was accepted")
	}
}

func TestValidateMetadataConfiguration(t *testing.T) {
	keys := func(n int) []string {
		out := make([]string, n)
		for i := range out {
			out[i] = fmt.Sprintf("k%d", i)
		}
		return out
	}
	for _, tc := range []struct {
		name string
		cfg  *MetadataConfiguration
		ok   bool
	}{
		{"nil", nil, true},
		{"ten keys", &MetadataConfiguration{NonFilterableMetadataKeys: keys(indexNonFilterableMaxKeys)}, true},
		{"eleven keys", &MetadataConfiguration{NonFilterableMetadataKeys: keys(indexNonFilterableMaxKeys + 1)}, false},
		{"empty key", &MetadataConfiguration{NonFilterableMetadataKeys: []string{""}}, false},
		{"long key", &MetadataConfiguration{NonFilterableMetadataKeys: []string{strings.Repeat("k", nonFilterableKeyMaxLength+1)}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateMetadataConfiguration(tc.cfg); (err == nil) != tc.ok {
				t.Fatalf("err = %v, want ok %v", err, tc.ok)
			}
		})
	}
}

// End to end through the handlers: the index's own non-filterable keys
// decide what counts toward the filterable limit, and a vector over a limit
// is refused with a ValidationException before the engine sees the batch.
func TestPutVectorsEnforcesMetadataLimits(t *testing.T) {
	fs := s3tablestest.Start(t)
	engine := &fakeVectors{}
	s := NewServer(s3tables.NewManagerClient(fs.Client), allowAllAuth{}, engine)
	if w := postJSON(t, s.handleCreateVectorBucket, &CreateVectorBucketRequest{VectorBucketName: "vec-bucket"}); w.Code != http.StatusOK {
		t.Fatalf("create bucket: %d %s", w.Code, w.Body.String())
	}
	tooMany := make([]string, indexNonFilterableMaxKeys+1)
	for i := range tooMany {
		tooMany[i] = fmt.Sprintf("k%d", i)
	}
	if w := postJSON(t, s.handleCreateIndex, &CreateIndexRequest{
		VectorBucketName: "vec-bucket", IndexName: "bad", DataType: "float32", Dimension: 3, DistanceMetric: "euclidean",
		MetadataConfiguration: &MetadataConfiguration{NonFilterableMetadataKeys: tooMany},
	}); w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "ValidationException") {
		t.Fatalf("create index with %d non-filterable keys: %d %s", len(tooMany), w.Code, w.Body.String())
	}
	if len(engine.created) != 0 {
		t.Fatalf("rejected index reached the engine: %v", engine.created)
	}
	if w := postJSON(t, s.handleCreateIndex, &CreateIndexRequest{
		VectorBucketName: "vec-bucket", IndexName: "idx", DataType: "float32", Dimension: 3, DistanceMetric: "euclidean",
		MetadataConfiguration: &MetadataConfiguration{NonFilterableMetadataKeys: []string{"body"}},
	}); w.Code != http.StatusOK {
		t.Fatalf("create index: %d %s", w.Code, w.Body.String())
	}
	put := func(v *PutInputVector) *http.Response {
		w := postJSON(t, s.handlePutVectors, &PutVectorsRequest{VectorBucketName: "vec-bucket", IndexName: "idx", Vectors: []*PutInputVector{v}})
		return w.Result()
	}
	if resp := put(vectorWithMetadata("ok", map[string]interface{}{"body": strings.Repeat("x", 20*1024)})); resp.StatusCode != http.StatusOK {
		t.Fatalf("large non-filterable metadata: %d", resp.StatusCode)
	}
	if resp := put(vectorWithMetadata("big", map[string]interface{}{"genre": strings.Repeat("x", 4096)})); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("4KB filterable metadata: %d, want 400", resp.StatusCode)
	}
	if len(engine.puts) != 1 {
		t.Fatalf("engine PutVectors calls = %d, want only the accepted vector", len(engine.puts))
	}
}

func TestFilterConstraints(t *testing.T) {
	for _, tc := range []struct {
		name   string
		filter map[string]interface{}
		want   int
	}{
		{"none", nil, 0},
		{"implicit equality", map[string]interface{}{"genre": "jazz"}, 1},
		{"operator object", map[string]interface{}{"year": map[string]interface{}{"$gte": 1990, "$lt": 2000}}, 2},
		{"nested logic", map[string]interface{}{"$and": []interface{}{
			map[string]interface{}{"genre": "jazz"},
			map[string]interface{}{"$or": []interface{}{
				map[string]interface{}{"year": map[string]interface{}{"$eq": 1990}},
				map[string]interface{}{"label": map[string]interface{}{"$in": []interface{}{"a", "b"}}},
			}},
		}}, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := filterConstraints(tc.filter); got != tc.want {
				t.Fatalf("filterConstraints = %d, want %d", got, tc.want)
			}
		})
	}
}

func orFilter(n int) map[string]interface{} {
	terms := make([]interface{}, n)
	for i := range terms {
		terms[i] = map[string]interface{}{fmt.Sprintf("k%d", i): i}
	}
	return map[string]interface{}{"$or": terms}
}

// A filter over the limit is refused before the catalog or the engine (the
// errFiler fails any catalog access); one at the limit gets past the gate.
func TestQueryVectorsFilterConstraintLimit(t *testing.T) {
	s := &Server{
		vectors:     newFakeVectors(0),
		tables:      s3tables.NewManager(),
		internal:    s3tables.NewManager(),
		filerClient: errFiler{},
	}
	query := func(filter map[string]interface{}) *httptest.ResponseRecorder {
		return postJSON(t, s.handleQueryVectors, &QueryVectorsRequest{
			VectorBucketName: "vec-bucket", IndexName: "idx", TopK: 1,
			QueryVector: &QueryVector{Float32: []float32{1, 2, 3}}, Filter: filter,
		})
	}
	w := query(orFilter(queryFilterMaxConstraints + 1))
	if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "ValidationException") ||
		!strings.Contains(w.Body.String(), "constraints") {
		t.Fatalf("%d constraints: %d %s, want 400 ValidationException", queryFilterMaxConstraints+1, w.Code, w.Body.String())
	}
	if w := query(orFilter(queryFilterMaxConstraints)); strings.Contains(w.Body.String(), "constraints") {
		t.Fatalf("%d constraints refused: %d %s", queryFilterMaxConstraints, w.Code, w.Body.String())
	}
}

func TestCheckVectorMetadataAcceptsExactLimits(t *testing.T) {
	filterable := map[string]interface{}{"genre": strings.Repeat("x", vectorFilterableMetadataMaxBytes-12)}
	if encoded, _ := json.Marshal(filterable); len(encoded) != vectorFilterableMetadataMaxBytes {
		t.Fatalf("filterable metadata encodes to %d bytes, want %d", len(encoded), vectorFilterableMetadataMaxBytes)
	}
	if err := checkVectorMetadata("a", filterable, nil); err != nil {
		t.Fatalf("filterable metadata of exactly %d bytes: %v", vectorFilterableMetadataMaxBytes, err)
	}
	total := map[string]interface{}{"genre": "x", "body": strings.Repeat("x", vectorMetadataMaxBytes-23)}
	if encoded, _ := json.Marshal(total); len(encoded) != vectorMetadataMaxBytes {
		t.Fatalf("metadata encodes to %d bytes, want %d", len(encoded), vectorMetadataMaxBytes)
	}
	if err := checkVectorMetadata("a", total, &MetadataConfiguration{NonFilterableMetadataKeys: []string{"body"}}); err != nil {
		t.Fatalf("metadata of exactly %d bytes: %v", vectorMetadataMaxBytes, err)
	}
}
