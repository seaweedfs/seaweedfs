package s3vectors

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/vector_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables/s3tablestest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fakeVectors serves a fixed row set with the engine's offset-token paging
// and records the mutating calls a handler test asserts on.
type fakeVectors struct {
	vector_pb.VectorServiceClient
	// legacy ignores segment_count, like an engine that predates it.
	legacy  bool
	rows    []*vector_pb.Vector
	created []*vector_pb.CreateIndexRequest
	puts    []*vector_pb.PutVectorsRequest
	gets    []*vector_pb.GetVectorsRequest
	deletes []*vector_pb.DeleteVectorsRequest
}

func newFakeVectors(n int) *fakeVectors {
	f := &fakeVectors{}
	for i := 0; i < n; i++ {
		f.rows = append(f.rows, &vector_pb.Vector{Key: fmt.Sprintf("k%d", i)})
	}
	return f
}

func (f *fakeVectors) ListVectors(ctx context.Context, req *vector_pb.ListVectorsRequest, opts ...grpc.CallOption) (*vector_pb.ListVectorsResponse, error) {
	offset := 0
	if req.NextToken != "" {
		n, err := strconv.Atoi(req.NextToken)
		if err != nil {
			return nil, fmt.Errorf("invalid next_token %q", req.NextToken)
		}
		offset = n
	}
	page := int(req.MaxResults)
	if page <= 0 {
		page = vectorsDefaultPage
	}
	if page > vectorsMaxPage {
		page = vectorsMaxPage
	}
	if offset > len(f.rows) {
		offset = len(f.rows)
	}
	if req.SegmentCount > 0 && !f.legacy {
		count, index := int(req.SegmentCount), int(req.SegmentIndex)
		resp := &vector_pb.ListVectorsResponse{Segmented: true}
		pos := offset + (index+count-offset%count)%count
		for ; pos < len(f.rows) && len(resp.Vectors) < page; pos += count {
			resp.Vectors = append(resp.Vectors, f.rows[pos])
		}
		if last := pos - count; len(resp.Vectors) == page && last+1 < len(f.rows) {
			resp.NextToken = strconv.Itoa(last + 1)
		}
		return resp, nil
	}
	end := offset + page
	if end > len(f.rows) {
		end = len(f.rows)
	}
	resp := &vector_pb.ListVectorsResponse{Vectors: f.rows[offset:end]}
	if rows := end - offset; rows == page && rows > 0 {
		resp.NextToken = strconv.Itoa(end)
	}
	return resp, nil
}

func (f *fakeVectors) CreateIndex(ctx context.Context, req *vector_pb.CreateIndexRequest, opts ...grpc.CallOption) (*vector_pb.CreateIndexResponse, error) {
	f.created = append(f.created, req)
	return &vector_pb.CreateIndexResponse{}, nil
}

func (f *fakeVectors) GetVectors(ctx context.Context, req *vector_pb.GetVectorsRequest, opts ...grpc.CallOption) (*vector_pb.GetVectorsResponse, error) {
	f.gets = append(f.gets, req)
	return &vector_pb.GetVectorsResponse{}, nil
}

func (f *fakeVectors) DeleteVectors(ctx context.Context, req *vector_pb.DeleteVectorsRequest, opts ...grpc.CallOption) (*vector_pb.DeleteVectorsResponse, error) {
	f.deletes = append(f.deletes, req)
	return &vector_pb.DeleteVectorsResponse{}, nil
}

func (f *fakeVectors) PutVectors(ctx context.Context, req *vector_pb.PutVectorsRequest, opts ...grpc.CallOption) (*vector_pb.PutVectorsResponse, error) {
	f.puts = append(f.puts, req)
	return &vector_pb.PutVectorsResponse{}, nil
}

// Following every segment's tokens must visit each vector exactly once.
func TestListSegmentVectorsCoversEveryRowOnce(t *testing.T) {
	f := newFakeVectors(2500)
	s := &Server{vectors: f}
	seen := map[string]int{}
	for segmentIndex := uint64(0); segmentIndex < 3; segmentIndex++ {
		token := ""
		for {
			offset, err := strconv.ParseUint(token, 10, 64)
			if token == "" {
				offset = 0
			} else if err != nil {
				t.Fatalf("bad token %q: %v", token, err)
			}
			out, next, err := s.listSegmentVectors(context.Background(), nil, offset, 7, 3, segmentIndex, false, false, nil)
			if err != nil {
				t.Fatal(err)
			}
			for _, v := range out {
				seen[v.Key]++
			}
			if next == "" {
				break
			}
			token = next
		}
	}
	if len(seen) != len(f.rows) {
		t.Fatalf("saw %d distinct rows, want %d", len(seen), len(f.rows))
	}
	for k, count := range seen {
		if count != 1 {
			t.Fatalf("row %s seen %d times", k, count)
		}
	}
}

// A limit reached mid-page must resume from the row after the last match, not
// the next engine page.
func TestListSegmentVectorsStopsInsideEnginePage(t *testing.T) {
	f := newFakeVectors(100)
	s := &Server{vectors: f}
	out, next, err := s.listSegmentVectors(context.Background(), nil, 0, 5, 2, 0, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 5 {
		t.Fatalf("got %d rows, want 5", len(out))
	}
	// Matches are rows 0,2,4,6,8 — resume scanning at row 9.
	if next != "9" {
		t.Fatalf("token %q, want 9", next)
	}
	offset, _ := strconv.ParseUint(next, 10, 64)
	out, next, err = s.listSegmentVectors(context.Background(), nil, offset, 5, 2, 0, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 5 || out[0].Key != "k10" {
		t.Fatalf("resumed at %v rows, first %v", len(out), out[0])
	}
	if next != "19" {
		t.Fatalf("resumed token %q, want 19", next)
	}
}

// The last row of the index filling the page exactly must not emit a token.
func TestListSegmentVectorsEndsOnLastRow(t *testing.T) {
	f := newFakeVectors(10)
	s := &Server{vectors: f}
	out, next, err := s.listSegmentVectors(context.Background(), nil, 0, 10, 1, 0, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 10 {
		t.Fatalf("got %d rows, want 10", len(out))
	}
	if next != "" {
		t.Fatalf("token %q, want empty", next)
	}
}

// maxResults 0 defaults to the engine page; values past the engine cap clamp.
func TestListSegmentVectorsPageLimits(t *testing.T) {
	f := newFakeVectors(1500)
	s := &Server{vectors: f}
	out, next, err := s.listSegmentVectors(context.Background(), nil, 0, 0, 1, 0, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != vectorsDefaultPage || next != strconv.Itoa(vectorsDefaultPage) {
		t.Fatalf("default page: %d rows token %q", len(out), next)
	}
	out, next, err = s.listSegmentVectors(context.Background(), nil, 0, 5000, 1, 0, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != vectorsMaxPage || next != strconv.Itoa(vectorsMaxPage) {
		t.Fatalf("capped page: %d rows token %q", len(out), next)
	}
}

// An explicit maxResults of zero is rejected like any out-of-range value;
// an omitted field still reaches the catalog as 0 and pages by default.
func TestListRequestMaxResultsBounds(t *testing.T) {
	fs := s3tablestest.Start(t)
	s := NewServer(s3tables.NewManagerClient(fs.Client), allowAllAuth{}, newFakeVectors(0))

	zero, over, max := 0, vectorsMaxListPage+1, vectorsMaxListPage
	for name, req := range map[string]*ListVectorBucketsRequest{
		"zero": {MaxResults: &zero},
		"over": {MaxResults: &over},
	} {
		if w := postJSONAs(t, "", s.handleListVectorBuckets, req); w.Code != http.StatusBadRequest {
			t.Fatalf("ListVectorBuckets %s: %d %s, want 400", name, w.Code, w.Body.String())
		}
	}
	for name, req := range map[string]*ListIndexesRequest{
		"zero": {VectorBucketName: "vec-bucket", MaxResults: &zero},
		"over": {VectorBucketName: "vec-bucket", MaxResults: &over},
	} {
		if w := postJSONAs(t, "", s.handleListIndexes, req); w.Code != http.StatusBadRequest {
			t.Fatalf("ListIndexes %s: %d %s, want 400", name, w.Code, w.Body.String())
		}
	}
	if w := postJSONAs(t, "", s.handleListVectorBuckets, &ListVectorBucketsRequest{}); w.Code != http.StatusOK {
		t.Fatalf("ListVectorBuckets omitted maxResults: %d %s", w.Code, w.Body.String())
	}
	if w := postJSONAs(t, "", s.handleListVectorBuckets, &ListVectorBucketsRequest{MaxResults: &max}); w.Code != http.StatusOK {
		t.Fatalf("ListVectorBuckets maxResults=%d: %d %s", max, w.Code, w.Body.String())
	}
	if w := postJSONAs(t, "", s.handleCreateVectorBucket, &CreateVectorBucketRequest{VectorBucketName: "vec-bucket"}); w.Code != http.StatusOK {
		t.Fatalf("create bucket: %d %s", w.Code, w.Body.String())
	}
	if w := postJSONAs(t, "", s.handleCreateIndex, &CreateIndexRequest{
		VectorBucketName: "vec-bucket",
		IndexName:        "idx",
		DataType:         "float32",
		Dimension:        3,
		DistanceMetric:   "euclidean",
	}); w.Code != http.StatusOK {
		t.Fatalf("create index: %d %s", w.Code, w.Body.String())
	}
	if w := postJSONAs(t, "", s.handleListIndexes, &ListIndexesRequest{VectorBucketName: "vec-bucket", MaxResults: &max}); w.Code != http.StatusOK {
		t.Fatalf("ListIndexes maxResults=%d: %d %s", max, w.Code, w.Body.String())
	}
}

func TestArnParts(t *testing.T) {
	s := &Server{region: "r", accountID: "a"}
	var v vectorBucketARNs

	if err := s.arnParts(&v, "plain", ""); err != nil || v.bucket != "plain" {
		t.Fatalf("plain name: bucket=%q err=%v", v.bucket, err)
	}
	if err := s.arnParts(&v, "", "arn:aws:s3vectors:r:a:bucket/mybucket"); err != nil || v.bucket != "mybucket" || v.index != "" {
		t.Fatalf("bucket ARN: %+v err=%v", v, err)
	}
	v = vectorBucketARNs{}
	if err := s.arnParts(&v, "", "arn:aws:s3vectors:r:a:bucket/mybucket/index/myindex"); err != nil || v.bucket != "mybucket" || v.index != "myindex" {
		t.Fatalf("index ARN: %+v err=%v", v, err)
	}
	for _, bad := range []string{
		"arn:aws:s3vectors:r:a:bucket/",
		"arn:aws:s3vectors:r:a:bucket//index/i",
		"arn:aws:s3vectors:r:a:bucket",
		"arn:aws:s3tables:r:a:bucket/b/index/i",
		"arn:aws:s3:r:a:bucket/b/index/i",
		"arn:aws:s3vectors:r:a:prefix/bucket/b/index/i",
		"arn:aws:s3vectors:r:a:bucket/b/index/",
		"arn:aws:s3vectors:r:a:bucket/b/index/i/index/j",
		"arn:aws:s3vectors:r:a:bucket/b/index/i/",
		"arn:aws-cn:s3vectors:r:a:bucket/b/index/i",
		"arn:aws:s3vectors:other:a:bucket/b/index/i",
		"arn:aws:s3vectors:r:other:bucket/b/index/i",
	} {
		v = vectorBucketARNs{}
		if err := s.arnParts(&v, "", bad); err == nil {
			t.Fatalf("ARN %q should be rejected", bad)
		}
	}
}

func TestBucketARNs(t *testing.T) {
	s := &Server{region: "r", accountID: "a"}
	if v, err := s.bucketARNs("plain", ""); err != nil || v.bucket != "plain" {
		t.Fatalf("plain name: %+v err=%v", v, err)
	}
	if v, err := s.bucketARNs("", "arn:aws:s3vectors:r:a:bucket/mybucket"); err != nil || v.bucket != "mybucket" {
		t.Fatalf("bucket ARN: %+v err=%v", v, err)
	}
	for _, bad := range []string{
		"arn:aws:s3vectors:r:a:bucket/b/index/i",
		"arn:aws:s3tables:r:a:bucket/b",
		"arn:aws:s3:r:a:bucket/b",
	} {
		if _, err := s.bucketARNs("", bad); err == nil {
			t.Fatalf("ARN %q should be rejected for a bucket operation", bad)
		}
	}
	if _, err := s.bucketARNs("a", "arn:aws:s3vectors:r:a:bucket/b"); err == nil {
		t.Fatal("conflicting vectorBucketName and vectorBucketArn should be rejected")
	}
}

func TestResolveIndexARNs(t *testing.T) {
	s := &Server{region: "r", accountID: "a"}
	arns, err := s.resolveIndexARNs("", "", "", "arn:aws:s3vectors:r:a:bucket/b/index/i")
	if err != nil || arns.bucket != "b" || arns.index != "i" {
		t.Fatalf("indexArn only: %+v err=%v", arns, err)
	}
	arns, err = s.resolveIndexARNs("b", "", "i", "")
	if err != nil || arns.bucket != "b" || arns.index != "i" {
		t.Fatalf("names only: %+v err=%v", arns, err)
	}
	arns, err = s.resolveIndexARNs("", "arn:aws:s3vectors:r:a:bucket/b", "", "arn:aws:s3vectors:r:a:bucket/b/index/i")
	if err != nil || arns.bucket != "b" || arns.index != "i" {
		t.Fatalf("bucket and index ARNs: %+v err=%v", arns, err)
	}
	for _, tc := range []struct{ bucketARN, indexARN, indexName string }{
		{"", "arn:aws:s3vectors:r:a:bucket/b", "i"},
		{"arn:aws:s3vectors:r:a:bucket/b/index/i", "", "i"},
		{"arn:aws:s3vectors:r:a:bucket/other", "arn:aws:s3vectors:r:a:bucket/b/index/i", ""},
		{"arn:aws-cn:s3vectors:r:a:bucket/b", "arn:aws:s3vectors:r:a:bucket/b/index/i", ""},
		{"arn:aws:s3vectors:other:a:bucket/b", "arn:aws:s3vectors:r:a:bucket/b/index/i", ""},
		{"arn:aws:s3vectors:r:other:bucket/b", "arn:aws:s3vectors:r:a:bucket/b/index/i", ""},
	} {
		if _, err := s.resolveIndexARNs("", tc.bucketARN, tc.indexName, tc.indexARN); err == nil {
			t.Errorf("accepted incompatible ARNs: %+v", tc)
		}
	}
	if _, err := s.resolveIndexARNs("other", "", "", "arn:aws:s3vectors:r:a:bucket/b/index/i"); err == nil {
		t.Fatal("conflicting vectorBucketName should be rejected")
	}
	if _, err := s.resolveIndexARNs("", "", "other", "arn:aws:s3vectors:r:a:bucket/b/index/i"); err == nil {
		t.Fatal("conflicting indexName should be rejected")
	}
	if _, err := s.resolveIndexARNs("", "not-an-arn", "i", ""); err == nil {
		t.Fatal("non-ARN vectorBucketArn should be rejected")
	}
	if _, err := s.resolveIndexARNs("", "", "", ""); err == nil {
		t.Fatal("missing identifiers should be rejected")
	}
	if _, err := s.resolveIndexARNs("b", "", "", ""); err == nil {
		t.Fatal("missing indexName should be rejected")
	}
}

func TestTranslateFilter(t *testing.T) {
	cases := []struct {
		name   string
		filter map[string]interface{}
		want   string
	}{
		{"bare string equality", map[string]interface{}{"k": "v"},
			"json_extract(`metadata`, '$[\"k\"]') = '\"v\"'"},
		{"bare number equality", map[string]interface{}{"k": float64(3)},
			"TRY_CAST(json_extract(`metadata`, '$[\"k\"]') AS DOUBLE) = 3"},
		{"bare bool equality", map[string]interface{}{"k": true},
			"json_extract(`metadata`, '$[\"k\"]') = 'true'"},
		{"ne", map[string]interface{}{"k": map[string]interface{}{"$ne": "v"}},
			"NOT COALESCE(json_extract(`metadata`, '$[\"k\"]') = '\"v\"', false)"},
		{"exists true", map[string]interface{}{"k": map[string]interface{}{"$exists": true}},
			"json_exists(`metadata`, '$[\"k\"]')"},
		{"exists false", map[string]interface{}{"k": map[string]interface{}{"$exists": false}},
			"NOT json_exists(`metadata`, '$[\"k\"]')"},
		{"exists special key", map[string]interface{}{`a.b"c`: map[string]interface{}{"$exists": true}},
			"json_exists(`metadata`, '$[\"a.b\\\"c\"]')"},
		{"range", map[string]interface{}{"k": map[string]interface{}{"$gt": float64(1)}},
			"TRY_CAST(json_extract(`metadata`, '$[\"k\"]') AS DOUBLE) > 1"},
		{"in strings", map[string]interface{}{"k": map[string]interface{}{"$in": []interface{}{"a", "b"}}},
			"(COALESCE(json_extract(`metadata`, '$[\"k\"]') in ('\"a\"', '\"b\"'), false))"},
		{"nin numbers", map[string]interface{}{"k": map[string]interface{}{"$nin": []interface{}{float64(1)}}},
			"NOT (COALESCE(TRY_CAST(json_extract(`metadata`, '$[\"k\"]') AS DOUBLE) in (1), false))"},
		{"in mixed types", map[string]interface{}{"k": map[string]interface{}{"$in": []interface{}{"a", float64(1), true, float64(2)}}},
			"(COALESCE(json_extract(`metadata`, '$[\"k\"]') in ('\"a\"', 'true'), false) OR COALESCE(TRY_CAST(json_extract(`metadata`, '$[\"k\"]') AS DOUBLE) in (1, 2), false))"},
		{"and", map[string]interface{}{"$and": []interface{}{
			map[string]interface{}{"a": "1"}, map[string]interface{}{"b": float64(2)},
		}},
			"(json_extract(`metadata`, '$[\"a\"]') = '\"1\"' AND TRY_CAST(json_extract(`metadata`, '$[\"b\"]') AS DOUBLE) = 2)"},
		{"sql injection", map[string]interface{}{"a'b": "v'1"},
			"json_extract(`metadata`, '$[\"a''b\"]') = '\"v''1\"'"},
	}
	for _, tc := range cases {
		got, err := translateFilter(tc.filter)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if got != tc.want {
			t.Fatalf("%s:\n got %s\nwant %s", tc.name, got, tc.want)
		}
	}

	for name, filter := range map[string]map[string]interface{}{
		"unknown operator":   {"k": map[string]interface{}{"$bogus": "v"}},
		"exists non-bool":    {"k": map[string]interface{}{"$exists": "yes"}},
		"range non-number":   {"k": map[string]interface{}{"$gt": "v"}},
		"in non-list":        {"k": map[string]interface{}{"$in": "v"}},
		"top-level bad op":   {"$bogus": []interface{}{}},
		"nested bad member":  {"$and": []interface{}{"x"}},
		"empty and list":     {"$and": []interface{}{}},
		"empty or list":      {"$or": []interface{}{}},
		"empty sub-object":   {"$and": []interface{}{map[string]interface{}{}}},
		"empty operators":    {"k": map[string]interface{}{}},
		"empty among fields": {"k": map[string]interface{}{}, "j": "1"},
		"empty under and": {"$and": []interface{}{map[string]interface{}{
			"k": map[string]interface{}{}, "j": map[string]interface{}{}}}},
	} {
		if _, err := translateFilter(filter); err == nil {
			t.Fatalf("%s should fail", name)
		}
	}
	if got, err := translateFilter(nil); err != nil || got != "" {
		t.Fatalf("empty filter: %q %v", got, err)
	}
}

// The catalog stores nonFilterableMetadataKeys as a JSON string; an older
// record may still carry the array form. Both must round-trip into the index.
func TestIndexFromRecordNonFilterableKeys(t *testing.T) {
	s := &Server{}
	record := func(keys interface{}) map[string]interface{} {
		return map[string]interface{}{
			"metadata": map[string]interface{}{
				"fullMetadata": map[string]interface{}{
					"dimension":                 "128",
					"distanceMetric":            "cosine",
					"nonFilterableMetadataKeys": keys,
				},
			},
		}
	}

	idx := indexFromRecord(s, "b", "i", record(`["sku","internal"]`))
	if idx.MetadataConfiguration == nil ||
		len(idx.MetadataConfiguration.NonFilterableMetadataKeys) != 2 ||
		idx.MetadataConfiguration.NonFilterableMetadataKeys[0] != "sku" ||
		idx.MetadataConfiguration.NonFilterableMetadataKeys[1] != "internal" {
		t.Fatalf("JSON string form: %+v", idx.MetadataConfiguration)
	}

	idx = indexFromRecord(s, "b", "i", record([]interface{}{"sku"}))
	if idx.MetadataConfiguration == nil ||
		len(idx.MetadataConfiguration.NonFilterableMetadataKeys) != 1 ||
		idx.MetadataConfiguration.NonFilterableMetadataKeys[0] != "sku" {
		t.Fatalf("array form: %+v", idx.MetadataConfiguration)
	}

	for _, absent := range []interface{}{nil, "not-json", `{"a":1}`} {
		idx = indexFromRecord(s, "b", "i", record(absent))
		if idx.MetadataConfiguration != nil {
			t.Fatalf("keys %v should leave configuration unset, got %+v", absent, idx.MetadataConfiguration)
		}
	}
}

// QueryVectors rejects filters touching non-filterable keys; the extraction
// must see through nested boolean operators.
func TestFilterFieldsNestedOperators(t *testing.T) {
	filter := map[string]interface{}{
		"$and": []interface{}{
			map[string]interface{}{"color": "red"},
			map[string]interface{}{"$or": []interface{}{
				map[string]interface{}{"sku": map[string]interface{}{"$eq": "x"}},
				map[string]interface{}{"$and": []interface{}{
					map[string]interface{}{"size": map[string]interface{}{"$gt": 3}},
				}},
			}},
		},
	}
	fields := map[string]bool{}
	filterFields(filter, fields)
	for _, want := range []string{"color", "sku", "size"} {
		if !fields[want] {
			t.Fatalf("missing field %q in %v", want, fields)
		}
	}
	if fields["$and"] || fields["$or"] || fields["$eq"] {
		t.Fatalf("operators leaked into fields: %v", fields)
	}

	// The rejection gate the handler applies to the extracted fields.
	blocked := &MetadataConfiguration{NonFilterableMetadataKeys: []string{"sku"}}
	if got, hit := nonFilterableField(blocked, filter); !hit || got != "sku" {
		t.Fatalf("blocked key inside nested filter not detected: %q %v", got, hit)
	}
	allowed := &MetadataConfiguration{NonFilterableMetadataKeys: []string{"secret"}}
	if got, hit := nonFilterableField(allowed, filter); hit {
		t.Fatalf("unrelated non-filterable key should pass, got %q", got)
	}
	if got, hit := nonFilterableField(nil, filter); hit {
		t.Fatalf("no configuration should pass, got %q", got)
	}
	emptyBlocked := &MetadataConfiguration{NonFilterableMetadataKeys: []string{""}}
	if _, hit := nonFilterableField(emptyBlocked, map[string]interface{}{
		"": map[string]interface{}{"$eq": "x"},
	}); !hit {
		t.Fatal("an empty blocked key should still reject")
	}
}

func TestToPutVectorDimensionCheck(t *testing.T) {
	good := &PutInputVector{Key: "a", Data: &VectorData{Float32: []float32{1, 2, 3}}}
	for _, tc := range []struct {
		name string
		v    *PutInputVector
		dim  int32
		ok   bool
	}{
		{"matching", good, 3, true},
		{"mismatched", good, 4, false},
		{"missing data", &PutInputVector{Key: "a"}, 3, false},
		{"empty data", &PutInputVector{Key: "a", Data: &VectorData{}}, 3, false},
		{"null element", nil, 3, false},
		{"empty key", &PutInputVector{Data: &VectorData{Float32: []float32{1, 2, 3}}}, 3, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := toPutVector(tc.v, tc.dim); (err == nil) != tc.ok {
				t.Fatalf("toPutVector err = %v, want ok %v", err, tc.ok)
			}
		})
	}
}

func TestPutVectorsRejectsBadBatchSizes(t *testing.T) {
	s := &Server{}
	for _, n := range []int{0, vectorsMaxPutBatch + 1} {
		vectors := make([]*PutInputVector, n)
		for i := range vectors {
			vectors[i] = &PutInputVector{}
		}
		body, _ := json.Marshal(&PutVectorsRequest{
			VectorBucketName: "bucket",
			IndexName:        "index",
			Vectors:          vectors,
		})
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodPost, "/PutVectors", bytes.NewReader(body))
		s.handlePutVectors(w, r)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("batch of %d: status %d, want %d", n, w.Code, http.StatusBadRequest)
		}
	}
}

// allowAllAuth stands in for the zero-config deployment NewServer wires
// DefaultAllow from.
type allowAllAuth struct{}

func (allowAllAuth) AuthenticateRequest(r *http.Request) (string, interface{}, s3err.ErrorCode) {
	return "", nil, s3err.ErrNone
}
func (allowAllAuth) DefaultAllow() bool { return true }

func postJSON(t *testing.T, handler http.HandlerFunc, payload interface{}) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	w := httptest.NewRecorder()
	handler(w, httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body)))
	return w
}

// The handler must take the declared dimension from the catalog record: a
// matching vector reaches the engine, a mismatching one is rejected without a
// PutVectors call, and a full vectorsMaxPutBatch batch is accepted.
func TestPutVectorsUsesCatalogDimension(t *testing.T) {
	fs := s3tablestest.Start(t)
	engine := &fakeVectors{}
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
	if len(engine.created) != 1 || engine.created[0].Dimension != 3 {
		t.Fatalf("engine CreateIndex calls = %v, want one dimension-3 index", engine.created)
	}

	put := func(vectors ...*PutInputVector) *httptest.ResponseRecorder {
		return postJSON(t, s.handlePutVectors, &PutVectorsRequest{
			VectorBucketName: "vec-bucket",
			IndexName:        "idx",
			Vectors:          vectors,
		})
	}

	matching := &PutInputVector{Key: "a", Data: &VectorData{Float32: []float32{1, 2, 3}}}
	if w := put(matching); w.Code != http.StatusOK {
		t.Fatalf("matching vector: %d %s", w.Code, w.Body.String())
	}
	if len(engine.puts) != 1 || len(engine.puts[0].Vectors) != 1 {
		t.Fatalf("engine PutVectors calls = %v, want one call of one vector", engine.puts)
	}

	mismatched := &PutInputVector{Key: "b", Data: &VectorData{Float32: []float32{1, 2, 3, 4}}}
	if w := put(mismatched); w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "ValidationException") {
		t.Fatalf("mismatched vector: %d %s, want 400 ValidationException", w.Code, w.Body.String())
	}
	if len(engine.puts) != 1 {
		t.Fatalf("rejected vector reached the engine: %d calls", len(engine.puts))
	}

	batch := make([]*PutInputVector, vectorsMaxPutBatch)
	for i := range batch {
		batch[i] = &PutInputVector{Key: fmt.Sprintf("k%d", i), Data: &VectorData{Float32: []float32{1, 2, 3}}}
	}
	if w := put(batch...); w.Code != http.StatusOK {
		t.Fatalf("%d-vector batch: %d %s", vectorsMaxPutBatch, w.Code, w.Body.String())
	}
	if len(engine.puts) != 2 || len(engine.puts[1].Vectors) != vectorsMaxPutBatch {
		t.Fatalf("engine PutVectors calls = %d, want a second call of %d vectors", len(engine.puts), vectorsMaxPutBatch)
	}
}

// errFiler fails any catalog access, so a request that passes request-shape
// validation dies in the catalog rather than reaching the engine.
type errFiler struct{}

func (errFiler) WithFilerClient(_ bool, _ func(filer_pb.SeaweedFilerClient) error) error {
	return fmt.Errorf("no filer")
}

// Rejected topK values fail before the catalog and the engine; valid ones get
// past the gate (here: into the catalog, which the errFiler fails).
func TestQueryVectorsTopKBounds(t *testing.T) {
	s := &Server{
		vectors:     newFakeVectors(0),
		tables:      s3tables.NewManager(),
		internal:    s3tables.NewManager(),
		filerClient: errFiler{},
	}
	for _, tc := range []struct {
		name     string
		topK     int
		rejected bool
	}{
		{"zero", 0, true},
		{"negative", -3, true},
		{"over the limit", vectorsMaxTopK + 1, true},
		{"one", 1, false},
		{"at the limit", vectorsMaxTopK, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := fmt.Sprintf(`{"vectorBucketName":"b","indexName":"i","queryVector":{"float32":[0.1]},"topK":%d}`, tc.topK)
			w := httptest.NewRecorder()
			s.handleQueryVectors(w, httptest.NewRequest(http.MethodPost, "/QueryVectors", strings.NewReader(body)))
			rejected := w.Code == http.StatusBadRequest && strings.Contains(w.Body.String(), "topK must be between")
			if rejected != tc.rejected {
				t.Fatalf("topK %d: rejected=%v (%d %s), want %v", tc.topK, rejected, w.Code, w.Body.String(), tc.rejected)
			}
		})
	}
}
func TestValidateEncryptionConfigurationRejectsCustomerKeys(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  map[string]interface{}
		want bool
	}{
		{"empty", nil, true},
		{"default sse", map[string]interface{}{"sseType": "AES256"}, true},
		{"tables wire name", map[string]interface{}{"sseAlgorithm": "AES256"}, false},
		{"both wire names", map[string]interface{}{"sseType": "AES256", "sseAlgorithm": "AES256"}, false},
		{"customer key", map[string]interface{}{"sseType": "aws:kms"}, false},
		{"tables kms", map[string]interface{}{"sseAlgorithm": "aws:kms"}, false},
		{"unknown sse", map[string]interface{}{"sseType": "aws:fsx"}, false},
		{"missing algorithm", map[string]interface{}{"other": "x"}, false},
		{"key without a type", map[string]interface{}{"kmsKeyArn": "arn:aws:kms:us-east-1:123456789012:key/x"}, false},
		{"key on default sse", map[string]interface{}{"sseType": "AES256", "kmsKeyArn": "arn:aws:kms:us-east-1:123456789012:key/x"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := s3tables.ValidateEncryptionConfiguration(tc.cfg, "sseType") == nil; got != tc.want {
				t.Fatalf("cfg %v: accepted=%v, want %v", tc.cfg, got, tc.want)
			}
		})
	}
}

// A vector index is colocated: its catalog entry sits at the dataset root, so
// the catalog delete removes entry and data together.
func TestDeleteIndexRemovesDatasetDirectory(t *testing.T) {
	fs := s3tablestest.Start(t)
	internal := s3tables.NewManager()
	internal.SetTrusted(true)
	tables := s3tables.NewManager()
	tables.SetDefaultAllow(true)
	s := &Server{
		filerClient: s3tables.NewManagerClient(fs.Client),
		tables:      tables,
		internal:    internal,
	}
	ctx := context.Background()
	bucketArn := s.s3tablesArn("vec-bucket")

	var resp map[string]interface{}
	if err := s.internalCatalog(ctx, "CreateTableBucket",
		&s3tables.CreateTableBucketRequest{Name: "vec-bucket", Format: s3tables.FormatLance}, &resp); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	if err := s.internalCatalog(ctx, "CreateNamespace", &s3tables.CreateNamespaceRequest{
		TableBucketARN: bucketArn, Namespace: []string{indexNamespace}}, &resp); err != nil {
		t.Fatalf("create namespace: %v", err)
	}
	if err := s.internalCatalog(ctx, "CreateTable", &s3tables.CreateTableRequest{
		TableBucketARN: bucketArn, Namespace: []string{indexNamespace}, Name: "idx",
		Format: s3tables.FormatLance}, &resp); err != nil {
		t.Fatalf("create index record: %v", err)
	}
	fs.PutFile("/buckets/vec-bucket/vector/idx/data", "part-0.lance", time.Now())

	body, _ := json.Marshal(&DeleteIndexRequest{VectorBucketName: "vec-bucket", IndexName: "idx"})
	w := httptest.NewRecorder()
	s.handleDeleteIndex(w, httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body)))
	if w.Code != http.StatusOK {
		t.Fatalf("delete index: %d %s", w.Code, w.Body.String())
	}
	if e := fs.Get("/buckets/vec-bucket/vector", "idx"); e != nil {
		t.Fatal("index catalog entry survived DeleteIndex")
	}
	if e := fs.Get("/buckets/vec-bucket/vector/idx/data", "part-0.lance"); e != nil {
		t.Fatal("index dataset survived DeleteIndex")
	}
}

func TestValidateVectorPolicy(t *testing.T) {
	valid := `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3vectors:PutVectors","Resource":"arn:aws:s3vectors:r:a:bucket/b"}]}`
	if err := validateVectorPolicy(valid); err != nil {
		t.Fatalf("valid policy rejected: %v", err)
	}
	if err := validateVectorPolicy("not-json"); err == nil {
		t.Fatal("malformed policy should be rejected")
	}
	for _, field := range []string{"NotAction", "NotResource", "NotPrincipal", "Condition"} {
		bad := fmt.Sprintf(`{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","%s":"s3vectors:GetIndex","Action":"s3vectors:PutVectors","Resource":"*"}]}`, field)
		if err := validateVectorPolicy(bad); err == nil {
			t.Fatalf("%s should be rejected", field)
		}
		lower := fmt.Sprintf(`{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","%s":"s3vectors:GetIndex","Action":"s3vectors:PutVectors","Resource":"*"}]}`, strings.ToLower(field))
		if err := validateVectorPolicy(lower); err == nil {
			t.Fatalf("case-variant %s should be rejected", strings.ToLower(field))
		}
	}
	for _, dead := range []string{
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3tables:GetTable","Resource":"*"}]}`,
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","action":"s3tables:GetTable","Resource":"*"}]}`,
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3vectors:GetVectors","Resource":"arn:aws:s3:r:a:bucket/b"}]}`,
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3vectors:GetVectors","resource":"arn:aws:s3tables:r:a:bucket/b"}]}`,
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3vectors:GetVectors","Resource":"arn:aws:s3tables:r:a:bucket/b"}]}`,
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3vectors:GetVectors","Resource":"arn:aws-cn:s3tables:r:a:bucket/b"}]}`,
	} {
		if err := validateVectorPolicy(dead); err == nil {
			t.Fatalf("unenforceable rule should be rejected: %s", dead)
		}
	}
}

// postJSONAs runs the handler under a named identity: empty resolves to the
// account the catalog would charge, a name is a non-owner principal.
func postJSONAs(t *testing.T, identity string, handler http.HandlerFunc, payload interface{}) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	handler(w, r.WithContext(contextWithIdentity(r.Context(), identity)))
	return w
}

// A grant names the exact vector action and resource it allows: a put grant
// carries no delete, an index-scoped rule reaches only that index, and a
// read grant never authorizes a write.
func TestVectorBucketPolicyGatesVectorActions(t *testing.T) {
	fs := s3tablestest.Start(t)
	s := NewServer(s3tables.NewManagerClient(fs.Client), allowAllAuth{}, newFakeVectors(0))

	if w := postJSONAs(t, "", s.handleCreateVectorBucket, &CreateVectorBucketRequest{VectorBucketName: "vec-bucket"}); w.Code != http.StatusOK {
		t.Fatalf("create bucket: %d %s", w.Code, w.Body.String())
	}
	newIndex := func(name string) {
		t.Helper()
		if w := postJSONAs(t, "", s.handleCreateIndex, &CreateIndexRequest{
			VectorBucketName: "vec-bucket",
			IndexName:        name,
			DataType:         "float32",
			Dimension:        3,
			DistanceMetric:   "euclidean",
		}); w.Code != http.StatusOK {
			t.Fatalf("create index %s: %d %s", name, w.Code, w.Body.String())
		}
	}
	newIndex("idx")
	newIndex("other")

	indexARN := s.indexArn("vec-bucket", "idx")
	policy := fmt.Sprintf(`{"Version":"2012-10-17","Statement":[`+
		`{"Sid":"put","Effect":"Allow","Principal":"*","Action":"s3vectors:PutVectors","Resource":"%s"},`+
		`{"Sid":"reads","Effect":"Allow","Principal":"*","Action":"s3vectors:Get*","Resource":"%s"},`+
		`{"Sid":"noDelete","Effect":"Deny","Principal":"*","Action":"s3vectors:DeleteVectors","Resource":"%s"}]}`,
		indexARN, indexARN, indexARN)
	if w := postJSONAs(t, "", s.handlePutVectorBucketPolicy, &PutVectorBucketPolicyRequest{
		VectorBucketName: "vec-bucket",
		Policy:           policy,
	}); w.Code != http.StatusOK {
		t.Fatalf("put policy: %d %s", w.Code, w.Body.String())
	}
	if w := postJSONAs(t, "", s.handleGetVectorBucketPolicy, &GetVectorBucketPolicyRequest{VectorBucketName: "vec-bucket"}); w.Code != http.StatusOK {
		t.Fatalf("get policy: %d %s", w.Code, w.Body.String())
	} else {
		var resp GetVectorBucketPolicyResponse
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("decode policy response: %v", err)
		}
		if resp.Policy != policy {
			t.Fatalf("policy round-trip changed the document:\nsaved: %s\ngot:   %s", policy, resp.Policy)
		}
	}

	put := func(index string) *httptest.ResponseRecorder {
		return postJSONAs(t, "alice", s.handlePutVectors, &PutVectorsRequest{
			VectorBucketName: "vec-bucket",
			IndexName:        index,
			Vectors:          []*PutInputVector{{Key: "a", Data: &VectorData{Float32: []float32{1, 2, 3}}}},
		})
	}
	if w := put("idx"); w.Code != http.StatusOK {
		t.Fatalf("put on granted index: %d %s", w.Code, w.Body.String())
	}
	if w := put("other"); w.Code != http.StatusForbidden {
		t.Fatalf("put on ungranted index: %d %s, want 403", w.Code, w.Body.String())
	}
	if w := postJSONAs(t, "alice", s.handleDeleteVectors, &DeleteVectorsRequest{
		VectorBucketName: "vec-bucket", IndexName: "idx", Keys: []string{"a"},
	}); w.Code != http.StatusForbidden {
		t.Fatalf("delete under deny: %d %s, want 403", w.Code, w.Body.String())
	}
	if w := postJSONAs(t, "alice", s.handleGetVectors, &GetVectorsRequest{
		VectorBucketName: "vec-bucket", IndexName: "idx", Keys: []string{"a"},
	}); w.Code != http.StatusOK {
		t.Fatalf("get under wildcard read grant: %d %s", w.Code, w.Body.String())
	}
	if w := postJSONAs(t, "alice", s.handleListIndexes, &ListIndexesRequest{VectorBucketName: "vec-bucket"}); w.Code != http.StatusForbidden {
		t.Fatalf("bucket-scoped list under index grant: %d %s, want 403", w.Code, w.Body.String())
	}
}

// A vector document still living in the shared policy slot is a legacy
// policy: deleting through the vector API strips its s3vectors statements,
// keeps catalog rules, and the stripped grant stops authorizing.
func TestDeleteVectorBucketPolicyRemovesLegacyDoc(t *testing.T) {
	fs := s3tablestest.Start(t)
	s := NewServer(s3tables.NewManagerClient(fs.Client), allowAllAuth{}, newFakeVectors(0))

	lookupEntry := func(bucket string) *filer_pb.Entry {
		t.Helper()
		dir, name := splitEntryPath(s3tables.GetTableBucketPath(bucket))
		resp, err := filer_pb.LookupEntry(context.Background(), fs.Client, &filer_pb.LookupDirectoryEntryRequest{Directory: dir, Name: name})
		if err != nil {
			t.Fatalf("lookup bucket %s: %v", bucket, err)
		}
		return resp.Entry
	}
	writeSharedPolicy := func(bucket, doc string) {
		t.Helper()
		entry := lookupEntry(bucket)
		if entry.Extended == nil {
			entry.Extended = map[string][]byte{}
		}
		entry.Extended[s3tables.ExtendedKeyPolicy] = []byte(doc)
		dir, _ := splitEntryPath(s3tables.GetTableBucketPath(bucket))
		if _, err := fs.Client.UpdateEntry(context.Background(), &filer_pb.UpdateEntryRequest{Directory: dir, Entry: entry}); err != nil {
			t.Fatalf("write shared policy on %s: %v", bucket, err)
		}
	}
	create := func(bucket string) {
		t.Helper()
		if w := postJSONAs(t, "", s.handleCreateVectorBucket, &CreateVectorBucketRequest{VectorBucketName: bucket}); w.Code != http.StatusOK {
			t.Fatalf("create bucket %s: %d %s", bucket, w.Code, w.Body.String())
		}
	}
	deletePolicy := func(bucket string) *httptest.ResponseRecorder {
		return postJSONAs(t, "", s.handleDeleteVectorBucketPolicy, &DeleteVectorBucketPolicyRequest{VectorBucketName: bucket})
	}
	getAsAlice := func(bucket string) *httptest.ResponseRecorder {
		return postJSONAs(t, "alice", s.handleGetVectorBucket, &GetVectorBucketRequest{VectorBucketName: bucket})
	}

	create("vec-mixed")
	writeSharedPolicy("vec-mixed", fmt.Sprintf(`{"Version":"2012-10-17","Statement":[`+
		`{"Effect":"Allow","Principal":"*","Action":"s3tables:GetTableBucket","Resource":"%s"},`+
		`{"Effect":"Allow","Principal":"*","Action":"s3vectors:GetVectorBucket","Resource":"%s"}]}`,
		s.s3tablesArn("vec-mixed"), s.vectorBucketArn("vec-mixed")))
	if w := getAsAlice("vec-mixed"); w.Code != http.StatusOK {
		t.Fatalf("legacy grant should authorize: %d %s", w.Code, w.Body.String())
	}
	if w := deletePolicy("vec-mixed"); w.Code != http.StatusOK {
		t.Fatalf("delete mixed policy: %d %s", w.Code, w.Body.String())
	}
	if w := getAsAlice("vec-mixed"); w.Code != http.StatusForbidden {
		t.Fatalf("stripped grant should deny: %d %s, want 403", w.Code, w.Body.String())
	}
	if doc := string(lookupEntry("vec-mixed").Extended[s3tables.ExtendedKeyPolicy]); !strings.Contains(doc, "s3tables:") || strings.Contains(doc, "s3vectors:") {
		t.Fatalf("catalog statements should survive the delete: %s", doc)
	}

	create("vec-only")
	writeSharedPolicy("vec-only", fmt.Sprintf(`{"Version":"2012-10-17","Statement":[`+
		`{"Effect":"Allow","Principal":"*","Action":"s3vectors:GetVectorBucket","Resource":"%s"}]}`,
		s.vectorBucketArn("vec-only")))
	if w := deletePolicy("vec-only"); w.Code != http.StatusOK {
		t.Fatalf("delete vector-only policy: %d %s", w.Code, w.Body.String())
	}
	if doc := string(lookupEntry("vec-only").Extended[s3tables.ExtendedKeyPolicy]); doc != "" {
		t.Fatalf("empty shared policy should be removed: %s", doc)
	}
}

func keys(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = "k"
	}
	return out
}

func TestVectorKeysBounds(t *testing.T) {
	for _, tc := range []struct {
		name string
		keys []string
		max  int
		ok   bool
	}{
		{"empty", nil, vectorsMaxGetKeys, false},
		{"one", []string{"a"}, vectorsMaxGetKeys, true},
		{"get full", keys(vectorsMaxGetKeys), vectorsMaxGetKeys, true},
		{"get over", keys(vectorsMaxGetKeys + 1), vectorsMaxGetKeys, false},
		{"delete full", keys(vectorsMaxPutBatch), vectorsMaxPutBatch, true},
		{"delete over", keys(vectorsMaxPutBatch + 1), vectorsMaxPutBatch, false},
	} {
		if err := vectorKeysBounds(tc.keys, tc.max); (err == nil) != tc.ok {
			t.Fatalf("%s: accepted=%v, want %v", tc.name, err == nil, tc.ok)
		}
	}
}

// An out-of-window key list is rejected before the engine sees it.
func TestVectorKeysBoundsRejectsBeforeEngine(t *testing.T) {
	f := newFakeVectors(0)
	s := &Server{vectors: f}
	for _, tc := range []struct {
		name    string
		handler http.HandlerFunc
		payload interface{}
	}{
		{"get empty", s.handleGetVectors, &GetVectorsRequest{}},
		{"get over", s.handleGetVectors, &GetVectorsRequest{Keys: keys(vectorsMaxGetKeys + 1)}},
		{"delete empty", s.handleDeleteVectors, &DeleteVectorsRequest{}},
		{"delete over", s.handleDeleteVectors, &DeleteVectorsRequest{Keys: keys(vectorsMaxPutBatch + 1)}},
	} {
		w := postJSONAs(t, "", tc.handler, tc.payload)
		if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "ValidationException") {
			t.Fatalf("%s: %d %s, want 400 ValidationException", tc.name, w.Code, w.Body.String())
		}
	}
	if len(f.gets)+len(f.deletes) != 0 {
		t.Fatalf("engine saw %d get/%d delete calls, want none", len(f.gets), len(f.deletes))
	}
}

// Two indexes created at once in a new bucket both declare the implicit
// namespace. One create takes over the other's namespace while it is still
// marked unfinished, so the other's confirmation fails; its index is created
// all the same.
func TestCreateIndexSurvivesAConcurrentNamespaceCreate(t *testing.T) {
	fs := s3tablestest.Start(t)
	s := NewServer(s3tables.NewManagerClient(fs.Client), allowAllAuth{}, newFakeVectors(0))
	if w := postJSON(t, s.handleCreateVectorBucket, &CreateVectorBucketRequest{VectorBucketName: "vec-bucket"}); w.Code != http.StatusOK {
		t.Fatalf("create bucket: %d %s", w.Code, w.Body.String())
	}
	bucketPath := s3tables.GetTableBucketPath("vec-bucket")
	reached := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseFirst := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseFirst)
	held := false
	// Hold the first create where it confirms the bucket after writing the namespace.
	fs.OnLookup = func(dir, name string) {
		if held || dir != s3tables.TablesPath || name != "vec-bucket" {
			return
		}
		ns := fs.Get(bucketPath, indexNamespace)
		if ns == nil || string(ns.Extended[s3tables.ExtendedKeyDeleting]) != "create" {
			return
		}
		held = true
		close(reached)
		<-release
	}
	createIndex := func(name string) *httptest.ResponseRecorder {
		return postJSON(t, s.handleCreateIndex, &CreateIndexRequest{
			VectorBucketName: "vec-bucket", IndexName: name, DataType: "float32", Dimension: 2, DistanceMetric: "cosine",
		})
	}
	first := make(chan *httptest.ResponseRecorder, 1)
	go func() { first <- createIndex("first") }()
	select {
	case <-reached:
	case <-time.After(10 * time.Second):
		t.Fatal("the first create never wrote an unfinished namespace")
	}

	if w := createIndex("second"); w.Code != http.StatusOK {
		t.Fatalf("second create index: %d %s", w.Code, w.Body.String())
	}
	releaseFirst()
	if w := <-first; w.Code != http.StatusOK {
		t.Fatalf("first create index: %d %s", w.Code, w.Body.String())
	}
}

func TestVectorErrKeepsEngineDetailsOutOfTheResponse(t *testing.T) {
	arns := vectorBucketARNs{bucket: "media", index: "movies"}
	for _, tc := range []struct {
		err         error
		wantStatus  int
		wantCode    string
		wantMessage string
	}{
		{status.Error(codes.Internal, "declare: 500 from http://10.0.0.5:8888/lance/v1/table/media$vector$movies/declare"),
			http.StatusInternalServerError, "InternalFailure", internalErrorMessage},
		{status.Error(codes.Unavailable, "no weed-vector registered on master"),
			http.StatusServiceUnavailable, "ServiceUnavailableException", "vector service unavailable"},
		{status.Error(codes.InvalidArgument, "vector has dimension 2, index has 3"),
			http.StatusBadRequest, "ValidationException", "vector has dimension 2, index has 3"},
	} {
		rec := httptest.NewRecorder()
		vectorErr(rec, httptest.NewRequest(http.MethodPost, "/PutVectors", nil), arns, tc.err)
		var body map[string]string
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if rec.Code != tc.wantStatus || body["code"] != tc.wantCode || body["message"] != tc.wantMessage {
			t.Errorf("%v: got %d %s %q, want %d %s %q", status.Code(tc.err), rec.Code, body["code"], body["message"],
				tc.wantStatus, tc.wantCode, tc.wantMessage)
		}
	}
}
