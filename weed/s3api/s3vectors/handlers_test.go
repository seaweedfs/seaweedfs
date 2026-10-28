package s3vectors

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/vector_pb"
	"google.golang.org/grpc"
)

// fakeVectors serves a fixed row set with the engine's offset-token paging.
type fakeVectors struct {
	vector_pb.VectorServiceClient
	rows []*vector_pb.Vector
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
			out, next, err := s.listSegmentVectors(context.Background(), nil, offset, 7, 3, segmentIndex, false, false)
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
	out, next, err := s.listSegmentVectors(context.Background(), nil, 0, 5, 2, 0, false, false)
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
	out, next, err = s.listSegmentVectors(context.Background(), nil, offset, 5, 2, 0, false, false)
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
	out, next, err := s.listSegmentVectors(context.Background(), nil, 0, 10, 1, 0, false, false)
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
	out, next, err := s.listSegmentVectors(context.Background(), nil, 0, 0, 1, 0, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != vectorsDefaultPage || next != strconv.Itoa(vectorsDefaultPage) {
		t.Fatalf("default page: %d rows token %q", len(out), next)
	}
	out, next, err = s.listSegmentVectors(context.Background(), nil, 0, 5000, 1, 0, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != vectorsMaxPage || next != strconv.Itoa(vectorsMaxPage) {
		t.Fatalf("capped page: %d rows token %q", len(out), next)
	}
}

func TestArnParts(t *testing.T) {
	var v vectorBucketARNs

	if err := arnParts(&v, "plain", ""); err != nil || v.bucket != "plain" {
		t.Fatalf("plain name: bucket=%q err=%v", v.bucket, err)
	}
	if err := arnParts(&v, "", "arn:aws:s3vectors:r:a:bucket/mybucket"); err != nil || v.bucket != "mybucket" || v.index != "" {
		t.Fatalf("bucket ARN: %+v err=%v", v, err)
	}
	v = vectorBucketARNs{}
	if err := arnParts(&v, "", "arn:aws:s3vectors:r:a:bucket/mybucket/index/myindex"); err != nil || v.bucket != "mybucket" || v.index != "myindex" {
		t.Fatalf("index ARN: %+v err=%v", v, err)
	}
	for _, bad := range []string{
		"arn:aws:s3vectors:r:a:bucket/",
		"arn:aws:s3vectors:r:a:bucket//index/i",
		"arn:aws:s3vectors:r:a:bucket",
	} {
		v = vectorBucketARNs{}
		if err := arnParts(&v, "", bad); err == nil {
			t.Fatalf("ARN %q should be rejected", bad)
		}
	}
}

func TestResolveIndexARNs(t *testing.T) {
	arns, err := resolveIndexARNs("", "", "", "arn:aws:s3vectors:r:a:bucket/b/index/i")
	if err != nil || arns.bucket != "b" || arns.index != "i" {
		t.Fatalf("indexArn only: %+v err=%v", arns, err)
	}
	arns, err = resolveIndexARNs("b", "", "i", "")
	if err != nil || arns.bucket != "b" || arns.index != "i" {
		t.Fatalf("names only: %+v err=%v", arns, err)
	}
	if _, err := resolveIndexARNs("", "not-an-arn", "i", ""); err == nil {
		t.Fatal("non-ARN vectorBucketArn should be rejected")
	}
	if _, err := resolveIndexARNs("", "", "", ""); err == nil {
		t.Fatal("missing identifiers should be rejected")
	}
	if _, err := resolveIndexARNs("b", "", "", ""); err == nil {
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
			"(COALESCE(json_extract(`metadata`, '$[\"k\"]') in ('\"a\"'), false) OR COALESCE(json_extract(`metadata`, '$[\"k\"]') in ('\"b\"'), false))"},
		{"nin numbers", map[string]interface{}{"k": map[string]interface{}{"$nin": []interface{}{float64(1)}}},
			"NOT (COALESCE(TRY_CAST(json_extract(`metadata`, '$[\"k\"]') AS DOUBLE) in (1), false))"},
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
		"unknown operator":  {"k": map[string]interface{}{"$bogus": "v"}},
		"exists non-bool":   {"k": map[string]interface{}{"$exists": "yes"}},
		"range non-number":  {"k": map[string]interface{}{"$gt": "v"}},
		"in non-list":       {"k": map[string]interface{}{"$in": "v"}},
		"top-level bad op":  {"$bogus": []interface{}{}},
		"nested bad member": {"$and": []interface{}{"x"}},
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

	blocked := []string{"sku"}
	hit := false
	for _, k := range blocked {
		hit = hit || fields[k]
	}
	if !hit {
		t.Fatal("blocked key inside nested filter was not detected")
	}
}
