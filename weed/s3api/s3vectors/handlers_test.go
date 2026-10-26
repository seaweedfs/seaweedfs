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
