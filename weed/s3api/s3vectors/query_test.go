package s3vectors

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/seaweedfs/seaweedfs/weed/pb/vector_pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestSearchMode(t *testing.T) {
	for _, tc := range []struct {
		name, mode, auth, query string
		indexed, invalid        bool
	}{
		{name: "default"},
		{name: "all", mode: "all"},
		{name: "indexed", mode: "indexed-only", indexed: true},
		{name: "unknown", mode: "fast", invalid: true},
		{name: "unsigned header", mode: "indexed-only", auth: "AWS4-HMAC-SHA256 Credential=x, SignedHeaders=host, Signature=x", invalid: true},
		{name: "signed header", mode: "indexed-only", auth: "AWS4-HMAC-SHA256 Credential=x, SignedHeaders=host;x-seaweedfs-vector-search-mode, Signature=x", indexed: true},
		{name: "signed query", mode: "indexed-only", query: "?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-SignedHeaders=host%3Bx-seaweedfs-vector-search-mode", indexed: true},
		{name: "unsigned query", mode: "indexed-only", query: "?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-SignedHeaders=host", invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "/QueryVectors"+tc.query, nil)
			if tc.mode != "" {
				r.Header.Set(searchModeHeader, tc.mode)
			}
			r.Header.Set("Authorization", tc.auth)
			indexed, err := searchMode(r)
			if indexed != tc.indexed || (err != nil) != tc.invalid {
				t.Fatalf("indexed=%v err=%v", indexed, err)
			}
		})
	}
	for _, values := range [][]string{{""}, {"all", "indexed-only"}, {"indexed-only,all"}} {
		r := httptest.NewRequest(http.MethodPost, "/QueryVectors", nil)
		r.Header[http.CanonicalHeaderKey(searchModeHeader)] = values
		if _, err := searchMode(r); err == nil {
			t.Fatalf("accepted %v", values)
		}
	}
}

type queryClient struct {
	*fakeVectors
	headers           metadata.MD
	err               error
	indexed, ordinary int
}

func (c *queryClient) QueryVectors(context.Context, *vector_pb.QueryVectorsRequest, ...grpc.CallOption) (*vector_pb.QueryVectorsResponse, error) {
	c.ordinary++
	return &vector_pb.QueryVectorsResponse{}, nil
}

func (c *queryClient) QueryIndexedVectors(_ context.Context, _ *vector_pb.QueryVectorsRequest, opts ...grpc.CallOption) (*vector_pb.QueryVectorsResponse, error) {
	c.indexed++
	for _, opt := range opts {
		if header, ok := opt.(grpc.HeaderCallOption); ok {
			*header.HeaderAddr = c.headers
		}
	}
	return &vector_pb.QueryVectorsResponse{}, c.err
}

type routedQueryClient struct{ vector_pb.VectorServiceClient }

func (*routedQueryClient) QueryVectors(context.Context, *vector_pb.QueryVectorsRequest, ...grpc.CallOption) (*vector_pb.QueryVectorsResponse, error) {
	return nil, status.Error(codes.Internal, "distributed query must not handle indexed-only")
}

func TestQueryVectorsSearchMode(t *testing.T) {
	s := &Server{}
	backend := &queryClient{fakeVectors: newFakeVectors(0)}
	s.vectors = backend
	body := `{"vectorBucketName":"vec-bucket","indexName":"live-idx","queryVector":{"float32":[1,0,0]},"topK":1}`
	for _, tc := range []struct {
		name, mode string
		headers    metadata.MD
		err        error
		want       int
	}{
		{name: "default", want: http.StatusOK},
		{name: "explicit all", mode: "all", want: http.StatusOK},
		{name: "indexed", mode: "indexed-only", headers: metadata.Pairs(searchModeHeader, "indexed-only", excludedFragmentsHeader, "2"), want: http.StatusOK},
		{name: "old engine", mode: "indexed-only", err: status.Error(codes.Unimplemented, "old"), want: http.StatusServiceUnavailable},
		{name: "missing acknowledgement", mode: "indexed-only", want: http.StatusServiceUnavailable},
		{name: "invalid coverage", mode: "indexed-only", headers: metadata.Pairs(searchModeHeader, "indexed-only", excludedFragmentsHeader, "-1"), want: http.StatusInternalServerError},
		{name: "unsupported filter", mode: "indexed-only", err: status.Error(codes.InvalidArgument, "filter requires an index"), want: http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend.headers, backend.err = tc.headers, tc.err
			r := httptest.NewRequest(http.MethodPost, "/QueryVectors", strings.NewReader(body))
			if tc.mode != "" {
				r.Header.Set(searchModeHeader, tc.mode)
			}
			w := httptest.NewRecorder()
			indexed, err := searchMode(r)
			if err != nil {
				t.Fatal(err)
			}
			_, err = s.queryVectors(w, r, &vector_pb.QueryVectorsRequest{TopK: 1}, indexed)
			if err != nil {
				vectorErr(w, r, vectorBucketARNs{}, err)
			}
			if w.Code != tc.want {
				t.Fatalf("status=%d body=%s", w.Code, w.Body)
			}
			if tc.mode == "indexed-only" && tc.want == http.StatusOK {
				if w.Header().Get(dataExcludedHeader) != "true" || w.Header().Get(excludedFragmentsHeader) != "2" {
					t.Fatalf("headers=%v", w.Header())
				}
			}
		})
	}
	if backend.ordinary != 2 || backend.indexed != 5 {
		t.Fatalf("ordinary=%d indexed=%d", backend.ordinary, backend.indexed)
	}
	s.vectors = &routedQueryClient{VectorServiceClient: backend}
	backend.headers, backend.err = metadata.Pairs(searchModeHeader, "indexed-only", excludedFragmentsHeader, "0"), nil
	r := httptest.NewRequest(http.MethodPost, "/QueryVectors", strings.NewReader(body))
	r.Header.Set(searchModeHeader, "indexed-only")
	w := httptest.NewRecorder()
	indexed, err := searchMode(r)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.queryVectors(w, r, &vector_pb.QueryVectorsRequest{TopK: 1}, indexed)
	if err != nil {
		vectorErr(w, r, vectorBucketARNs{}, err)
	}
	if w.Code != http.StatusOK || w.Header().Get(dataExcludedHeader) != "false" {
		t.Fatalf("status=%d headers=%v body=%s", w.Code, w.Header(), w.Body)
	}
}
