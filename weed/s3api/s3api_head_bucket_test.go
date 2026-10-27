package s3api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path"
	"testing"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/wdclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type fakeLookupFiler struct {
	filer_pb.UnimplementedSeaweedFilerServer
	entry     *filer_pb.Entry
	lookupErr error
	lookups   int
	failAfter int
	failErr   error
	deleted   []string
	updateErr error
	txnResp   *filer_pb.ObjectTransactionResponse
	txnErr    error
	txnSeen   []*filer_pb.ObjectTransactionRequest
}

func (f *fakeLookupFiler) LookupDirectoryEntry(ctx context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	f.lookups++
	if f.failAfter > 0 && f.lookups > f.failAfter {
		return nil, f.failErr
	}
	if f.lookupErr != nil {
		return nil, f.lookupErr
	}
	return &filer_pb.LookupDirectoryEntryResponse{Entry: f.entry}, nil
}

func (f *fakeLookupFiler) DeleteEntry(ctx context.Context, req *filer_pb.DeleteEntryRequest) (*filer_pb.DeleteEntryResponse, error) {
	f.deleted = append(f.deleted, path.Join(req.Directory, req.Name))
	f.entry = nil
	return &filer_pb.DeleteEntryResponse{}, nil
}

func (f *fakeLookupFiler) UpdateEntry(ctx context.Context, req *filer_pb.UpdateEntryRequest) (*filer_pb.UpdateEntryResponse, error) {
	if f.updateErr != nil {
		return nil, f.updateErr
	}
	return &filer_pb.UpdateEntryResponse{}, nil
}

func (f *fakeLookupFiler) ObjectTransaction(ctx context.Context, req *filer_pb.ObjectTransactionRequest) (*filer_pb.ObjectTransactionResponse, error) {
	f.txnSeen = append(f.txnSeen, req)
	if f.txnErr != nil {
		return nil, f.txnErr
	}
	if f.txnResp != nil {
		return f.txnResp, nil
	}
	return &filer_pb.ObjectTransactionResponse{}, nil
}

func newHeadBucketTestServer(t *testing.T, impl filer_pb.SeaweedFilerServer) *S3ApiServer {
	t.Helper()
	filers := []pb.ServerAddress{startFakeFiler(t, impl)}
	dialOption := grpc.WithTransportCredentials(insecure.NewCredentials())
	return &S3ApiServer{
		option:      &S3ApiServerOption{Filers: filers, GrpcDialOption: dialOption, BucketsPath: "/buckets"},
		filerClient: wdclient.NewFilerClient(filers, dialOption, ""),
	}
}

// A lookup that fails for a reason other than "not found" must not be reported
// as a missing bucket: a 404 is a definite answer and stops the client retrying.
func TestHeadBucketSeparatesLookupFailureFromMissingBucket(t *testing.T) {
	const bucket = "head-bucket"
	cases := []struct {
		name     string
		filer    *fakeLookupFiler
		wantCode int
	}{
		{
			name:     "transient lookup failure",
			filer:    &fakeLookupFiler{lookupErr: status.Error(codes.Internal, "filer store unavailable")},
			wantCode: http.StatusInternalServerError,
		},
		{
			name:     "missing bucket",
			filer:    &fakeLookupFiler{},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "existing bucket",
			filer:    &fakeLookupFiler{entry: &filer_pb.Entry{Name: bucket, IsDirectory: true}},
			wantCode: http.StatusOK,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s3a := newHeadBucketTestServer(t, tc.filer)
			req := httptest.NewRequest(http.MethodHead, "/"+bucket, nil)
			req = mux.SetURLVars(req, map[string]string{"bucket": bucket})
			rr := httptest.NewRecorder()

			s3a.HeadBucketHandler(rr, req)

			if rr.Code != tc.wantCode {
				t.Fatalf("status = %d, want %d: %s", rr.Code, tc.wantCode, rr.Body.String())
			}
			// HEAD returns the status without an XML error body, including for
			// lookup failures and missing buckets.
			if rr.Body.Len() != 0 {
				t.Fatalf("HEAD response must have an empty body, got %q", rr.Body.String())
			}
		})
	}
}
