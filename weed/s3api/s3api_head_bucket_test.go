package s3api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
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
}

func (f *fakeLookupFiler) LookupDirectoryEntry(ctx context.Context, req *filer_pb.LookupDirectoryEntryRequest) (*filer_pb.LookupDirectoryEntryResponse, error) {
	if f.lookupErr != nil {
		return nil, f.lookupErr
	}
	return &filer_pb.LookupDirectoryEntryResponse{Entry: f.entry}, nil
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
		wantBody string
	}{
		{
			name:     "transient lookup failure",
			filer:    &fakeLookupFiler{lookupErr: status.Error(codes.Internal, "filer store unavailable")},
			wantCode: http.StatusInternalServerError,
			wantBody: "<Code>InternalError</Code>",
		},
		{
			name:     "missing bucket",
			filer:    &fakeLookupFiler{},
			wantCode: http.StatusNotFound,
			wantBody: "<Code>NoSuchBucket</Code>",
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
			if tc.wantBody != "" && !strings.Contains(rr.Body.String(), tc.wantBody) {
				t.Fatalf("body = %s, want %s", rr.Body.String(), tc.wantBody)
			}
		})
	}
}
