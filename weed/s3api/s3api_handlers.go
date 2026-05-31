package s3api

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"google.golang.org/grpc"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
)

var _ = filer_pb.FilerClient(&S3ApiServer{})

func (s3a *S3ApiServer) WithFilerClient(streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) error {
	return s3a.WithFilerClientContext(context.Background(), streamingMode, fn)
}

func (s3a *S3ApiServer) WithFilerClientContext(ctx context.Context, streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) error {
	if ctx == nil {
		ctx = context.Background()
	}

	// Use filerClient for proper connection management and failover
	if s3a.filerClient != nil {
		return s3a.withFilerClientFailover(ctx, streamingMode, fn)
	}

	// Fallback to direct connection if filerClient not initialized
	// This should only happen during initialization or testing
	return s3a.withGrpcFilerClient(ctx, streamingMode, s3a.getFilerAddress().ToGrpcAddress(), fn)
}

// withFilerClientFailover attempts to execute fn with automatic failover to other filers
func (s3a *S3ApiServer) withFilerClientFailover(ctx context.Context, streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) error {
	// Get current filer as starting point
	currentFiler := s3a.filerClient.GetCurrentFiler()

	// Try current filer first (fast path)
	err := s3a.withGrpcFilerClient(ctx, streamingMode, currentFiler.ToGrpcAddress(), fn)

	if err == nil {
		s3a.filerClient.RecordFilerSuccess(currentFiler)
		return nil
	}

	if shouldStopFilerFailover(ctx, err) {
		return err
	}

	// A reachable filer answering ErrNotFound is authoritative; failover is for
	// unreachable/unhealthy filers, not for re-asking about an absent entry.
	if errors.Is(err, filer_pb.ErrNotFound) {
		return err
	}

	s3a.filerClient.RecordFilerFailure(currentFiler)

	// Current filer failed - try all other filers with health-aware selection
	filers := s3a.filerClient.GetAllFilers()
	var lastErr error = err

	for _, filer := range filers {
		if filer == currentFiler {
			continue // Already tried this one
		}

		// Skip filers known to be unhealthy (circuit breaker pattern)
		if s3a.filerClient.ShouldSkipUnhealthyFiler(filer) {
			glog.V(2).Infof("WithFilerClient: skipping unhealthy filer %s", filer)
			continue
		}

		err = s3a.withGrpcFilerClient(ctx, streamingMode, filer.ToGrpcAddress(), fn)

		if err == nil {
			// Success! Record success and update current filer for future requests
			s3a.filerClient.RecordFilerSuccess(filer)
			s3a.filerClient.SetCurrentFiler(filer)
			glog.V(1).Infof("WithFilerClient: failover from %s to %s succeeded", currentFiler, filer)
			return nil
		}

		if shouldStopFilerFailover(ctx, err) {
			return err
		}

		// Authoritative not-found - stop failing over.
		if errors.Is(err, filer_pb.ErrNotFound) {
			return err
		}

		s3a.filerClient.RecordFilerFailure(filer)
		glog.V(2).Infof("WithFilerClient: failover to %s failed: %v", filer, err)
		lastErr = err
	}

	// All filers failed
	return fmt.Errorf("all filers failed, last error: %w", lastErr)
}

func (s3a *S3ApiServer) withGrpcFilerClient(ctx context.Context, streamingMode bool, address string, fn func(filer_pb.SeaweedFilerClient) error) error {
	return pb.WithGrpcClient(streamingMode, s3a.randomClientId, func(grpcConnection *grpc.ClientConn) error {
		err := fn(filer_pb.NewSeaweedFilerClient(grpcConnection))
		if shouldStopFilerFailover(ctx, err) {
			return pb.WithoutConnectionInvalidation(err)
		}
		return err
	}, address, false, s3a.option.GrpcDialOption)
}

func shouldStopFilerFailover(ctx context.Context, err error) bool {
	return err != nil && ctx.Err() != nil && isRequestDoneError(err)
}

func (s3a *S3ApiServer) AdjustedUrl(location *filer_pb.Location) string {
	return location.Url
}

func (s3a *S3ApiServer) GetDataCenter() string {
	return s3a.option.DataCenter
}

func writeSuccessResponseXML(w http.ResponseWriter, r *http.Request, response interface{}) {
	s3err.WriteXMLResponse(w, r, http.StatusOK, response)
	s3err.PostLog(r, http.StatusOK, s3err.ErrNone)
}

func writeSuccessResponseXMLBytes(w http.ResponseWriter, r *http.Request, response []byte) {
	s3err.WriteResponse(w, r, http.StatusOK, response, s3err.MimeXML)
	s3err.PostLog(r, http.StatusOK, s3err.ErrNone)
}

func writeSuccessResponseEmpty(w http.ResponseWriter, r *http.Request) {
	s3err.WriteEmptyResponse(w, r, http.StatusOK)
}

func writeFailureResponse(w http.ResponseWriter, r *http.Request, errCode s3err.ErrorCode) {
	s3err.WriteErrorResponse(w, r, errCode)
}

func validateContentMd5(h http.Header) ([]byte, error) {
	md5B64, ok := h["Content-Md5"]
	if ok {
		if md5B64[0] == "" {
			return nil, fmt.Errorf("Content-Md5 header set to empty value")
		}
		return base64.StdEncoding.DecodeString(md5B64[0])
	}
	return []byte{}, nil
}
