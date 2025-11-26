package s3api

import (
	"encoding/base64"
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
	// Use filerClient for proper connection management and failover
	if s3a.filerClient != nil {
		return s3a.withFilerClientFailover(streamingMode, fn)
	}
	
	// Fallback to direct connection if filerClient not initialized
	// This should only happen during initialization or testing
	return pb.WithGrpcClient(streamingMode, s3a.randomClientId, func(grpcConnection *grpc.ClientConn) error {
		client := filer_pb.NewSeaweedFilerClient(grpcConnection)
		return fn(client)
	}, s3a.getFilerAddress().ToGrpcAddress(), false, s3a.option.GrpcDialOption)

}

// withFilerClientFailover attempts to execute fn with automatic failover to other filers
func (s3a *S3ApiServer) withFilerClientFailover(streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) error {
	// Get current filer as starting point
	currentFiler := s3a.filerClient.GetCurrentFiler()
	
	// Try current filer first
	err := pb.WithGrpcClient(streamingMode, s3a.randomClientId, func(grpcConnection *grpc.ClientConn) error {
		client := filer_pb.NewSeaweedFilerClient(grpcConnection)
		return fn(client)
	}, currentFiler.ToGrpcAddress(), false, s3a.option.GrpcDialOption)
	
	if err == nil {
		return nil
	}
	
	// Current filer failed - try all other filers
	// Note: This is a simple failover implementation
	// For production, consider implementing exponential backoff and circuit breakers
	filers := s3a.filerClient.GetAllFilers()
	
	for _, filer := range filers {
		if filer == currentFiler {
			continue // Already tried this one
		}
		
		err = pb.WithGrpcClient(streamingMode, s3a.randomClientId, func(grpcConnection *grpc.ClientConn) error {
			client := filer_pb.NewSeaweedFilerClient(grpcConnection)
			return fn(client)
		}, filer.ToGrpcAddress(), false, s3a.option.GrpcDialOption)
		
		if err == nil {
			// Success! Update current filer for future requests
			s3a.filerClient.SetCurrentFiler(filer)
			glog.V(1).Infof("WithFilerClient: failover from %s to %s succeeded", currentFiler, filer)
			return nil
		}
		
		glog.V(2).Infof("WithFilerClient: failover to %s failed: %v", filer, err)
	}
	
	// All filers failed
	return fmt.Errorf("all filers failed, last error: %w", err)
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
