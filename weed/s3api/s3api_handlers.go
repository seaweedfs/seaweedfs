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
	// Use filerClient for proper connection management and failover
	if s3a.filerClient != nil {
		return s3a.withFilerClientFailover("", streamingMode, fn)
	}

	// Fallback to direct connection if filerClient not initialized
	// This should only happen during initialization or testing
	return pb.WithGrpcClient(context.Background(), streamingMode, s3a.randomClientId, func(grpcConnection *grpc.ClientConn) error {
		client := filer_pb.NewSeaweedFilerClient(grpcConnection)
		return fn(client)
	}, s3a.getFilerAddress().ToGrpcAddress(), false, s3a.option.GrpcDialOption)

}

// withFilerClientFailover runs fn against an ordered set of filers, failing over
// only on transport errors. A NotFound from a reached filer is authoritative: it
// stops failover, so an absent entry is never re-asked elsewhere (no fan-out, and
// no resurrecting a peer's not-yet-replicated tombstone).
//
// Order: preferred (if set), then the gateway's current filer, then the rest.
// preferred lets a caller route a read to a key's ring owner — where routed writes
// land — for read-after-write across gateways. It may be a group filer outside the
// static -filer list; the health and current-filer bookkeeping no-op for untracked
// addresses. A successful failover updates the current filer for later requests, but
// a successful preferred read does not (its owner is per-key, not a new default).
func (s3a *S3ApiServer) withFilerClientFailover(preferred pb.ServerAddress, streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) error {
	currentFiler := s3a.filerClient.GetCurrentFiler()

	// Build an ordered, de-duplicated candidate list.
	candidates := make([]pb.ServerAddress, 0, 2+len(s3a.option.Filers))
	seen := make(map[pb.ServerAddress]bool)
	addCandidate := func(filer pb.ServerAddress) {
		if filer == "" || seen[filer] {
			return
		}
		seen[filer] = true
		candidates = append(candidates, filer)
	}
	addCandidate(preferred)
	addCandidate(currentFiler)
	for _, filer := range s3a.filerClient.GetAllFilers() {
		addCandidate(filer)
	}

	var lastErr error
	for i, filer := range candidates {
		// Always attempt the first candidate so a request makes progress even when
		// every filer is flagged unhealthy; skip flagged ones only when failing over.
		if i > 0 && s3a.filerClient.ShouldSkipUnhealthyFiler(filer) {
			glog.V(2).Infof("WithFilerClient: skipping unhealthy filer %s", filer)
			continue
		}

		err := pb.WithGrpcClient(context.Background(), streamingMode, s3a.randomClientId, func(grpcConnection *grpc.ClientConn) error {
			return fn(filer_pb.NewSeaweedFilerClient(grpcConnection))
		}, filer.ToGrpcAddress(), false, s3a.option.GrpcDialOption)

		if err == nil {
			s3a.filerClient.RecordFilerSuccess(filer)
			// Prefer a healthy filer we failed over to for future requests, but don't
			// let a per-key preferred owner become the gateway's default.
			if filer != currentFiler && filer != preferred {
				s3a.filerClient.SetCurrentFiler(filer)
				glog.V(1).Infof("WithFilerClient: failover from %s to %s succeeded", currentFiler, filer)
			}
			return nil
		}

		// A reachable filer answering ErrNotFound is authoritative; failover is for
		// unreachable/unhealthy filers, not for re-asking about an absent entry.
		if errors.Is(err, filer_pb.ErrNotFound) {
			return err
		}

		s3a.filerClient.RecordFilerFailure(filer)
		glog.V(2).Infof("WithFilerClient: filer %s failed: %v", filer, err)
		lastErr = err
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("no filer available")
	}
	return fmt.Errorf("all filers failed, last error: %w", lastErr)
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
