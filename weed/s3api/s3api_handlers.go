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

// withFilerClientFailover runs fn against preferred (if set) first, then the
// remaining filers (current, then the rest) with healthy ones before unhealthy.
// Failover is for transport errors only: a reached filer's ErrNotFound is
// authoritative (no
// fan-out, no resurrecting a peer's not-yet-replicated tombstone). preferred lets a
// caller route to a key's ring owner for read-after-write; it may be a filer outside
// the static list (the bookkeeping no-ops for untracked addresses). A failover
// updates the current filer; a preferred read does not, as its owner is per-key.
// Failover replays fn from scratch, so it stops once any response has reached
// fn: a replay after that could silently duplicate state fn accumulated (a
// listing that failed mid-stream, say), so the error surfaces instead and the
// caller decides whether a clean-slate retry is safe.
func (s3a *S3ApiServer) withFilerClientFailover(preferred pb.ServerAddress, streamingMode bool, fn func(filer_pb.SeaweedFilerClient) error) error {
	currentFiler := s3a.filerClient.GetCurrentFiler()

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

	// preferred (the routed owner) is tried first even when health-flagged, so a
	// replica's authoritative NotFound can't mask a write that only reached the owner.
	// A recently-unreachable filer counts as unhealthy, deferring a dead owner that is
	// also the current filer to the tail rather than dialing it before healthy replicas.
	var healthy, unhealthy []pb.ServerAddress
	for _, filer := range candidates {
		if filer == preferred {
			continue
		}
		if s3a.filerClient.ShouldSkipUnhealthyFiler(filer) || s3a.ownerRecentlyUnreachable(filer) {
			unhealthy = append(unhealthy, filer)
		} else {
			healthy = append(healthy, filer)
		}
	}
	ordered := make([]pb.ServerAddress, 0, len(candidates))
	if preferred != "" {
		ordered = append(ordered, preferred)
	}
	ordered = append(ordered, healthy...)
	ordered = append(ordered, unhealthy...)

	var lastErr error
	for _, filer := range ordered {
		received := false
		err := pb.WithGrpcClient(context.Background(), streamingMode, s3a.randomClientId, func(grpcConnection *grpc.ClientConn) error {
			return fn(filer_pb.NewSeaweedFilerClient(receiveTrackingConn{ClientConnInterface: grpcConnection, received: &received}))
		}, filer.ToGrpcAddress(), false, s3a.option.GrpcDialOption)

		if err == nil {
			s3a.filerClient.RecordFilerSuccess(filer)
			if filer != currentFiler && filer != preferred {
				s3a.filerClient.SetCurrentFiler(filer)
				glog.V(1).Infof("WithFilerClient: failover from %s to %s succeeded", currentFiler, filer)
			}
			return nil
		}
		if errors.Is(err, filer_pb.ErrNotFound) {
			return err
		}

		s3a.filerClient.RecordFilerFailure(filer)
		// A preferred owner is often outside the static filer list, where the health
		// tracking above no-ops; flag it so route-by-key reads skip it briefly.
		if filer == preferred {
			s3a.markOwnerUnreachable(filer)
		}
		glog.V(2).Infof("WithFilerClient: filer %s failed: %v", filer, err)
		// fn consumed part of a response; a replay would stack a second copy
		// onto whatever it accumulated, so surface the error unwrapped.
		if received {
			return err
		}
		lastErr = err
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("no filer available")
	}
	return fmt.Errorf("all filers failed, last error: %w", lastErr)
}

// receiveTrackingConn flags *received once a unary reply or a streamed message
// has been handed to the callback, the point past which a failover replay is
// no longer transparent.
type receiveTrackingConn struct {
	grpc.ClientConnInterface
	received *bool
}

func (c receiveTrackingConn) Invoke(ctx context.Context, method string, args, reply any, opts ...grpc.CallOption) error {
	err := c.ClientConnInterface.Invoke(ctx, method, args, reply, opts...)
	if err == nil {
		*c.received = true
	}
	return err
}

func (c receiveTrackingConn) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	stream, err := c.ClientConnInterface.NewStream(ctx, desc, method, opts...)
	if err != nil {
		return nil, err
	}
	return receiveTrackingStream{ClientStream: stream, received: c.received}, nil
}

type receiveTrackingStream struct {
	grpc.ClientStream
	received *bool
}

func (s receiveTrackingStream) RecvMsg(m any) error {
	err := s.ClientStream.RecvMsg(m)
	if err == nil {
		*s.received = true
	}
	return err
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
