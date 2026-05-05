package operation

import (
	"context"
	"fmt"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type VolumeAssignRequest struct {
	Count               uint64
	Replication         string
	Collection          string
	Ttl                 string
	DiskType            string
	DataCenter          string
	Rack                string
	DataNode            string
	WritableVolumeCount uint32
	ExpectedDataSize    uint64
}

type AssignResult struct {
	Fid       string              `json:"fid,omitempty"`
	Url       string              `json:"url,omitempty"`
	PublicUrl string              `json:"publicUrl,omitempty"`
	GrpcPort  int                 `json:"grpcPort,omitempty"`
	Count     uint64              `json:"count,omitempty"`
	Error     string              `json:"error,omitempty"`
	Auth      security.EncodedJwt `json:"auth,omitempty"`
	Replicas  []Location          `json:"replicas,omitempty"`
}

func Assign(ctx context.Context, masterFn GetMasterFn, grpcDialOption grpc.DialOption, primaryRequest *VolumeAssignRequest, alternativeRequests ...*VolumeAssignRequest) (*AssignResult, error) {

	var requests []*VolumeAssignRequest
	requests = append(requests, primaryRequest)
	requests = append(requests, alternativeRequests...)

	var lastError error
	ret := &AssignResult{}

	// Compute a single deadline so all request entries (primary + fallback)
	// share one 30s retry budget instead of each getting its own.
	// Use a deadline-aware context so both RetryWithBackoff and per-attempt
	// timeouts are bounded by the shared budget.
	deadline := time.Now().Add(30 * time.Second)
	deadlineCtx, deadlineCancel := context.WithDeadline(ctx, deadline)
	defer deadlineCancel()

	for i, request := range requests {
		if request == nil {
			continue
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}

		lastError = util.RetryWithBackoff(deadlineCtx, "assign", remaining,
			func(err error) bool {
				st, ok := status.FromError(err)
				if !ok {
					return false
				}
				switch st.Code() {
				case codes.Unavailable:
					return true
				case codes.Canceled, codes.DeadlineExceeded:
					// A stale cached gRPC channel (e.g., master restart behind
					// a k8s Service VIP) can return Canceled/DeadlineExceeded
					// immediately even though the caller's context is still
					// live. The first failure invalidates the cached ClientConn
					// via shouldInvalidateConnection; retry so the next attempt
					// dials a fresh channel.
					return deadlineCtx.Err() == nil
				}
				return false
			},
			func() error {
				// Per-attempt timeout to prevent a single slow RPC from consuming the entire retry budget
				attemptCtx, attemptCancel := context.WithTimeout(deadlineCtx, 10*time.Second)
				defer attemptCancel()
				return WithMasterServerClient(false, masterFn(attemptCtx), grpcDialOption, func(masterClient master_pb.SeaweedClient) error {
					req := &master_pb.AssignRequest{
						Count:               request.Count,
						Replication:         request.Replication,
						Collection:          request.Collection,
						Ttl:                 request.Ttl,
						DiskType:            request.DiskType,
						DataCenter:          request.DataCenter,
						Rack:                request.Rack,
						DataNode:            request.DataNode,
						WritableVolumeCount: request.WritableVolumeCount,
						ExpectedDataSize:    request.ExpectedDataSize,
					}
					resp, grpcErr := masterClient.Assign(attemptCtx, req)
					if grpcErr != nil {
						return grpcErr
					}

					if resp.Error != "" {
						return fmt.Errorf("assignRequest: %v", resp.Error)
					}

					ret.Count = resp.Count
					ret.Fid = resp.Fid
					ret.Url = resp.Location.Url
					ret.PublicUrl = resp.Location.PublicUrl
					ret.GrpcPort = int(resp.Location.GrpcPort)
					ret.Error = resp.Error
					ret.Auth = security.EncodedJwt(resp.Auth)
					ret.Replicas = nil
					for _, r := range resp.Replicas {
						ret.Replicas = append(ret.Replicas, Location{
							Url:        r.Url,
							PublicUrl:  r.PublicUrl,
							DataCenter: r.DataCenter,
						})
					}

					return nil
				})
			})

		if lastError != nil {
			stats.FilerHandlerCounter.WithLabelValues(stats.ErrorChunkAssign).Inc()
			continue
		}

		if ret.Count <= 0 {
			lastError = fmt.Errorf("assign failure %d: %v", i+1, ret.Error)
			continue
		}

		break
	}

	return ret, lastError
}

func LookupJwt(master pb.ServerAddress, grpcDialOption grpc.DialOption, fileId string) (token security.EncodedJwt) {

	WithMasterServerClient(false, master, grpcDialOption, func(masterClient master_pb.SeaweedClient) error {

		resp, grpcErr := masterClient.LookupVolume(context.Background(), &master_pb.LookupVolumeRequest{
			VolumeOrFileIds: []string{fileId},
		})
		if grpcErr != nil {
			return grpcErr
		}

		if len(resp.VolumeIdLocations) == 0 {
			return nil
		}

		token = security.EncodedJwt(resp.VolumeIdLocations[0].Auth)

		return nil

	})

	return
}

type StorageOption struct {
	Replication       string
	DiskType          string
	Collection        string
	DataCenter        string
	Rack              string
	DataNode          string
	TtlSeconds        int32
	VolumeGrowthCount uint32
	MaxFileNameLength uint32
	Fsync             bool
	SaveInside        bool
}

func (so *StorageOption) TtlString() string {
	return needle.SecondsToTTL(so.TtlSeconds)
}

func (so *StorageOption) ToAssignRequests(count int) (ar *VolumeAssignRequest, altRequest *VolumeAssignRequest) {
	ar = &VolumeAssignRequest{
		Count:               uint64(count),
		Replication:         so.Replication,
		Collection:          so.Collection,
		Ttl:                 so.TtlString(),
		DiskType:            so.DiskType,
		DataCenter:          so.DataCenter,
		Rack:                so.Rack,
		DataNode:            so.DataNode,
		WritableVolumeCount: so.VolumeGrowthCount,
	}
	if so.DataCenter != "" || so.Rack != "" || so.DataNode != "" {
		altRequest = &VolumeAssignRequest{
			Count:               uint64(count),
			Replication:         so.Replication,
			Collection:          so.Collection,
			Ttl:                 so.TtlString(),
			DiskType:            so.DiskType,
			DataCenter:          "",
			Rack:                "",
			DataNode:            "",
			WritableVolumeCount: so.VolumeGrowthCount,
		}
	}
	return
}
