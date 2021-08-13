package operation

import (
	"context"
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"google.golang.org/grpc"

	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/security"
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
}

type AssignResult struct {
	Fid       string              `json:"fid,omitempty"`
	Url       string              `json:"url,omitempty"`
	PublicUrl string              `json:"publicUrl,omitempty"`
	Count     uint64              `json:"count,omitempty"`
	Error     string              `json:"error,omitempty"`
	Auth      security.EncodedJwt `json:"auth,omitempty"`
}

func Assign(masterFn GetMasterFn, grpcDialOption grpc.DialOption, primaryRequest *VolumeAssignRequest, alternativeRequests ...*VolumeAssignRequest) (*AssignResult, error) {

	var requests []*VolumeAssignRequest
	requests = append(requests, primaryRequest)
	requests = append(requests, alternativeRequests...)

	var lastError error
	ret := &AssignResult{}

	for i, request := range requests {
		if request == nil {
			continue
		}

		lastError = WithMasterServerClient(masterFn(), grpcDialOption, func(masterClient master_pb.SeaweedClient) error {

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
			}
			resp, grpcErr := masterClient.Assign(context.Background(), req)
			if grpcErr != nil {
				return grpcErr
			}

			ret.Count = resp.Count
			ret.Fid = resp.Fid
			ret.Url = resp.Url
			ret.PublicUrl = resp.PublicUrl
			ret.Error = resp.Error
			ret.Auth = security.EncodedJwt(resp.Auth)

			if resp.Error != "" {
				return fmt.Errorf("assignRequest: %v", resp.Error)
			}

			return nil

		})

		if lastError != nil {
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

func LookupJwt(master string, grpcDialOption grpc.DialOption, fileId string) (token security.EncodedJwt) {

	WithMasterServerClient(master, grpcDialOption, func(masterClient master_pb.SeaweedClient) error {

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
	TtlSeconds        int32
	Fsync             bool
	VolumeGrowthCount uint32
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
		WritableVolumeCount: so.VolumeGrowthCount,
	}
	if so.DataCenter != "" || so.Rack != "" {
		altRequest = &VolumeAssignRequest{
			Count:               uint64(count),
			Replication:         so.Replication,
			Collection:          so.Collection,
			Ttl:                 so.TtlString(),
			DiskType:            so.DiskType,
			DataCenter:          "",
			Rack:                "",
			WritableVolumeCount: so.VolumeGrowthCount,
		}
	}
	return
}
