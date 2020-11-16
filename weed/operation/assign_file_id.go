package operation

import (
	"context"
	"fmt"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"google.golang.org/grpc"

	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/util"
)

type VolumeAssignRequest struct {
	Count               uint64
	Replication         string
	Collection          string
	Ttl                 string
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

func Assign(server string, grpcDialOption grpc.DialOption, primaryRequest *VolumeAssignRequest, alternativeRequests ...*VolumeAssignRequest) (*AssignResult, error) {

	var requests []*VolumeAssignRequest
	requests = append(requests, primaryRequest)
	requests = append(requests, alternativeRequests...)

	var lastError error
	ret := &AssignResult{}

	for i, request := range requests {
		if request == nil {
			continue
		}

		lastError = WithMasterServerClient(server, grpcDialOption, func(masterClient master_pb.SeaweedClient) error {

			req := &master_pb.AssignRequest{
				Count:               primaryRequest.Count,
				Replication:         primaryRequest.Replication,
				Collection:          primaryRequest.Collection,
				Ttl:                 primaryRequest.Ttl,
				DataCenter:          primaryRequest.DataCenter,
				Rack:                primaryRequest.Rack,
				DataNode:            primaryRequest.DataNode,
				WritableVolumeCount: primaryRequest.WritableVolumeCount,
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

			return nil

		})

		if lastError != nil {
			continue
		}

		if ret.Count <= 0 {
			lastError = fmt.Errorf("assign failure %d: %v", i+1, ret.Error)
			continue
		}

	}

	return ret, lastError
}

func LookupJwt(master string, fileId string) security.EncodedJwt {

	tokenStr := ""

	if h, e := util.Head(fmt.Sprintf("http://%s/dir/lookup?fileId=%s", master, fileId)); e == nil {
		bearer := h.Get("Authorization")
		if len(bearer) > 7 && strings.ToUpper(bearer[0:6]) == "BEARER" {
			tokenStr = bearer[7:]
		}
	}

	return security.EncodedJwt(tokenStr)
}

type StorageOption struct {
	Replication string
	Collection  string
	DataCenter  string
	Rack        string
	TtlSeconds  int32
	Fsync       bool
}

func (so *StorageOption) TtlString() string {
	return needle.SecondsToTTL(so.TtlSeconds)
}

func (so *StorageOption) ToAssignRequests(count int) (ar *VolumeAssignRequest, altRequest *VolumeAssignRequest) {
	ar = &VolumeAssignRequest{
		Count:       uint64(count),
		Replication: so.Replication,
		Collection:  so.Collection,
		Ttl:         so.TtlString(),
		DataCenter:  so.DataCenter,
		Rack:        so.Rack,
	}
	if so.DataCenter != "" || so.Rack != "" {
		altRequest = &VolumeAssignRequest{
			Count:       uint64(count),
			Replication: so.Replication,
			Collection:  so.Collection,
			Ttl:         so.TtlString(),
			DataCenter:  "",
			Rack:        "",
		}
	}
	return
}

