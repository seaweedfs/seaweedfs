package operation

import (
	"context"
	"fmt"

	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
)

type VolumeAssignRequest struct {
	Count       uint64
	Replication string
	Collection  string
	Ttl         string
	DataCenter  string
	Rack        string
	DataNode    string
}

type AssignResult struct {
	Fid       string `json:"fid,omitempty"`
	Url       string `json:"url,omitempty"`
	PublicUrl string `json:"publicUrl,omitempty"`
	Count     uint64 `json:"count,omitempty"`
	Error     string `json:"error,omitempty"`
}

func Assign(server string, primaryRequest *VolumeAssignRequest, alternativeRequests ...*VolumeAssignRequest) (*AssignResult, error) {

	var requests []*VolumeAssignRequest
	requests = append(requests, primaryRequest)
	requests = append(requests, alternativeRequests...)

	var lastError error
	ret := &AssignResult{}

	for i, request := range requests {
		if request == nil {
			continue
		}

		lastError = withMasterServerClient(server, func(masterClient master_pb.SeaweedClient) error {
			req := &master_pb.AssignRequest{
				Count:       primaryRequest.Count,
				Replication: primaryRequest.Replication,
				Collection:  primaryRequest.Collection,
				Ttl:         primaryRequest.Ttl,
				DataCenter:  primaryRequest.DataCenter,
				Rack:        primaryRequest.Rack,
				DataNode:    primaryRequest.DataNode,
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
