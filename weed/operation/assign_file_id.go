package operation

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"strings"
	"time"

	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/util"
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

		lastError = withMasterServerClient(server, grpcDialOption, func(masterClient master_pb.SeaweedClient) error {
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(5*time.Second))
			defer cancel()

			req := &master_pb.AssignRequest{
				Count:       primaryRequest.Count,
				Replication: primaryRequest.Replication,
				Collection:  primaryRequest.Collection,
				Ttl:         primaryRequest.Ttl,
				DataCenter:  primaryRequest.DataCenter,
				Rack:        primaryRequest.Rack,
				DataNode:    primaryRequest.DataNode,
			}
			resp, grpcErr := masterClient.Assign(ctx, req)
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
