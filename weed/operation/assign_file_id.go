package operation

import (
	"bytes"
	"context"
	"fmt"

	"github.com/valyala/fasthttp"
	"google.golang.org/grpc"

	"github.com/chrislusf/seaweedfs/weed/glog"
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

		lastError = WithMasterServerClient(server, grpcDialOption, func(ctx context.Context, masterClient master_pb.SeaweedClient) error {

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
	lookupUrl := fmt.Sprintf("http://%s/dir/lookup?fileId=%s", master, fileId)

	err := util.Head(lookupUrl, func(header *fasthttp.ResponseHeader) {
		bearer := header.Peek("Authorization")
		if len(bearer) > 7 && string(bytes.ToUpper(bearer[0:6])) == "BEARER" {
			tokenStr = string(bearer[7:])
		}
	})
	if err != nil {
		glog.V(0).Infof("failed to lookup jwt %s: %v", lookupUrl, err)
	}

	return security.EncodedJwt(tokenStr)
}
