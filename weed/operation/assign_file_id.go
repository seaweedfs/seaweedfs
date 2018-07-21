package operation

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"

	"github.com/chrislusf/seaweedfs/weed/glog"
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
	for i, request := range requests {
		if request == nil {
			continue
		}
		values := make(url.Values)
		values.Add("count", strconv.FormatUint(request.Count, 10))
		if request.Replication != "" {
			values.Add("replication", request.Replication)
		}
		if request.Collection != "" {
			values.Add("collection", request.Collection)
		}
		if request.Ttl != "" {
			values.Add("ttl", request.Ttl)
		}
		if request.DataCenter != "" {
			values.Add("dataCenter", request.DataCenter)
		}
		if request.Rack != "" {
			values.Add("rack", request.Rack)
		}
		if request.DataNode != "" {
			values.Add("dataNode", request.DataNode)
		}

		postUrl := fmt.Sprintf("http://%s/dir/assign", server)
		jsonBlob, err := util.Post(postUrl, values)
		glog.V(2).Infof("assign %d result from %s %+v : %s", i, postUrl, values, string(jsonBlob))
		if err != nil {
			return nil, err
		}
		var ret AssignResult
		err = json.Unmarshal(jsonBlob, &ret)
		if err != nil {
			return nil, fmt.Errorf("/dir/assign result JSON unmarshal error:%v, json:%s", err, string(jsonBlob))
		}
		if ret.Count <= 0 {
			lastError = fmt.Errorf("assign failure %d: %v", i+1, ret.Error)
			continue
		}
		return &ret, nil
	}
	return nil, lastError
}
