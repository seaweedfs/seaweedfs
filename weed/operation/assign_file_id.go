package operation

import (
	"encoding/json"
	"errors"
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

func Assign(server string, r *VolumeAssignRequest) (*AssignResult, error) {
	values := make(url.Values)
	values.Add("count", strconv.FormatUint(r.Count, 10))
	if r.Replication != "" {
		values.Add("replication", r.Replication)
	}
	if r.Collection != "" {
		values.Add("collection", r.Collection)
	}
	if r.Ttl != "" {
		values.Add("ttl", r.Ttl)
	}
	if r.DataCenter != "" {
		values.Add("dataCenter", r.DataCenter)
	}
	if r.Rack != "" {
		values.Add("rack", r.Rack)
	}
	if r.DataNode != "" {
		values.Add("dataNode", r.DataNode)
	}

	jsonBlob, err := util.Post("http://"+server+"/dir/assign", values)
	glog.V(2).Infof("assign result from %s : %s", server, string(jsonBlob))
	if err != nil {
		return nil, err
	}
	var ret AssignResult
	err = json.Unmarshal(jsonBlob, &ret)
	if err != nil {
		return nil, fmt.Errorf("/dir/assign result JSON unmarshal error:%v, json:%s", err, string(jsonBlob))
	}
	if ret.Count <= 0 {
		return nil, errors.New(ret.Error)
	}
	return &ret, nil
}
