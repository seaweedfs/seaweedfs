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

type AssignResult struct {
	Fid       string `json:"fid,omitempty"`
	Url       string `json:"url,omitempty"`
	PublicUrl string `json:"publicUrl,omitempty"`
	Count     uint64 `json:"count,omitempty"`
	Error     string `json:"error,omitempty"`
}

/*
options params meaning:
options[0] main data Center
options[1] main rack
options[2] main data node
*/
func Assign(server string, count uint64, replication string, collection string, ttl string, options ...string) (*AssignResult, error) {
	values := make(url.Values)
	values.Add("count", strconv.FormatUint(count, 10))
	if replication != "" {
		values.Add("replication", replication)
	}
	if collection != "" {
		values.Add("collection", collection)
	}
	if ttl != "" {
		values.Add("ttl", ttl)
	}

	var dataCenter, rack, dataNode string
	switch len(options) {
	case 1:
		dataCenter = options[0]
	case 2:
		dataCenter = options[0]
		rack = options[1]
	case 3:
		dataCenter = options[0]
		rack = options[1]
		dataNode = options[2]
	default:
	}

	if dataCenter != "" {
		values.Add("dataCenter", dataCenter)
	}
	if rack != "" {
		values.Add("rack", rack)
	}
	if dataNode != "" {
		values.Add("dataNode", dataNode)
	}

	jsonBlob, err := util.Post("http://"+server+"/dir/assign", values)
	glog.V(2).Info("assign result :", string(jsonBlob))
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
