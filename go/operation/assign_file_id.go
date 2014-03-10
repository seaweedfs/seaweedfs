package operation

import (
	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/util"
	"encoding/json"
	"errors"
	"net/url"
	"strconv"
)

type AssignResult struct {
	Fid       string `json:"fid"`
	Url       string `json:"url"`
	PublicUrl string `json:"publicUrl"`
	Count     int
	Error     string `json:"error"`
}

func Assign(server string, count int, replication string, collection string) (*AssignResult, error) {
	values := make(url.Values)
	values.Add("count", strconv.Itoa(count))
	if replication != "" {
		values.Add("replication", replication)
	}
	if collection != "" {
		values.Add("collection", collection)
	}
	jsonBlob, err := util.Post("http://"+server+"/dir/assign", values)
	glog.V(2).Info("assign result :", string(jsonBlob))
	if err != nil {
		return nil, err
	}
	var ret AssignResult
	err = json.Unmarshal(jsonBlob, &ret)
	if err != nil {
		return nil, err
	}
	if ret.Count <= 0 {
		return nil, errors.New(ret.Error)
	}
	return &ret, nil
}
