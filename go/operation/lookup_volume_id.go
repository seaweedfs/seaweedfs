package operation

import (
	"encoding/json"
	"errors"
	_ "fmt"
	"net/url"
	"code.google.com/p/weed-fs/go/storage"
	"code.google.com/p/weed-fs/go/util"
)

type Location struct {
	Url       string `json:"url"`
	PublicUrl string `json:"publicUrl"`
}
type LookupResult struct {
	Locations []Location `json:"locations"`
	Error     string     `json:"error"`
}

//TODO: Add a caching for vid here
func Lookup(server string, vid storage.VolumeId) (*LookupResult, error) {
	values := make(url.Values)
	values.Add("volumeId", vid.String())
	jsonBlob, err := util.Post("http://"+server+"/dir/lookup", values)
	if err != nil {
		return nil, err
	}
	var ret LookupResult
	err = json.Unmarshal(jsonBlob, &ret)
	if err != nil {
		return nil, err
	}
	if ret.Error != "" {
		return nil, errors.New(ret.Error)
	}
	return &ret, nil
}
