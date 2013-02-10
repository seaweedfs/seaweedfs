package operation

import (
	"encoding/json"
	"errors"
	_ "fmt"
	"net/url"
	"weed/storage"
	"weed/util"
)

type Location struct {
	Url       string "url"
	PublicUrl string "publicUrl"
}
type LookupResult struct {
	Locations []Location "locations"
	Error     string     "error"
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
