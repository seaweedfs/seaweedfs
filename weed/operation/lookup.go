package operation

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/url"
	"strings"
	"time"

	"github.com/chrislusf/seaweedfs/weed/util"
)

type Location struct {
	Url       string `json:"url,omitempty"`
	PublicUrl string `json:"publicUrl,omitempty"`
}
type LookupResult struct {
	VolumeId  string     `json:"volumeId,omitempty"`
	Locations []Location `json:"locations,omitempty"`
	Error     string     `json:"error,omitempty"`
}

func (lr *LookupResult) String() string {
	return fmt.Sprintf("VolumeId:%s, Locations:%v, Error:%s", lr.VolumeId, lr.Locations, lr.Error)
}

var (
	vc VidCache // caching of volume locations, re-check if after 10 minutes
)

func Lookup(server string, vid string) (ret *LookupResult, err error) {
	locations, cache_err := vc.Get(vid)
	if cache_err != nil {
		if ret, err = do_lookup(server, vid); err == nil {
			vc.Set(vid, ret.Locations, 10*time.Minute)
		}
	} else {
		ret = &LookupResult{VolumeId: vid, Locations: locations}
	}
	return
}

func do_lookup(server string, vid string) (*LookupResult, error) {
	values := make(url.Values)
	values.Add("volumeId", vid)
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

func LookupFileId(server string, fileId string) (fullUrl string, err error) {
	parts := strings.Split(fileId, ",")
	if len(parts) != 2 {
		return "", errors.New("Invalid fileId " + fileId)
	}
	lookup, lookupError := Lookup(server, parts[0])
	if lookupError != nil {
		return "", lookupError
	}
	if len(lookup.Locations) == 0 {
		return "", errors.New("File Not Found")
	}
	return "http://" + lookup.Locations[rand.Intn(len(lookup.Locations))].Url + "/" + fileId, nil
}

// LookupVolumeIds find volume locations by cache and actual lookup
func LookupVolumeIds(server string, vids []string) (map[string]LookupResult, error) {
	ret := make(map[string]LookupResult)
	var unknown_vids []string

	//check vid cache first
	for _, vid := range vids {
		locations, cache_err := vc.Get(vid)
		if cache_err == nil {
			ret[vid] = LookupResult{VolumeId: vid, Locations: locations}
		} else {
			unknown_vids = append(unknown_vids, vid)
		}
	}
	//return success if all volume ids are known
	if len(unknown_vids) == 0 {
		return ret, nil
	}

	//only query unknown_vids
	values := make(url.Values)
	for _, vid := range unknown_vids {
		values.Add("volumeId", vid)
	}
	jsonBlob, err := util.Post("http://"+server+"/vol/lookup", values)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(jsonBlob, &ret)
	if err != nil {
		return nil, errors.New(err.Error() + " " + string(jsonBlob))
	}

	//set newly checked vids to cache
	for _, vid := range unknown_vids {
		locations := ret[vid].Locations
		vc.Set(vid, locations, 10*time.Minute)
	}

	return ret, nil
}
