package operation

import (
	"github.com/chrislusf/weed-fs/go/util"
	"encoding/json"
	"errors"
	_ "fmt"
	"math/rand"
	"net/url"
	"strings"
	"time"
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

var (
	vc VidCache
)

func Lookup(server string, vid string) (ret *LookupResult, err error) {
	locations, cache_err := vc.Get(vid)
	if cache_err != nil {
		if ret, err = do_lookup(server, vid); err == nil {
			vc.Set(vid, ret.Locations, 1*time.Minute)
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
	return "http://" + lookup.Locations[rand.Intn(len(lookup.Locations))].PublicUrl + "/" + fileId, nil
}

func LookupVolumeIds(server string, vids []string) (map[string]LookupResult, error) {
	values := make(url.Values)
	for _, vid := range vids {
		values.Add("volumeId", vid)
	}
	jsonBlob, err := util.Post("http://"+server+"/vol/lookup", values)
	if err != nil {
		return nil, err
	}
	ret := make(map[string]LookupResult)
	err = json.Unmarshal(jsonBlob, &ret)
	if err != nil {
		return nil, errors.New(err.Error() + " " + string(jsonBlob))
	}
	return ret, nil
}
