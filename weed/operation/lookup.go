package operation

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"math/rand"
	"net/url"
	"strings"
	"time"

	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
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

func Lookup(masterFn GetMasterFn, vid string) (ret *LookupResult, err error) {
	locations, cache_err := vc.Get(vid)
	if cache_err != nil {
		if ret, err = do_lookup(masterFn, vid); err == nil {
			vc.Set(vid, ret.Locations, 10*time.Minute)
		}
	} else {
		ret = &LookupResult{VolumeId: vid, Locations: locations}
	}
	return
}

func do_lookup(masterFn GetMasterFn, vid string) (*LookupResult, error) {
	values := make(url.Values)
	values.Add("volumeId", vid)
	server := masterFn()
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

func LookupFileId(masterFn GetMasterFn, fileId string) (fullUrl string, err error) {
	parts := strings.Split(fileId, ",")
	if len(parts) != 2 {
		return "", errors.New("Invalid fileId " + fileId)
	}
	lookup, lookupError := Lookup(masterFn, parts[0])
	if lookupError != nil {
		return "", lookupError
	}
	if len(lookup.Locations) == 0 {
		return "", errors.New("File Not Found")
	}
	return "http://" + lookup.Locations[rand.Intn(len(lookup.Locations))].Url + "/" + fileId, nil
}

// LookupVolumeIds find volume locations by cache and actual lookup
func LookupVolumeIds(masterFn GetMasterFn, grpcDialOption grpc.DialOption, vids []string) (map[string]LookupResult, error) {
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

	err := WithMasterServerClient(masterFn(), grpcDialOption, func(masterClient master_pb.SeaweedClient) error {

		req := &master_pb.LookupVolumeRequest{
			VolumeIds: unknown_vids,
		}
		resp, grpcErr := masterClient.LookupVolume(context.Background(), req)
		if grpcErr != nil {
			return grpcErr
		}

		//set newly checked vids to cache
		for _, vidLocations := range resp.VolumeIdLocations {
			var locations []Location
			for _, loc := range vidLocations.Locations {
				locations = append(locations, Location{
					Url:       loc.Url,
					PublicUrl: loc.PublicUrl,
				})
			}
			if vidLocations.Error != "" {
				vc.Set(vidLocations.VolumeId, locations, 10*time.Minute)
			}
			ret[vidLocations.VolumeId] = LookupResult{
				VolumeId:  vidLocations.VolumeId,
				Locations: locations,
				Error:     vidLocations.Error,
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return ret, nil
}
