package topology

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"

	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/util"
)

type AllocateVolumeResult struct {
	Error string
}

func AllocateVolume(dn *DataNode, vid storage.VolumeId, option *VolumeGrowOption) error {
	values := make(url.Values)
	values.Add("volume", vid.String())
	values.Add("collection", option.Collection)
	values.Add("ttl", option.Ttl.String())
	jsonBlob, err := util.Post(dn.Url(), "/admin/assign_volume", values)
	if err != nil {
		return err
	}
	var ret AllocateVolumeResult
	if err := json.Unmarshal(jsonBlob, &ret); err != nil {
		return fmt.Errorf("Invalid JSON result for %s: %s", "/admin/assign_volum", string(jsonBlob))
	}
	if ret.Error != "" {
		return errors.New(ret.Error)
	}
	return nil
}
