package topology

import (
	"github.com/aszxqw/weed-fs/go/storage"
	"github.com/aszxqw/weed-fs/go/util"
	"encoding/json"
	"errors"
	"net/url"
)

type AllocateVolumeResult struct {
	Error string
}

func AllocateVolume(dn *DataNode, vid storage.VolumeId, option *VolumeGrowOption) error {
	values := make(url.Values)
	values.Add("volume", vid.String())
	values.Add("collection", option.Collection)
	values.Add("replication", option.ReplicaPlacement.String())
	values.Add("ttl", option.Ttl.String())
	jsonBlob, err := util.Post("http://"+dn.PublicUrl+"/admin/assign_volume", values)
	if err != nil {
		return err
	}
	var ret AllocateVolumeResult
	if err := json.Unmarshal(jsonBlob, &ret); err != nil {
		return err
	}
	if ret.Error != "" {
		return errors.New(ret.Error)
	}
	return nil
}
