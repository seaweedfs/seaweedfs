package operation

import (
	"encoding/json"
	"errors"
	"net/url"
	"weed/storage"
	"weed/topology"
	"weed/util"
)

type AllocateVolumeResult struct {
	Error string
}

func AllocateVolume(dn *topology.DataNode, vid storage.VolumeId, repType storage.ReplicationType) error {
	values := make(url.Values)
	values.Add("volume", vid.String())
	values.Add("replicationType", repType.String())
	jsonBlob, err := util.Post("http://"+dn.Url()+"/admin/assign_volume", values)
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
