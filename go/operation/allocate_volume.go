package operation

import (
	"code.google.com/p/weed-fs/go/storage"
	"code.google.com/p/weed-fs/go/topology"
	"code.google.com/p/weed-fs/go/util"
	"encoding/json"
	"errors"
	"net/url"
)

type AllocateVolumeResult struct {
	Error string
}

func AllocateVolume(dn *topology.DataNode, vid storage.VolumeId, collection string, repType storage.ReplicationType) error {
	values := make(url.Values)
	values.Add("volume", vid.String())
	values.Add("collection", collection)
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
