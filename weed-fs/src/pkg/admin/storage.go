package admin

import (
  "encoding/json"
  "errors"
  "strconv"
  "net/url"
  "pkg/util"
  "pkg/storage"
  "pkg/topology"
)

type AllocateVolumeResult struct {
  Error string
}

func AllocateVolume(dn *topology.DataNode, vid storage.VolumeId, repType storage.ReplicationType) error{
  values := make(url.Values)
  values.Add("volume", vid.String())
  values.Add("replicationType", repType.String())
  jsonBlob := util.Post("http://"+dn.Ip+":"+strconv.Itoa(dn.Port)+"/admin/assign_volume", values)
  var ret AllocateVolumeResult
  err := json.Unmarshal(jsonBlob, &ret)
  if err != nil {
    return err
  }
  if ret.Error != "" {
    return errors.New(ret.Error)
  }
  return nil
}
