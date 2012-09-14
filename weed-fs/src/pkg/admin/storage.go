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

func SendVolumeLocationsList(t *topology.Topology, vid storage.VolumeId) error{
//  values := make(url.Values)
//  values.Add("volumeLocationsList", vid.String())
//  volumeLocations:= []map[string]string{}
//  list := t.GetVolumeLocations(vid)
//  m := make(map[string]interface{})
//  m["Vid"] = vid.String()
//  for _, dn := range list {
//    m["Locations"] = append(m["Locations"], dn)
//  }
//  for _, dn := range list {
//    util.Post("http://"+dn.Ip+":"+strconv.Itoa(dn.Port)+"/admin/set_volume_locations_list", values)
//  }
  return nil
}
