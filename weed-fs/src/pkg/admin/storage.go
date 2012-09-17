package admin

import (
  "pkg/storage"
  "pkg/topology"
)

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
