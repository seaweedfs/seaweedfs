package operation

import (
  "encoding/json"
  "net/url"
  "pkg/storage"
  "pkg/util"
  "fmt"
)

type Location struct {
  Url       string "url"
  PublicUrl string "publicUrl"
}
type LookupResult struct {
  Locations []Location "locations"
  Error     string "error"
}

func Lookup(server string, vid storage.VolumeId) (*LookupResult, error) {
  values := make(url.Values)
  values.Add("volumeId", vid.String())
  jsonBlob, err := util.Post("http://"+server+"/dir/lookup", values)
  fmt.Println("Lookup Result:", string(jsonBlob))
  if err != nil {
    return nil, err
  }
  var ret LookupResult
  err = json.Unmarshal(jsonBlob, &ret)
  if err != nil {
    return nil, err
  }
  return &ret, nil
}
