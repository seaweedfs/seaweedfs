package topology

import (
	"encoding/json"
	"errors"
  "fmt"
	"net/url"
	"pkg/storage"
	"pkg/util"
	"time"
)

func (t *Topology) Vacuum() int {
	total_counter := 0
	for _, vl := range t.replicaType2VolumeLayout {
		if vl != nil {
			for vid, locationlist := range vl.vid2location {
				each_volume_counter := 0
				vl.removeFromWritable(vid)
				ch := make(chan int, locationlist.Length())
				for _, dn := range locationlist.list {
					go func(url string, vid storage.VolumeId) {
						vacuumVolume_Compact(url, vid)
					}(dn.Url(), vid)
				}
				for _ = range locationlist.list {
					select {
					case count := <-ch:
						each_volume_counter += count
					case <-time.After(30 * time.Minute):
						each_volume_counter = 0
						break
					}
				}
				if each_volume_counter > 0 {
					for _, dn := range locationlist.list {
						if e := vacuumVolume_Commit(dn.Url(), vid); e != nil {
							fmt.Println("Error when committing on", dn.Url(), e)
							panic(e)
						}
					}
					vl.setVolumeWritable(vid)
					total_counter += each_volume_counter
				}
			}
		}
	}
	return 0
}

type VacuumVolumeResult struct {
	Bytes int
	Error string
}

func vacuumVolume_Compact(urlLocation string, vid storage.VolumeId) (error, int) {
	values := make(url.Values)
	values.Add("volume", vid.String())
	jsonBlob, err := util.Post("http://"+urlLocation+"/admin/vacuum_volume_compact", values)
	if err != nil {
		return err, 0
	}
	var ret VacuumVolumeResult
	if err := json.Unmarshal(jsonBlob, &ret); err != nil {
		return err, 0
	}
	if ret.Error != "" {
		return errors.New(ret.Error), 0
	}
	return nil, ret.Bytes
}
func vacuumVolume_Commit(urlLocation string, vid storage.VolumeId) error {
	values := make(url.Values)
	values.Add("volume", vid.String())
	jsonBlob, err := util.Post("http://"+urlLocation+"/admin/vacuum_volume_commit", values)
	if err != nil {
		return err
	}
	var ret VacuumVolumeResult
	if err := json.Unmarshal(jsonBlob, &ret); err != nil {
		return err
	}
	if ret.Error != "" {
		return errors.New(ret.Error)
	}
	return nil
}
