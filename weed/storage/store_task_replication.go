package storage

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"

	"github.com/chrislusf/seaweedfs/weed/util"
)

type ReplicaTask struct {
	VID         VolumeId
	Collection  string
	SrcDataNode string
	s           *Store
	location    *DiskLocation
}

func NewReplicaTask(s *Store, args url.Values) (*ReplicaTask, error) {
	volumeIdString := args.Get("volume")
	vid, err := NewVolumeId(volumeIdString)
	if err != nil {
		return nil, fmt.Errorf("Volume Id %s is not a valid unsigned integer", volumeIdString)
	}
	source := args.Get("source")
	if source == "" {
		return nil, errors.New("Invalid source data node.")

	}
	location := s.findFreeLocation()
	if location == nil {
		return nil, errors.New("No more free space left")
	}
	collection := args.Get("collection")
	return &ReplicaTask{
		VID:         vid,
		Collection:  collection,
		SrcDataNode: source,
		s:           s,
		location:    location,
	}, nil
}

func (t *ReplicaTask) Run() error {
	ch := make(chan error)
	go func() {
		idxUrl := util.MkUrl(t.SrcDataNode, "/admin/sync/index", url.Values{"volume": {t.VID.String()}})
		e := util.DownloadToFile(idxUrl, t.FileName()+".repx")
		if e != nil {
			e = fmt.Errorf("Replicat error: %s, %v", idxUrl, e)
		}
		ch <- e
	}()
	go func() {
		datUrl := util.MkUrl(t.SrcDataNode, "/admin/sync/vol_data", url.Values{"volume": {t.VID.String()}})
		e := util.DownloadToFile(datUrl, t.FileName()+".repd")
		if e != nil {
			e = fmt.Errorf("Replicat error: %s, %v", datUrl, e)
		}
		ch <- e
	}()
	errs := make([]error, 0)
	for i := 0; i < 2; i++ {
		if e := <-ch; e != nil {
			errs = append(errs, e)
		}
	}
	if len(errs) == 0 {
		return nil
	} else {
		return fmt.Errorf("%v", errs)
	}
}

func (t *ReplicaTask) Commit() error {
	var (
		volume *Volume
		e      error
	)

	if e = os.Rename(t.FileName()+".repd", t.FileName()+".dat"); e != nil {
		return e
	}
	if e = os.Rename(t.FileName()+".repx", t.FileName()+".idx"); e != nil {
		return e
	}
	volume, e = NewVolume(t.location.Directory, t.Collection, t.VID, t.s.needleMapKind, nil)
	if e == nil {
		t.location.AddVolume(t.VID, volume)
		t.s.SendHeartbeatToMaster(nil)
	}
	return e
}

func (t *ReplicaTask) Clean() error {
	os.Remove(t.FileName() + ".repx")
	os.Remove(t.FileName() + ".repd")
	return nil
}

func (t *ReplicaTask) Info() url.Values {
	//TODO
	return url.Values{}
}

func (t *ReplicaTask) FileName() (fileName string) {
	if t.Collection == "" {
		fileName = path.Join(t.location.Directory, t.VID.String())
	} else {
		fileName = path.Join(t.location.Directory, t.Collection+"_"+t.VID.String())
	}
	return
}
