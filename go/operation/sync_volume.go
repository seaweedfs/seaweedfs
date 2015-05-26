package operation

import (
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/chrislusf/seaweedfs/go/glog"
	"github.com/chrislusf/seaweedfs/go/util"
)

type SyncVolumeResponse struct {
	Replication     string `json:"Replication,omitempty"`
	Ttl             string `json:"Ttl,omitempty"`
	TailOffset      uint64 `json:"TailOffset,omitempty"`
	CompactRevision uint16 `json:"CompactRevision,omitempty"`
	IdxFileSize     uint64 `json:"IdxFileSize,omitempty"`
	Error           string `json:"error,omitempty"`
}

func GetVolumeSyncStatus(server string, vid string) (*SyncVolumeResponse, error) {
	values := make(url.Values)
	values.Add("volume", vid)
	jsonBlob, err := util.Post("http://"+server+"/admin/sync/status", values)
	glog.V(2).Info("sync volume result :", string(jsonBlob))
	if err != nil {
		return nil, err
	}
	var ret SyncVolumeResponse
	err = json.Unmarshal(jsonBlob, &ret)
	if err != nil {
		return nil, err
	}
	if ret.Error != "" {
		return nil, fmt.Errorf("Volume %s get sync status error: %s", vid, ret.Error)
	}
	return &ret, nil
}

func GetVolumeIdxEntries(server string, vid string, eachEntryFn func(key uint64, offset, size uint32)) error {
	values := make(url.Values)
	values.Add("volume", vid)
	line := make([]byte, 16)
	err := util.GetBufferStream("http://"+server+"/admin/sync/index", values, line, func(bytes []byte) {
		key := util.BytesToUint64(bytes[:8])
		offset := util.BytesToUint32(bytes[8:12])
		size := util.BytesToUint32(bytes[12:16])
		eachEntryFn(key, offset, size)
	})
	if err != nil {
		return err
	}
	return nil
}
