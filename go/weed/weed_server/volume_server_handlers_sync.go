package weed_server

import (
	"fmt"
	"net/http"

	"github.com/chrislusf/seaweedfs/go/glog"
	"github.com/chrislusf/seaweedfs/go/storage"
	"github.com/chrislusf/seaweedfs/go/util"
)

func (vs *VolumeServer) getVolumeSyncStatusHandler(w http.ResponseWriter, r *http.Request) {
	v, err := vs.getVolume("volume", r)
	if v == nil {
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}
	syncStat := v.GetVolumeSyncStatus()
	if syncStat.Error != "" {
		writeJsonError(w, r, http.StatusInternalServerError, fmt.Errorf("Get Volume %d status error: %s", v.Id, syncStat.Error))
		glog.V(2).Infoln("getVolumeSyncStatusHandler volume =", r.FormValue("volume"), ", error =", err)
	} else {
		writeJsonQuiet(w, r, http.StatusOK, syncStat)
	}
}

func (vs *VolumeServer) getVolumeIndexContentHandler(w http.ResponseWriter, r *http.Request) {
	v, err := vs.getVolume("volume", r)
	if v == nil {
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}
	content, err := v.IndexFileContent()
	if err != nil {
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	}
	w.Write(content)
}

func (vs *VolumeServer) getVolumeDataContentHandler(w http.ResponseWriter, r *http.Request) {
	v, err := vs.getVolume("volume", r)
	if v == nil {
		writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("Not Found volume: %v", err))
		return
	}
	if int(v.SuperBlock.CompactRevision) != util.ParseInt(r.FormValue("revision"), 0) {
		writeJsonError(w, r, http.StatusExpectationFailed, fmt.Errorf("Requested Volume Revision is %s, but current revision is %d", r.FormValue("revision"), v.SuperBlock.CompactRevision))
		return
	}
	offset := uint32(util.ParseUint64(r.FormValue("offset"), 0))
	size := uint32(util.ParseUint64(r.FormValue("size"), 0))
	content, err := storage.ReadNeedleBlob(v.DataFile(), int64(offset)*storage.NeedlePaddingSize, size)
	if err != nil {
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	}

	id := util.ParseUint64(r.FormValue("id"), 0)
	n := new(storage.Needle)
	n.ParseNeedleHeader(content)
	if id != n.Id {
		writeJsonError(w, r, http.StatusNotFound, fmt.Errorf("Expected file entry id %d, but found %d", id, n.Id))
		return
	}

	w.Write(content)
}

func (vs *VolumeServer) getVolume(volumeParameterName string, r *http.Request) (*storage.Volume, error) {
	volumeIdString := r.FormValue(volumeParameterName)
	if volumeIdString == "" {
		err := fmt.Errorf("Empty Volume Id: Need to pass in %s=the_volume_id.", volumeParameterName)
		return nil, err
	}
	vid, err := storage.NewVolumeId(volumeIdString)
	if err != nil {
		err = fmt.Errorf("Volume Id %s is not a valid unsigned integer", volumeIdString)
		return nil, err
	}
	v := vs.store.GetVolume(vid)
	if v == nil {
		return nil, fmt.Errorf("Not Found Volume Id %s: %d", volumeIdString, vid)
	}
	return v, nil
}
