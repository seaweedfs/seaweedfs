package weed_server

import (
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/chrislusf/seaweedfs/go/glog"
	"github.com/chrislusf/seaweedfs/go/storage"
	"github.com/chrislusf/seaweedfs/go/util"
	"github.com/pierrec/lz4"
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

func (vs *VolumeServer) getVolumeCleanDataHandler(w http.ResponseWriter, r *http.Request) {
	v, e := vs.getVolume("volume", r)
	if v == nil {
		http.Error(w, fmt.Sprintf("Not Found volume: %v", e), http.StatusBadRequest)
		return
	}
	//set read only when replicating
	v.SetReadOnly(true)
	defer v.SetReadOnly(false)
	cr, e := v.GetVolumeCleanReader()
	if e != nil {
		http.Error(w, fmt.Sprintf("Get volume clean reader: %v", e), http.StatusInternalServerError)
		return
	}
	totalSize, e := cr.Size()
	if e != nil {
		http.Error(w, fmt.Sprintf("Get volume size: %v", e), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`filename="%d.dat.lz4"`, v.Id))

	rangeReq := r.Header.Get("Range")
	if rangeReq == "" {
		w.Header().Set("Content-Length", strconv.FormatInt(totalSize, 10))
		w.Header().Set("Content-Encoding", "lz4")
		lz4w := lz4.NewWriter(w)
		if _, e = io.Copy(lz4w, cr); e != nil {
			glog.V(4).Infoln("response write error:", e)
		}
		lz4w.Close()
		return
	}
	ranges, e := parseRange(rangeReq, totalSize)
	if e != nil {
		http.Error(w, e.Error(), http.StatusRequestedRangeNotSatisfiable)
		return
	}
	if len(ranges) != 1 {
		http.Error(w, "Only support one range", http.StatusNotImplemented)
		return
	}
	ra := ranges[0]
	if _, e := cr.Seek(ra.start, 0); e != nil {
		http.Error(w, e.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Length", strconv.FormatInt(ra.length, 10))
	w.Header().Set("Content-Range", ra.contentRange(totalSize))
	w.Header().Set("Content-Encoding", "lz4")
	w.WriteHeader(http.StatusPartialContent)
	lz4w := lz4.NewWriter(w)
	if _, e = io.CopyN(lz4w, cr, ra.length); e != nil {
		glog.V(2).Infoln("response write error:", e)
	}
	lz4w.Close()
}
