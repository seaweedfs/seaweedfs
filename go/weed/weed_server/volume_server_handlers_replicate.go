package weed_server

import (
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/chrislusf/seaweedfs/go/glog"
	"github.com/pierrec/lz4"
)

func (vs *VolumeServer) getVolumeCleanDataHandler(w http.ResponseWriter, r *http.Request) {
	v, e := vs.getVolume("volume", r)
	if v == nil {
		http.Error(w, fmt.Sprintf("Not Found volume: %v", e), http.StatusBadRequest)
		return
	}
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
