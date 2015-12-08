package weed_server

import (
	"fmt"
	"net/http"
	"strconv"
	"github.com/chrislusf/seaweedfs/go/glog"
	"io"
	"github.com/pierrec/lz4"
)

func (vs *VolumeServer) getVolumeCleanDataHandler(w http.ResponseWriter, r *http.Request) {
	v, err := vs.getVolume("volume", r)
	if v == nil {
		http.Error(w, fmt.Sprintf("Not Found volume: %v", err), http.StatusBadRequest)
		return
	}
	cr, e := v.GetVolumeCleanReader()
	if e != nil {
		http.Error(w, fmt.Sprintf("Get volume clean reader: %v", err), http.StatusInternalServerError)
		return
	}
	totalSize, e := cr.Size()
	if e != nil {
		http.Error(w, fmt.Sprintf("Get volume size: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Content-Encoding", "lz4")
	lz4w := lz4.NewWriter(w)
	defer lz4w.Close()
	rangeReq := r.Header.Get("Range")
	if rangeReq == "" {
		w.Header().Set("Content-Length", strconv.FormatInt(totalSize, 10))
		if _, e = io.Copy(lz4w, cr); e != nil {
			glog.V(4).Infoln("response write error:", e)
		}
		return
	}
	ranges, err := parseRange(rangeReq, totalSize)
	if err != nil {
		http.Error(w, err.Error(), http.StatusRequestedRangeNotSatisfiable)
		return
	}
	if len(ranges) != 1 {
		http.Error(w, "Only support one range", http.StatusNotImplemented)
		return
	}
	ra := ranges[0]
	if _, e := cr.Seek(ra.start, 0); e != nil {
		http.Error(w, fmt.Sprintf("Seek: %v", err), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Length", strconv.FormatInt(ra.length, 10))
	w.Header().Set("Content-Range", ra.contentRange(totalSize))
	w.WriteHeader(http.StatusPartialContent)
	if _, e = io.CopyN(lz4w, cr, ra.length); e != nil {
		glog.V(2).Infoln("response write error:", e)
	}
}
