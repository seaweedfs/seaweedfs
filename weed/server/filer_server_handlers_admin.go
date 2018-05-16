package weed_server

import (
	"net/http"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/filer2"
	"strconv"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
)

func (fs *FilerServer) registerHandler(w http.ResponseWriter, r *http.Request) {
	path := r.FormValue("path")
	fileId := r.FormValue("fileId")
	fileSize, err := strconv.ParseUint(r.FormValue("fileSize"), 10, 64)
	if err != nil {
		glog.V(4).Infof("register %s to %s parse fileSize %s: %v", fileId, path, r.FormValue("fileSize"), err)
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	}
	entry := &filer2.Entry{
		FullPath: filer2.FullPath(path),
		Attr: filer2.Attr{
			Mode: 0660,
		},
		Chunks: []*filer_pb.FileChunk{{
			FileId: fileId,
			Size:   fileSize,
		}},
	}
	err = fs.filer.CreateEntry(entry)
	if err != nil {
		glog.V(4).Infof("register %s to %s error: %v", fileId, path, err)
		writeJsonError(w, r, http.StatusInternalServerError, err)
	} else {
		w.WriteHeader(http.StatusOK)
	}
}
