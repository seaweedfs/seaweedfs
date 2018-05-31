package weed_server

import (
	"net/http"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"strconv"
	"time"
	"fmt"
)

func (fs *FilerServer) registerHandler(w http.ResponseWriter, r *http.Request) {
	path := r.FormValue("path")
	fileId := r.FormValue("fileId")
	fileSize, err := strconv.ParseUint(r.FormValue("fileSize"), 10, 64)
	if err != nil {
		glog.V(0).Infof("register %s to %s parse fileSize %s: %v", fileId, path, r.FormValue("fileSize"), err)
		writeJsonError(w, r, http.StatusInternalServerError, fmt.Errorf("parsing fileSize: %v", err))
		return
	}
	uid, err := strconv.ParseUint(r.FormValue("uid"), 10, 64)
	if err != nil && r.FormValue("uid") != "" {
		glog.V(0).Infof("register %s to %s parse uid %s: %v", fileId, path, r.FormValue("uid"), err)
		writeJsonError(w, r, http.StatusInternalServerError, fmt.Errorf("parsing uid: %v", err))
		return
	}
	gid, err := strconv.ParseUint(r.FormValue("gid"), 10, 64)
	if err != nil && r.FormValue("gid") != "" {
		glog.V(0).Infof("register %s to %s parse gid %s: %v", fileId, path, r.FormValue("gid"), err)
		writeJsonError(w, r, http.StatusInternalServerError, fmt.Errorf("parsing gid: %v", err))
		return
	}
	mime := r.FormValue("mime")
	entry := &filer2.Entry{
		FullPath: filer2.FullPath(path),
		Attr: filer2.Attr{
			Mode:   0660,
			Crtime: time.Now(),
			Mtime:  time.Now(),
			Uid:    uint32(uid),
			Gid:    uint32(gid),
			Mime:   mime,
		},
		Chunks: []*filer_pb.FileChunk{{
			FileId: fileId,
			Size:   fileSize,
			Mtime:  time.Now().UnixNano(),
		}},
	}
	glog.V(2).Infof("register %s to %s parse fileSize %s", fileId, path, r.FormValue("fileSize"))
	err = fs.filer.CreateEntry(entry)
	if err != nil {
		glog.V(0).Infof("register %s to %s error: %v", fileId, path, err)
		writeJsonError(w, r, http.StatusInternalServerError, fmt.Errorf("create %s: %v", path, err))
	} else {
		w.WriteHeader(http.StatusOK)
	}
}
