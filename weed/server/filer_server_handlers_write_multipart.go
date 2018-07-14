package weed_server

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage"
)

func (fs *FilerServer) multipartUploadAnalyzer(w http.ResponseWriter, r *http.Request, replication, collection string, dataCenter string) (fileId, urlLocation string, err error) {
	//Default handle way for http multipart
	if r.Method == "PUT" {
		buf, _ := ioutil.ReadAll(r.Body)
		r.Body = ioutil.NopCloser(bytes.NewBuffer(buf))
		fileName, _, _, _, _, _, _, _, pe := storage.ParseUpload(r)
		if pe != nil {
			glog.V(0).Infoln("failing to parse post body", pe.Error())
			writeJsonError(w, r, http.StatusInternalServerError, pe)
			err = pe
			return
		}
		//reconstruct http request body for following new request to volume server
		r.Body = ioutil.NopCloser(bytes.NewBuffer(buf))

		path := r.URL.Path
		if strings.HasSuffix(path, "/") {
			if fileName != "" {
				path += fileName
			}
		}
		fileId, urlLocation, err = fs.queryFileInfoByPath(w, r, path)
	} else {
		fileId, urlLocation, err = fs.assignNewFileInfo(w, r, replication, collection, dataCenter)
	}
	return
}
