package weed_server

import (
	"encoding/json"
	"net/http"

	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
)

func (fs *FilerServer) apiHandler(w http.ResponseWriter, r *http.Request) {
	request := r.FormValue("request")
	apiRequest := filer.ApiRequest{}

	err := json.Unmarshal([]byte(request), &apiRequest)
	if err != nil {
		glog.V(0).Infoln("failing to read request", r.RequestURI, request)
		writeJsonError(w, r, http.StatusInternalServerError, err)
	}
	switch apiRequest.Command {
	case "listDirectories":
		res := filer.ListDirectoriesResult{}
		res.Directories, err = fs.filer.ListDirectories(apiRequest.Directory)
		if err != nil {
			res.Error = err.Error()
		}
		writeJsonQuiet(w, r, http.StatusOK, res)
	case "listFiles":
		res := filer.ListFilesResult{}
		res.Files, err = fs.filer.ListFiles(apiRequest.Directory, apiRequest.FileName, 100)
		if err != nil {
			res.Error = err.Error()
		}
		writeJsonQuiet(w, r, http.StatusOK, res)
	}
}
