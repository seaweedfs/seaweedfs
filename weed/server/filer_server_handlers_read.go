package weed_server

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/glog"
	ui "github.com/chrislusf/seaweedfs/weed/server/filer_ui"
	"github.com/syndtr/goleveldb/leveldb"
)

func (fs *FilerServer) GetOrHeadHandler(w http.ResponseWriter, r *http.Request, isGetMethod bool) {
	if !strings.HasSuffix(r.URL.Path, "/") {
		return
	}

	if fs.disableDirListing {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	limit, limit_err := strconv.Atoi(r.FormValue("limit"))
	if limit_err != nil {
		limit = 100
	}

	lastFileName := r.FormValue("lastFileName")
	files, err := fs.filer.ListFiles(r.URL.Path, lastFileName, limit)

	if err == leveldb.ErrNotFound {
		glog.V(0).Infof("Error %s", err)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	directories, err2 := fs.filer.ListDirectories(r.URL.Path)
	if err2 == leveldb.ErrNotFound {
		glog.V(0).Infof("Error %s", err)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	shouldDisplayLoadMore := len(files) > 0

	lastFileName = ""
	if len(files) > 0 {
		lastFileName = files[len(files)-1].Name

		files2, err3 := fs.filer.ListFiles(r.URL.Path, lastFileName, limit)
		if err3 == leveldb.ErrNotFound {
			glog.V(0).Infof("Error %s", err)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		shouldDisplayLoadMore = len(files2) > 0
	}

	args := struct {
		Path                  string
		Files                 interface{}
		Directories           interface{}
		Limit                 int
		LastFileName          string
		ShouldDisplayLoadMore bool
	}{
		r.URL.Path,
		files,
		directories,
		limit,
		lastFileName,
		shouldDisplayLoadMore,
	}
	ui.StatusTpl.Execute(w, args)
}
