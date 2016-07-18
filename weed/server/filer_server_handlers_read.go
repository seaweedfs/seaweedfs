package weed_server

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/glog"
	ui "github.com/chrislusf/seaweedfs/weed/server/filer_ui"
	"github.com/syndtr/goleveldb/leveldb"
)

// listDirectoryHandler lists directories and folers under a directory
// files are sorted by name and paginated via "lastFileName" and "limit".
// sub directories are listed on the first page, when "lastFileName"
// is empty.
func (fs *FilerServer) listDirectoryHandler(w http.ResponseWriter, r *http.Request) {
	if !strings.HasSuffix(r.URL.Path, "/") {
		return
	}
	dirlist, err := fs.filer.ListDirectories(r.URL.Path)
	if err == leveldb.ErrNotFound {
		glog.V(3).Infoln("Directory Not Found in db", r.URL.Path)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	m := make(map[string]interface{})
	m["Directory"] = r.URL.Path
	lastFileName := r.FormValue("lastFileName")
	if lastFileName == "" {
		m["Subdirectories"] = dirlist
	}
	limit, limit_err := strconv.Atoi(r.FormValue("limit"))
	if limit_err != nil {
		limit = 100
	}
	m["Files"], _ = fs.filer.ListFiles(r.URL.Path, lastFileName, limit)
	writeJsonQuiet(w, r, http.StatusOK, m)
}

func (fs *FilerServer) GetOrHeadHandler(w http.ResponseWriter, r *http.Request, isGetMethod bool) {
	fileLimit := 100
	files, err := fs.filer.ListFiles(r.URL.Path, "", fileLimit)

	if err == leveldb.ErrNotFound {
		glog.V(0).Infof("Error %s", err)
		return
	}

	directories, err2 := fs.filer.ListDirectories(r.URL.Path)
	if err2 == leveldb.ErrNotFound {
		glog.V(0).Infof("Error %s", err)
		return
	}

	args := struct {
		Path                 string
		Files                interface{}
		Directories          interface{}
		NotAllFilesDisplayed bool
	}{
		r.URL.Path,
		files,
		directories,
		len(files) == fileLimit,
	}
	ui.StatusTpl.Execute(w, args)
}
