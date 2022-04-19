package weed_server

import (
	"context"
	"net/http"
	"strconv"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/glog"
	ui "github.com/chrislusf/seaweedfs/weed/server/filer_ui"
	"github.com/chrislusf/seaweedfs/weed/stats"
	"github.com/chrislusf/seaweedfs/weed/util"
)

// listDirectoryHandler lists directories and folers under a directory
// files are sorted by name and paginated via "lastFileName" and "limit".
// sub directories are listed on the first page, when "lastFileName"
// is empty.
func (fs *FilerServer) listDirectoryHandler(w http.ResponseWriter, r *http.Request) {

	stats.FilerRequestCounter.WithLabelValues("list").Inc()

	path := r.URL.Path
	if strings.HasSuffix(path, "/") && len(path) > 1 {
		path = path[:len(path)-1]
	}

	limit, limit_err := strconv.Atoi(r.FormValue("limit"))
	if limit_err != nil {
		limit = 100
	}

	lastFileName := r.FormValue("lastFileName")
	namePattern := r.FormValue("namePattern")
	namePatternExclude := r.FormValue("namePatternExclude")

	entries, shouldDisplayLoadMore, err := fs.filer.ListDirectoryEntries(context.Background(), util.FullPath(path), lastFileName, false, int64(limit), "", namePattern, namePatternExclude)

	if err != nil {
		glog.V(0).Infof("listDirectory %s %s %d: %s", path, lastFileName, limit, err)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	if path == "/" {
		path = ""
	}

	emptyFolder := true
	if len(entries) > 0 {
		lastFileName = entries[len(entries)-1].Name()
		emptyFolder = false
	}

	glog.V(4).Infof("listDirectory %s, last file %s, limit %d: %d items", path, lastFileName, limit, len(entries))

	if r.Header.Get("Accept") == "application/json" {
		writeJsonQuiet(w, r, http.StatusOK, struct {
			Path                  string
			Entries               interface{}
			Limit                 int
			LastFileName          string
			ShouldDisplayLoadMore bool
			EmptyFolder           bool
		}{
			path,
			entries,
			limit,
			lastFileName,
			shouldDisplayLoadMore,
			emptyFolder,
		})
		return
	}

	ui.StatusTpl.Execute(w, struct {
		Path                  string
		Breadcrumbs           []ui.Breadcrumb
		Entries               interface{}
		Limit                 int
		LastFileName          string
		ShouldDisplayLoadMore bool
		EmptyFolder           bool
	}{
		path,
		ui.ToBreadcrumb(path),
		entries,
		limit,
		lastFileName,
		shouldDisplayLoadMore,
		emptyFolder,
	})
}
