package weed_server

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	ui "github.com/seaweedfs/seaweedfs/weed/server/filer_ui"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// listDirectoryHandler lists directories and folders under a directory
// files are sorted by name and paginated via "lastFileName" and "limit".
// sub directories are listed on the first page, when "lastFileName"
// is empty.
func (fs *FilerServer) listDirectoryHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if fs.option.ExposeDirectoryData == false {
		writeJsonError(w, r, http.StatusForbidden, errors.New("ui is disabled"))
		return
	}

	stats.FilerHandlerCounter.WithLabelValues(stats.DirList).Inc()

	path := r.URL.Path
	if strings.HasSuffix(path, "/") && len(path) > 1 {
		path = path[:len(path)-1]
	}

	limit, limitErr := strconv.Atoi(r.FormValue("limit"))
	if limitErr != nil {
		limit = fs.option.DirListingLimit
	}

	lastFileName := r.FormValue("lastFileName")
	namePattern := r.FormValue("namePattern")
	namePatternExclude := r.FormValue("namePatternExclude")

	entries, shouldDisplayLoadMore, err := fs.filer.ListDirectoryEntries(ctx, util.FullPath(path), lastFileName, false, int64(limit), "", namePattern, namePatternExclude)

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
			Version               string
			Path                  string
			Entries               interface{}
			Limit                 int
			LastFileName          string
			ShouldDisplayLoadMore bool
			EmptyFolder           bool
		}{
			util.Version(),
			path,
			entries,
			limit,
			lastFileName,
			shouldDisplayLoadMore,
			emptyFolder,
		})
		return
	}

	err = ui.StatusTpl.Execute(w, struct {
		Version               string
		Path                  string
		Breadcrumbs           []ui.Breadcrumb
		Entries               interface{}
		Limit                 int
		LastFileName          string
		ShouldDisplayLoadMore bool
		EmptyFolder           bool
		ShowDirectoryDelete   bool
	}{
		util.Version(),
		path,
		ui.ToBreadcrumb(path),
		entries,
		limit,
		lastFileName,
		shouldDisplayLoadMore,
		emptyFolder,
		fs.option.ShowUIDirectoryDelete,
	})
	if err != nil {
		glog.V(0).Infof("Template Execute Error: %v", err)
	}

}
