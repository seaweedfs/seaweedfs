package weed_server

import (
	"context"
	"encoding/base64"
	"fmt"
	"github.com/skip2/go-qrcode"
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

	entries, err := fs.filer.ListDirectoryEntries(context.Background(), util.FullPath(path), lastFileName, false, limit, namePattern)

	if err != nil {
		glog.V(0).Infof("listDirectory %s %s %d: %s", path, lastFileName, limit, err)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	shouldDisplayLoadMore := len(entries) == limit
	if path == "/" {
		path = ""
	}

	if len(entries) > 0 {
		lastFileName = entries[len(entries)-1].Name()
	}

	glog.V(4).Infof("listDirectory %s, last file %s, limit %d: %d items", path, lastFileName, limit, len(entries))

	if r.Header.Get("Accept") == "application/json" {
		writeJsonQuiet(w, r, http.StatusOK, struct {
			Path                  string
			Entries               interface{}
			Limit                 int
			LastFileName          string
			ShouldDisplayLoadMore bool
		}{
			path,
			entries,
			limit,
			lastFileName,
			shouldDisplayLoadMore,
		})
		return
	}

	var qrImageString string
	img, err := qrcode.Encode(fmt.Sprintf("http://%s:%d%s", fs.option.Host, fs.option.Port, r.URL.Path), qrcode.Medium, 128)
	if err == nil {
		qrImageString = base64.StdEncoding.EncodeToString(img)
	}

	ui.StatusTpl.Execute(w, struct {
		Path                  string
		Breadcrumbs           []ui.Breadcrumb
		Entries               interface{}
		Limit                 int
		LastFileName          string
		ShouldDisplayLoadMore bool
		QrImage               string
	}{
		path,
		ui.ToBreadcrumb(path),
		entries,
		limit,
		lastFileName,
		shouldDisplayLoadMore,
		qrImageString,
	})
}
