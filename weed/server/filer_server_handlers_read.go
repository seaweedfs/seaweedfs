package weed_server

import (
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	ui "github.com/chrislusf/seaweedfs/weed/server/filer_ui"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/chrislusf/seaweedfs/weed/filer2"
)

// listDirectoryHandler lists directories and folers under a directory
// files are sorted by name and paginated via "lastFileName" and "limit".
// sub directories are listed on the first page, when "lastFileName"
// is empty.
func (fs *FilerServer) listDirectoryHandler(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	if strings.HasSuffix(path, "/") && len(path) > 1 {
		path = path[:len(path)-1]
	}

	limit, limit_err := strconv.Atoi(r.FormValue("limit"))
	if limit_err != nil {
		limit = 100
	}

	lastFileName := r.FormValue("lastFileName")

	entries, err := fs.filer.ListDirectoryEntries(filer2.FullPath(path), lastFileName, false, limit)

	if err != nil {
		glog.V(0).Infof("listDirectory %s %s $d: %s", path, lastFileName, limit, err)
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

	args := struct {
		Path                  string
		Breadcrumbs           []ui.Breadcrumb
		Entries               interface{}
		Limit                 int
		LastFileName          string
		ShouldDisplayLoadMore bool
	}{
		path,
		ui.ToBreadcrumb(path),
		entries,
		limit,
		lastFileName,
		shouldDisplayLoadMore,
	}

	if r.Header.Get("Accept") == "application/json" {
		writeJsonQuiet(w, r, http.StatusOK, args)
	} else {
		ui.StatusTpl.Execute(w, args)
	}
}

func (fs *FilerServer) GetOrHeadHandler(w http.ResponseWriter, r *http.Request, isGetMethod bool) {
	path := r.URL.Path
	if strings.HasSuffix(path, "/") && len(path) > 1 {
		path = path[:len(path)-1]
	}

	entry, err := fs.filer.FindEntry(filer2.FullPath(path))
	if err != nil {
		glog.V(1).Infof("Not found %s: %v", path, err)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	if entry.IsDirectory() {
		if fs.disableDirListing {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		fs.listDirectoryHandler(w, r)
		return
	}

	if len(entry.Chunks) == 0 {
		glog.V(1).Infof("no file chunks for %s, attr=%+v", path, entry.Attr)
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// FIXME pick the right fid
	fileId := entry.Chunks[0].FileId

	urlLocation, err := operation.LookupFileId(fs.getMasterNode(), fileId)
	if err != nil {
		glog.V(1).Infoln("operation LookupFileId %s failed, err is %s", fileId, err.Error())
		w.WriteHeader(http.StatusNotFound)
		return
	}
	urlString := urlLocation
	if fs.redirectOnRead {
		http.Redirect(w, r, urlString, http.StatusFound)
		return
	}
	u, _ := url.Parse(urlString)
	q := u.Query()
	for key, values := range r.URL.Query() {
		for _, value := range values {
			q.Add(key, value)
		}
	}
	u.RawQuery = q.Encode()
	request := &http.Request{
		Method:        r.Method,
		URL:           u,
		Proto:         r.Proto,
		ProtoMajor:    r.ProtoMajor,
		ProtoMinor:    r.ProtoMinor,
		Header:        r.Header,
		Body:          r.Body,
		Host:          r.Host,
		ContentLength: r.ContentLength,
	}
	glog.V(3).Infoln("retrieving from", u)
	resp, do_err := util.Do(request)
	if do_err != nil {
		glog.V(0).Infoln("failing to connect to volume server", do_err.Error())
		writeJsonError(w, r, http.StatusInternalServerError, do_err)
		return
	}
	defer resp.Body.Close()
	for k, v := range resp.Header {
		w.Header()[k] = v
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)

}
