package weed_server

import (
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/util"
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
	if strings.HasSuffix(r.URL.Path, "/") {
		if fs.disableDirListing {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		fs.listDirectoryHandler(w, r)
		return
	}

	fileId, err := fs.filer.FindFile(r.URL.Path)
	if err == leveldb.ErrNotFound {
		glog.V(3).Infoln("Not found in db", r.URL.Path)
		w.WriteHeader(http.StatusNotFound)
		return
	}

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
