package weed_server

import (
	"context"
	"net/http"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/util"
)

// add or replace one file Seaweed- prefixed attributes
// curl -X PUT -H "Seaweed-Name1: value1" http://localhost:8888/path/to/a/file?tagging
func (fs *FilerServer) PutTaggingHandler(w http.ResponseWriter, r *http.Request) {

	ctx := context.Background()

	path := r.URL.Path
	if strings.HasSuffix(path, "/") {
		path = path[:len(path)-1]
	}

	existingEntry, err := fs.filer.FindEntry(ctx, util.FullPath(path))
	if err != nil {
		writeJsonError(w, r, http.StatusNotFound, err)
		return
	}
	if existingEntry == nil {
		writeJsonError(w, r, http.StatusNotFound, err)
		return
	}

	if existingEntry.Extended == nil {
		existingEntry.Extended = make(map[string][]byte)
	}

	for header, values := range r.Header {
		if strings.HasPrefix(header, needle.PairNamePrefix) {
			for _, value := range values {
				existingEntry.Extended[header] = []byte(value)
			}
		}
	}

	if dbErr := fs.filer.CreateEntry(ctx, existingEntry, false, false, nil); dbErr != nil {
		glog.V(0).Infof("failing to update %s tagging : %v", path, dbErr)
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	}

	writeJsonQuiet(w, r, http.StatusAccepted, nil)
	return
}

// remove all Seaweed- prefixed attributes
// curl -X DELETE http://localhost:8888/path/to/a/file?tagging
func (fs *FilerServer) DeleteTaggingHandler(w http.ResponseWriter, r *http.Request) {

	ctx := context.Background()

	path := r.URL.Path
	if strings.HasSuffix(path, "/") {
		path = path[:len(path)-1]
	}

	existingEntry, err := fs.filer.FindEntry(ctx, util.FullPath(path))
	if err != nil {
		writeJsonError(w, r, http.StatusNotFound, err)
		return
	}
	if existingEntry == nil {
		writeJsonError(w, r, http.StatusNotFound, err)
		return
	}

	if existingEntry.Extended == nil {
		existingEntry.Extended = make(map[string][]byte)
	}

	hasDeletion := false
	for header, _ := range existingEntry.Extended {
		if strings.HasPrefix(header, needle.PairNamePrefix) {
			delete(existingEntry.Extended, header)
			hasDeletion = true
		}
	}

	if !hasDeletion {
		writeJsonQuiet(w, r, http.StatusNotModified, nil)
		return
	}

	if dbErr := fs.filer.CreateEntry(ctx, existingEntry, false, false, nil); dbErr != nil {
		glog.V(0).Infof("failing to delete %s tagging : %v", path, dbErr)
		writeJsonError(w, r, http.StatusInternalServerError, err)
		return
	}

	writeJsonQuiet(w, r, http.StatusAccepted, nil)
	return
}
