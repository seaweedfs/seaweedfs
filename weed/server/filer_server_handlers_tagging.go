package weed_server

import (
	"net/http"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// add or replace one file Seaweed- prefixed attributes
// curl -X PUT -H "Seaweed-Name1: value1" http://localhost:8888/path/to/a/file?tagging
func (fs *FilerServer) PutTaggingHandler(w http.ResponseWriter, r *http.Request) {

	ctx := r.Context()

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

	if dbErr := fs.filer.CreateEntry(ctx, existingEntry, false, false, nil, false, fs.filer.MaxFilenameLength); dbErr != nil {
		glog.V(0).Infof("failing to update %s tagging : %v", path, dbErr)
		writeJsonError(w, r, http.StatusInternalServerError, dbErr)
		return
	}

	writeJsonQuiet(w, r, http.StatusAccepted, nil)
	return
}

// remove all Seaweed- prefixed attributes
// curl -X DELETE http://localhost:8888/path/to/a/file?tagging
func (fs *FilerServer) DeleteTaggingHandler(w http.ResponseWriter, r *http.Request) {

	ctx := r.Context()

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

	// parse out tags to be deleted
	toDelete := strings.Split(r.URL.Query().Get("tagging"), ",")
	deletions := make(map[string]struct{})
	for _, deletion := range toDelete {
		if deletion != "" {
			deletions[deletion] = struct{}{}
		}
	}

	// delete all tags or specific tags
	hasDeletion := false
	for header, _ := range existingEntry.Extended {
		if strings.HasPrefix(header, needle.PairNamePrefix) {
			if len(deletions) == 0 {
				delete(existingEntry.Extended, header)
				hasDeletion = true
			} else {
				tag := header[len(needle.PairNamePrefix):]
				if _, found := deletions[tag]; found {
					delete(existingEntry.Extended, header)
					hasDeletion = true
				}
			}
		}
	}

	if !hasDeletion {
		writeJsonQuiet(w, r, http.StatusNotModified, nil)
		return
	}

	if dbErr := fs.filer.CreateEntry(ctx, existingEntry, false, false, nil, false, fs.filer.MaxFilenameLength); dbErr != nil {
		glog.V(0).Infof("failing to delete %s tagging : %v", path, dbErr)
		writeJsonError(w, r, http.StatusInternalServerError, dbErr)
		return
	}

	writeJsonQuiet(w, r, http.StatusAccepted, nil)
	return
}
