package weed_server

import (
	"bytes"
	"context"
	"io"
	"mime"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/images"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/stats"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func (fs *FilerServer) GetOrHeadHandler(w http.ResponseWriter, r *http.Request, isGetMethod bool) {

	path := r.URL.Path
	isForDirectory := strings.HasSuffix(path, "/")
	if isForDirectory && len(path) > 1 {
		path = path[:len(path)-1]
	}

	entry, err := fs.filer.FindEntry(context.Background(), util.FullPath(path))
	if err != nil {
		if path == "/" {
			fs.listDirectoryHandler(w, r)
			return
		}
		if err == filer_pb.ErrNotFound {
			glog.V(1).Infof("Not found %s: %v", path, err)
			stats.FilerRequestCounter.WithLabelValues("read.notfound").Inc()
			w.WriteHeader(http.StatusNotFound)
		} else {
			glog.V(0).Infof("Internal %s: %v", path, err)
			stats.FilerRequestCounter.WithLabelValues("read.internalerror").Inc()
			w.WriteHeader(http.StatusInternalServerError)
		}
		return
	}

	if entry.IsDirectory() {
		if fs.option.DisableDirListing {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		fs.listDirectoryHandler(w, r)
		return
	}

	if isForDirectory {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	if len(entry.Chunks) == 0 {
		glog.V(1).Infof("no file chunks for %s, attr=%+v", path, entry.Attr)
		stats.FilerRequestCounter.WithLabelValues("read.nocontent").Inc()
		w.WriteHeader(http.StatusNoContent)
		return
	}

	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("Last-Modified", entry.Attr.Mtime.Format(http.TimeFormat))

	// mime type
	mimeType := entry.Attr.Mime
	if mimeType == "" {
		if ext := filepath.Ext(entry.Name()); ext != "" {
			mimeType = mime.TypeByExtension(ext)
		}
	}
	if mimeType != "" {
		w.Header().Set("Content-Type", mimeType)
	}

	// if modified since
	if !entry.Attr.Mtime.IsZero() {
		w.Header().Set("Last-Modified", entry.Attr.Mtime.UTC().Format(http.TimeFormat))
		if r.Header.Get("If-Modified-Since") != "" {
			if t, parseError := time.Parse(http.TimeFormat, r.Header.Get("If-Modified-Since")); parseError == nil {
				if t.After(entry.Attr.Mtime) {
					w.WriteHeader(http.StatusNotModified)
					return
				}
			}
		}
	}

	// set etag
	etag := filer2.ETagEntry(entry)
	if inm := r.Header.Get("If-None-Match"); inm == "\""+etag+"\"" {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	setEtag(w, etag)

	if r.Method == "HEAD" {
		w.Header().Set("Content-Length", strconv.FormatInt(int64(filer2.TotalSize(entry.Chunks)), 10))
		return
	}

	filename := entry.Name()
	adjustHeadersAfterHEAD(w, r, filename)

	totalSize := int64(filer2.TotalSize(entry.Chunks))

	if rangeReq := r.Header.Get("Range"); rangeReq == "" {
		ext := filepath.Ext(filename)
		width, height, mode, shouldResize := shouldResizeImages(ext, r)
		if shouldResize {
			data, err := filer2.ReadAll(fs.filer.MasterClient, entry.Chunks)
			if err != nil {
				glog.Errorf("failed to read %s: %v", path, err)
				w.WriteHeader(http.StatusNotModified)
				return
			}
			rs, _, _ := images.Resized(ext, bytes.NewReader(data), width, height, mode)
			io.Copy(w, rs)
			return
		}
	}

	processRangeRequest(r, w, totalSize, mimeType, func(writer io.Writer, offset int64, size int64) error {
		return filer2.StreamContent(fs.filer.MasterClient, writer, entry.Chunks, offset, size)
	})

}
