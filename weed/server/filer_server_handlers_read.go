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

	"github.com/chrislusf/seaweedfs/weed/filer"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/images"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	xhttp "github.com/chrislusf/seaweedfs/weed/s3api/http"
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

	if len(entry.Chunks) == 0 && len(entry.Content) == 0 {
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

	// print out the header from extended properties
	for k, v := range entry.Extended {
		w.Header().Set(k, string(v))
	}

	//set tag count
	if r.Method == "GET" {
		tagCount := 0
		for k := range entry.Extended {
			if strings.HasPrefix(k, xhttp.AmzObjectTagging+"-") {
				tagCount++
			}
		}
		if tagCount > 0 {
			w.Header().Set(xhttp.AmzTagCount, strconv.Itoa(tagCount))
		}
	}

	// set etag
	etag := filer.ETagEntry(entry)
	if inm := r.Header.Get("If-None-Match"); inm == "\""+etag+"\"" {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	setEtag(w, etag)

	filename := entry.Name()
	adjustHeaderContentDisposition(w, r, filename)

	totalSize := int64(entry.Size())

	if r.Method == "HEAD" {
		w.Header().Set("Content-Length", strconv.FormatInt(totalSize, 10))
		return
	}

	if rangeReq := r.Header.Get("Range"); rangeReq == "" {
		ext := filepath.Ext(filename)
		width, height, mode, shouldResize := shouldResizeImages(ext, r)
		if shouldResize {
			data, err := filer.ReadAll(fs.filer.MasterClient, entry.Chunks)
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
		if offset+size <= int64(len(entry.Content)) {
			_, err := writer.Write(entry.Content[offset : offset+size])
			return err
		}
		return filer.StreamContent(fs.filer.MasterClient, writer, entry.Chunks, offset, size)
	})

}
