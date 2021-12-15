package weed_server

import (
	"bytes"
	"context"
	"fmt"
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

func (fs *FilerServer) GetOrHeadHandler(w http.ResponseWriter, r *http.Request) {

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
			glog.Errorf("Internal %s: %v", path, err)
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

	// set etag
	etag := filer.ETagEntry(entry)
	if ifm := r.Header.Get("If-Match"); ifm != "" && (ifm != "\""+etag+"\"" && ifm != etag) {
		w.WriteHeader(http.StatusPreconditionFailed)
		return
	}

	w.Header().Set("Accept-Ranges", "bytes")

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
				if !t.Before(entry.Attr.Mtime) {
					w.WriteHeader(http.StatusNotModified)
					return
				}
			}
		}
	}

	// print out the header from extended properties
	for k, v := range entry.Extended {
		if !strings.HasPrefix(k, "xattr-") {
			// "xattr-" prefix is set in filesys.XATTR_PREFIX
			w.Header().Set(k, string(v))
		}
	}

	//Seaweed custom header are not visible to Vue or javascript
	seaweedHeaders := []string{}
	for header := range w.Header() {
		if strings.HasPrefix(header, "Seaweed-") {
			seaweedHeaders = append(seaweedHeaders, header)
		}
	}
	seaweedHeaders = append(seaweedHeaders, "Content-Disposition")
	w.Header().Set("Access-Control-Expose-Headers", strings.Join(seaweedHeaders, ","))

	//set tag count
	tagCount := 0
	for k := range entry.Extended {
		if strings.HasPrefix(k, xhttp.AmzObjectTagging+"-") {
			tagCount++
		}
	}
	if tagCount > 0 {
		w.Header().Set(xhttp.AmzTagCount, strconv.Itoa(tagCount))
	}

	if inm := r.Header.Get("If-None-Match"); inm == "\""+etag+"\"" {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	setEtag(w, etag)

	filename := entry.Name()
	adjustPassthroughHeaders(w, r, filename)

	totalSize := int64(entry.Size())

	if r.Method == "HEAD" {
		w.Header().Set("Content-Length", strconv.FormatInt(totalSize, 10))
		return
	}

	if rangeReq := r.Header.Get("Range"); rangeReq == "" {
		ext := filepath.Ext(filename)
		if len(ext) > 0 {
			ext = strings.ToLower(ext)
		}
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
			if err != nil {
				glog.Errorf("failed to write entry content: %v", err)
			}
			return err
		}
		chunks := entry.Chunks
		if entry.IsInRemoteOnly() {
			dir, name := entry.FullPath.DirAndName()
			if resp, err := fs.CacheRemoteObjectToLocalCluster(context.Background(), &filer_pb.CacheRemoteObjectToLocalClusterRequest{
				Directory: dir,
				Name:      name,
			}); err != nil {
				glog.Errorf("CacheRemoteObjectToLocalCluster %s: %v", entry.FullPath, err)
				return fmt.Errorf("cache %s: %v", entry.FullPath, err)
			} else {
				chunks = resp.Entry.Chunks
			}
		}

		err = filer.StreamContent(fs.filer.MasterClient, writer, chunks, offset, size)
		if err != nil {
			glog.Errorf("failed to stream content %s: %v", r.URL, err)
		}
		return err
	})
}
