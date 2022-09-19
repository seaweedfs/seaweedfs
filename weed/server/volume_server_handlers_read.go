package weed_server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util/mem"
	"io"
	"mime"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/images"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var fileNameEscaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`)

func (vs *VolumeServer) GetOrHeadHandler(w http.ResponseWriter, r *http.Request) {
	n := new(needle.Needle)
	vid, fid, filename, ext, _ := parseURLPath(r.URL.Path)

	if !vs.maybeCheckJwtAuthorization(r, vid, fid, false) {
		writeJsonError(w, r, http.StatusUnauthorized, errors.New("wrong jwt"))
		return
	}

	volumeId, err := needle.NewVolumeId(vid)
	if err != nil {
		glog.V(2).Infof("parsing vid %s: %v", r.URL.Path, err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	err = n.ParsePath(fid)
	if err != nil {
		glog.V(2).Infof("parsing fid %s: %v", r.URL.Path, err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// glog.V(4).Infoln("volume", volumeId, "reading", n)
	hasVolume := vs.store.HasVolume(volumeId)
	_, hasEcVolume := vs.store.FindEcVolume(volumeId)
	if !hasVolume && !hasEcVolume {
		if vs.ReadMode == "local" {
			glog.V(0).Infoln("volume is not local:", err, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		lookupResult, err := operation.LookupVolumeId(vs.GetMaster, vs.grpcDialOption, volumeId.String())
		glog.V(2).Infoln("volume", volumeId, "found on", lookupResult, "error", err)
		if err != nil || len(lookupResult.Locations) <= 0 {
			glog.V(0).Infoln("lookup error:", err, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if vs.ReadMode == "proxy" {
			// proxy client request to target server
			u, _ := url.Parse(util.NormalizeUrl(lookupResult.Locations[0].Url))
			r.URL.Host = u.Host
			r.URL.Scheme = u.Scheme
			request, err := http.NewRequest("GET", r.URL.String(), nil)
			if err != nil {
				glog.V(0).Infof("failed to instance http request of url %s: %v", r.URL.String(), err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			for k, vv := range r.Header {
				for _, v := range vv {
					request.Header.Add(k, v)
				}
			}

			response, err := client.Do(request)
			if err != nil {
				glog.V(0).Infof("request remote url %s: %v", r.URL.String(), err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			defer util.CloseResponse(response)
			// proxy target response to client
			for k, vv := range response.Header {
				for _, v := range vv {
					w.Header().Add(k, v)
				}
			}
			w.WriteHeader(response.StatusCode)
			buf := mem.Allocate(128 * 1024)
			defer mem.Free(buf)
			io.CopyBuffer(w, response.Body, buf)
			return
		} else {
			// redirect
			u, _ := url.Parse(util.NormalizeUrl(lookupResult.Locations[0].PublicUrl))
			u.Path = fmt.Sprintf("%s/%s,%s", u.Path, vid, fid)
			arg := url.Values{}
			if c := r.FormValue("collection"); c != "" {
				arg.Set("collection", c)
			}
			u.RawQuery = arg.Encode()
			http.Redirect(w, r, u.String(), http.StatusMovedPermanently)
			return
		}
	}
	cookie := n.Cookie

	readOption := &storage.ReadOption{
		ReadDeleted:    r.FormValue("readDeleted") == "true",
		HasSlowRead:    vs.hasSlowRead,
		ReadBufferSize: vs.readBufferSizeMB * 1024 * 1024,
	}

	var count int
	var memoryCost types.Size
	readOption.AttemptMetaOnly, readOption.MustMetaOnly = shouldAttemptStreamWrite(hasVolume, ext, r)
	onReadSizeFn := func(size types.Size) {
		memoryCost = size
		atomic.AddInt64(&vs.inFlightDownloadDataSize, int64(memoryCost))
	}
	if hasVolume {
		count, err = vs.store.ReadVolumeNeedle(volumeId, n, readOption, onReadSizeFn)
	} else if hasEcVolume {
		count, err = vs.store.ReadEcShardNeedle(volumeId, n, onReadSizeFn)
	}
	defer func() {
		atomic.AddInt64(&vs.inFlightDownloadDataSize, -int64(memoryCost))
		vs.inFlightDownloadDataLimitCond.Signal()
	}()

	if err != nil && err != storage.ErrorDeleted && hasVolume {
		glog.V(4).Infof("read needle: %v", err)
		// start to fix it from other replicas, if not deleted and hasVolume and is not a replicated request
	}
	// glog.V(4).Infoln("read bytes", count, "error", err)
	if err != nil || count < 0 {
		glog.V(3).Infof("read %s isNormalVolume %v error: %v", r.URL.Path, hasVolume, err)
		if err == storage.ErrorNotFound || err == storage.ErrorDeleted {
			w.WriteHeader(http.StatusNotFound)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
		return
	}
	if n.Cookie != cookie {
		glog.V(0).Infof("request %s with cookie:%x expected:%x from %s agent %s", r.URL.Path, cookie, n.Cookie, r.RemoteAddr, r.UserAgent())
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if n.LastModified != 0 {
		w.Header().Set("Last-Modified", time.Unix(int64(n.LastModified), 0).UTC().Format(http.TimeFormat))
		if r.Header.Get("If-Modified-Since") != "" {
			if t, parseError := time.Parse(http.TimeFormat, r.Header.Get("If-Modified-Since")); parseError == nil {
				if t.Unix() >= int64(n.LastModified) {
					w.WriteHeader(http.StatusNotModified)
					return
				}
			}
		}
	}
	if inm := r.Header.Get("If-None-Match"); inm == "\""+n.Etag()+"\"" {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	setEtag(w, n.Etag())

	if n.HasPairs() {
		pairMap := make(map[string]string)
		err = json.Unmarshal(n.Pairs, &pairMap)
		if err != nil {
			glog.V(0).Infoln("Unmarshal pairs error:", err)
		}
		for k, v := range pairMap {
			w.Header().Set(k, v)
		}
	}

	if vs.tryHandleChunkedFile(n, filename, ext, w, r) {
		return
	}

	if n.NameSize > 0 && filename == "" {
		filename = string(n.Name)
		if ext == "" {
			ext = filepath.Ext(filename)
		}
	}
	mtype := ""
	if n.MimeSize > 0 {
		mt := string(n.Mime)
		if !strings.HasPrefix(mt, "application/octet-stream") {
			mtype = mt
		}
	}

	if n.IsCompressed() {
		if _, _, _, shouldResize := shouldResizeImages(ext, r); shouldResize {
			if n.Data, err = util.DecompressData(n.Data); err != nil {
				glog.V(0).Infoln("ungzip error:", err, r.URL.Path)
			}
			// } else if strings.Contains(r.Header.Get("Accept-Encoding"), "zstd") && util.IsZstdContent(n.Data) {
			//	w.Header().Set("Content-Encoding", "zstd")
		} else if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") && util.IsGzippedContent(n.Data) {
			w.Header().Set("Content-Encoding", "gzip")
		} else {
			if n.Data, err = util.DecompressData(n.Data); err != nil {
				glog.V(0).Infoln("uncompress error:", err, r.URL.Path)
			}
		}
	}

	if !readOption.IsMetaOnly {
		rs := conditionallyResizeImages(bytes.NewReader(n.Data), ext, r)
		if e := writeResponseContent(filename, mtype, rs, w, r); e != nil {
			glog.V(2).Infoln("response write error:", e)
		}
	} else {
		vs.streamWriteResponseContent(filename, mtype, volumeId, n, w, r, readOption)
	}
}

func shouldAttemptStreamWrite(hasLocalVolume bool, ext string, r *http.Request) (shouldAttempt bool, mustMetaOnly bool) {
	if !hasLocalVolume {
		return false, false
	}
	if len(ext) > 0 {
		ext = strings.ToLower(ext)
	}
	if r.Method == "HEAD" {
		return true, true
	}
	_, _, _, shouldResize := shouldResizeImages(ext, r)
	if shouldResize {
		return false, false
	}
	return true, false
}

func (vs *VolumeServer) tryHandleChunkedFile(n *needle.Needle, fileName string, ext string, w http.ResponseWriter, r *http.Request) (processed bool) {
	if !n.IsChunkedManifest() || r.URL.Query().Get("cm") == "false" {
		return false
	}

	chunkManifest, e := operation.LoadChunkManifest(n.Data, n.IsCompressed())
	if e != nil {
		glog.V(0).Infof("load chunked manifest (%s) error: %v", r.URL.Path, e)
		return false
	}
	if fileName == "" && chunkManifest.Name != "" {
		fileName = chunkManifest.Name
	}

	if ext == "" {
		ext = filepath.Ext(fileName)
	}

	mType := ""
	if chunkManifest.Mime != "" {
		mt := chunkManifest.Mime
		if !strings.HasPrefix(mt, "application/octet-stream") {
			mType = mt
		}
	}

	w.Header().Set("X-File-Store", "chunked")

	chunkedFileReader := operation.NewChunkedFileReader(chunkManifest.Chunks, vs.GetMaster(), vs.grpcDialOption)
	defer chunkedFileReader.Close()

	rs := conditionallyResizeImages(chunkedFileReader, ext, r)

	if e := writeResponseContent(fileName, mType, rs, w, r); e != nil {
		glog.V(2).Infoln("response write error:", e)
	}
	return true
}

func conditionallyResizeImages(originalDataReaderSeeker io.ReadSeeker, ext string, r *http.Request) io.ReadSeeker {
	rs := originalDataReaderSeeker
	if len(ext) > 0 {
		ext = strings.ToLower(ext)
	}
	width, height, mode, shouldResize := shouldResizeImages(ext, r)
	if shouldResize {
		rs, _, _ = images.Resized(ext, originalDataReaderSeeker, width, height, mode)
	}
	return rs
}

func shouldResizeImages(ext string, r *http.Request) (width, height int, mode string, shouldResize bool) {
	if ext == ".png" || ext == ".jpg" || ext == ".jpeg" || ext == ".gif" || ext == ".webp" {
		if r.FormValue("width") != "" {
			width, _ = strconv.Atoi(r.FormValue("width"))
		}
		if r.FormValue("height") != "" {
			height, _ = strconv.Atoi(r.FormValue("height"))
		}
	}
	mode = r.FormValue("mode")
	shouldResize = width > 0 || height > 0
	return
}

func writeResponseContent(filename, mimeType string, rs io.ReadSeeker, w http.ResponseWriter, r *http.Request) error {
	totalSize, e := rs.Seek(0, 2)
	if mimeType == "" {
		if ext := filepath.Ext(filename); ext != "" {
			mimeType = mime.TypeByExtension(ext)
		}
	}
	if mimeType != "" {
		w.Header().Set("Content-Type", mimeType)
	}
	w.Header().Set("Accept-Ranges", "bytes")

	adjustPassthroughHeaders(w, r, filename)

	if r.Method == "HEAD" {
		w.Header().Set("Content-Length", strconv.FormatInt(totalSize, 10))
		return nil
	}

	return processRangeRequest(r, w, totalSize, mimeType, func(writer io.Writer, offset int64, size int64) error {
		if _, e = rs.Seek(offset, 0); e != nil {
			return e
		}
		_, e = io.CopyN(writer, rs, size)
		return e
	})
}

func (vs *VolumeServer) streamWriteResponseContent(filename string, mimeType string, volumeId needle.VolumeId, n *needle.Needle, w http.ResponseWriter, r *http.Request, readOption *storage.ReadOption) {
	totalSize := int64(n.DataSize)
	if mimeType == "" {
		if ext := filepath.Ext(filename); ext != "" {
			mimeType = mime.TypeByExtension(ext)
		}
	}
	if mimeType != "" {
		w.Header().Set("Content-Type", mimeType)
	}
	w.Header().Set("Accept-Ranges", "bytes")
	adjustPassthroughHeaders(w, r, filename)

	if r.Method == "HEAD" {
		w.Header().Set("Content-Length", strconv.FormatInt(totalSize, 10))
		return
	}

	processRangeRequest(r, w, totalSize, mimeType, func(writer io.Writer, offset int64, size int64) error {
		return vs.store.ReadVolumeNeedleDataInto(volumeId, n, readOption, writer, offset, size)
	})

}
