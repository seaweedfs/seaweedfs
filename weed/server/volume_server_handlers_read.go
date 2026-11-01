package weed_server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"mime"
	"net/http"
	"net/url"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
	"github.com/seaweedfs/seaweedfs/weed/util/mem"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/images"
	"github.com/seaweedfs/seaweedfs/weed/operation"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

const reqIsProxied = "proxied"

func NotFound(w http.ResponseWriter) {
	stats.VolumeServerFileReadFailures.Inc()
	stats.VolumeServerHandlerCounter.WithLabelValues(stats.ErrorGetNotFound).Inc()
	w.WriteHeader(http.StatusNotFound)
}

func InternalError(w http.ResponseWriter, r *http.Request, err error) {
	stats.VolumeServerFileReadFailures.Inc()
	stats.VolumeServerHandlerCounter.WithLabelValues(stats.ErrorGetInternal).Inc()
	if err == nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if r.Method == http.MethodHead {
		glog.V(0).Infof("response method:%s URL:%s with httpStatus:%d and error:%v",
			r.Method, redactedURL(r), http.StatusInternalServerError, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	writeJsonError(w, r, http.StatusInternalServerError, err)
}

// hopByHopHeaders names the fields that describe a single hop and must not
// be relayed: the fixed hop-by-hop set plus every field the header's own
// Connection values nominate (RFC 9110 7.6.1).
func hopByHopHeaders(h http.Header, extra ...string) map[string]bool {
	drop := map[string]bool{
		"Connection": true, "Keep-Alive": true, "Te": true, "Trailer": true,
		"Upgrade": true, "Proxy-Connection": true, "Proxy-Authenticate": true, "Proxy-Authorization": true,
	}
	for _, v := range h.Values("Connection") {
		for _, token := range strings.Split(v, ",") {
			if name := http.CanonicalHeaderKey(strings.TrimSpace(token)); name != "" {
				drop[name] = true
			}
		}
	}
	for _, name := range extra {
		drop[http.CanonicalHeaderKey(name)] = true
	}
	return drop
}

func (vs *VolumeServer) proxyReqToTargetServer(w http.ResponseWriter, r *http.Request) {
	vid, fid, _, _, _ := parseURLPath(r.URL.Path)
	volumeId, err := needle.NewVolumeId(vid)
	if err != nil {
		glog.V(2).Infof("parsing vid %s: %v", r.URL.Path, err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	lookupResult, err := operation.LookupVolumeIdWithContext(r.Context(), vs.GetMaster, vs.grpcDialOption, volumeId.String())
	if err != nil && vs.ReadMode == "proxy" {
		if r.Context().Err() != nil {
			return
		}
		glog.V(0).InfofCtx(r.Context(), "lookup error: %v %s", err, r.URL.Path)
		InternalError(w, r, nil)
		return
	}
	if err != nil || lookupResult == nil || len(lookupResult.Locations) <= 0 {
		glog.V(0).Infoln("lookup error:", err, r.URL.Path)
		stats.VolumeServerFileReadInvalidNeedles.Inc()
		NotFound(w)
		return
	}
	if len(lookupResult.Locations) >= 2 {
		// cached locations are shared between lookups, so shuffle a copy
		locations := slices.Clone(lookupResult.Locations)
		rand.Shuffle(len(locations), func(i, j int) {
			locations[i], locations[j] = locations[j], locations[i]
		})
		lookupResult.Locations = locations
	}
	var targetUrl *url.URL
	location := fmt.Sprintf("%s:%d", vs.store.Ip, vs.store.Port)
	for _, loc := range lookupResult.Locations {
		if !strings.Contains(loc.Url, location) {
			rawURL, _ := util_http.NormalizeUrl(loc.Url)
			targetUrl, _ = url.Parse(rawURL)
			break
		}
	}
	if targetUrl == nil {
		stats.VolumeServerFileReadInvalidNeedles.Inc()
		stats.VolumeServerHandlerCounter.WithLabelValues(stats.EmptyReadProxyLoc).Inc()
		glog.Errorf("failed lookup target host is empty locations: %+v, %s", lookupResult.Locations, location)
		NotFound(w)
		return
	}
	if vs.ReadMode == "proxy" {
		stats.VolumeServerHandlerCounter.WithLabelValues(stats.ReadProxyReq).Inc()
		// proxy client request to target server
		r.URL.Host = targetUrl.Host
		r.URL.Scheme = targetUrl.Scheme
		query := r.URL.Query()
		query.Set(reqIsProxied, "true")
		r.URL.RawQuery = query.Encode()
		request, err := http.NewRequestWithContext(r.Context(), r.Method, r.URL.String(), nil)
		if err != nil {
			glog.V(0).Infof("failed to instance http request of url %s: %v", redactedURL(r), err)
			InternalError(w, r, nil)
			return
		}
		// the proxied request carries no body, so it must not forward this
		// hop's framing either
		hopByHop := hopByHopHeaders(r.Header, "Host", "Content-Length", "Transfer-Encoding")
		for k, vv := range r.Header {
			if hopByHop[http.CanonicalHeaderKey(k)] {
				continue
			}
			for _, v := range vv {
				request.Header.Add(k, v)
			}
		}

		response, err := util_http.GetGlobalHttpClient().Do(request)
		if err != nil {
			if r.Context().Err() != nil {
				return
			}
			stats.VolumeServerHandlerCounter.WithLabelValues(stats.FailedReadProxyReq).Inc()
			glog.V(0).Infof("request remote url %s: %v", redactedURL(r), err)
			InternalError(w, r, nil)
			return
		}
		defer util_http.CloseResponse(response)
		// proxy target response to client
		respHopByHop := hopByHopHeaders(response.Header)
		for k, vv := range response.Header {
			if k == "Server" || respHopByHop[http.CanonicalHeaderKey(k)] {
				continue
			}
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
		stats.VolumeServerHandlerCounter.WithLabelValues(stats.ReadRedirectReq).Inc()
		targetUrl.Path = fmt.Sprintf("%s/%s,%s", targetUrl.Path, vid, fid)
		arg := url.Values{}
		if c := r.FormValue("collection"); c != "" {
			arg.Set("collection", c)
		}
		arg.Set(reqIsProxied, "true")
		targetUrl.RawQuery = arg.Encode()
		http.Redirect(w, r, targetUrl.String(), http.StatusMovedPermanently)
		return
	}
}

// notModified checks If-Modified-Since only when there is no If-None-Match
// (RFC 9110 13.2.2).
func notModified(r *http.Request, etag string, lastModified uint64) bool {
	if inm := r.Header.Values("If-None-Match"); len(inm) != 0 {
		return etagListMatches(strings.Join(inm, ","), etag, true)
	}
	ims := r.Header.Get("If-Modified-Since")
	if ims == "" || lastModified == 0 {
		return false
	}
	t, err := time.Parse(http.TimeFormat, ims)
	return err == nil && t.Unix() >= int64(lastModified)
}

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
		if vs.ReadMode == "local" || r.URL.Query().Get(reqIsProxied) == "true" {
			stats.VolumeServerFileReadInvalidNeedles.Inc()
			glog.V(0).Infoln("volume is not local:", err, r.URL.Path)
			NotFound(w)
			return
		}
		vs.proxyReqToTargetServer(w, r)
		return
	}
	cookie := n.Cookie

	readOption := &storage.ReadOption{
		ReadDeleted:    r.FormValue("readDeleted") == "true",
		HasSlowRead:    vs.hasSlowRead,
		ReadBufferSize: vs.readBufferSizeMB * 1024 * 1024,
	}

	var count int
	var memoryCost types.Size
	readOption.AttemptMetaOnly, readOption.MustMetaOnly, readOption.HasRange = shouldAttemptStreamWrite(hasVolume, ext, r)
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
		if vs.concurrentDownloadLimit != 0 {
			vs.inFlightDownloadDataLimitCond.Broadcast()
		}
	}()

	if err != nil && err != storage.ErrorDeleted && hasVolume {
		glog.V(4).Infof("read needle: %v", err)
		// start to fix it from other replicas, if not deleted and hasVolume and is not a replicated request
	}
	// glog.V(4).Infoln("read bytes", count, "error", err)
	if err != nil || count < 0 {
		glog.V(3).Infof("read %s isNormalVolume %v error: %v", r.URL.Path, hasVolume, err)
		invalid_needle := err == storage.ErrorNotFound || errors.Is(err, erasure_coding.NotFoundError)
		if invalid_needle {
			stats.VolumeServerFileReadInvalidNeedles.Inc()
		}
		if invalid_needle || err == storage.ErrorDeleted {
			NotFound(w)
		} else {
			InternalError(w, r, err)
		}
		return
	}
	if n.Cookie != cookie {
		// A meta-only read ended the request; record its success.
		if readOption.IsMetaOnly {
			if v := vs.store.GetVolume(volumeId); v != nil {
				v.NoteMetaOnlySuccess()
			}
		}
		glog.V(0).Infof("request %s with cookie:%x expected:%x from %s agent %s", r.URL.Path, cookie, n.Cookie, r.RemoteAddr, r.UserAgent())
		NotFound(w)
		return
	}
	if n.LastModified != 0 {
		w.Header().Set("Last-Modified", time.Unix(int64(n.LastModified), 0).UTC().Format(http.TimeFormat))
	}
	SetEtag(w, n.Etag())
	if notModified(r, n.Etag(), n.LastModified) {
		if readOption.IsMetaOnly {
			if v := vs.store.GetVolume(volumeId); v != nil {
				v.NoteMetaOnlySuccess()
			}
		}
		w.WriteHeader(http.StatusNotModified)
		return
	}

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
		_, _, _, shouldResize := shouldResizeImages(ext, r)
		_, _, _, _, shouldCrop := shouldCropImages(ext, r)
		if shouldResize || shouldCrop {
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
		rs := conditionallyCropImages(bytes.NewReader(n.Data), ext, r)
		rs = conditionallyResizeImages(rs, ext, r)
		if e := writeResponseContent(filename, mtype, rs, w, r); e != nil {
			glog.V(2).Infoln("response write error:", e)
		}
	} else {
		vs.streamWriteResponseContent(filename, mtype, volumeId, n, w, r, readOption)
	}
}

// shouldAttemptStreamWrite picks the ReadOption flags that let a GET or HEAD
// skip the payload read: AttemptMetaOnly, MustMetaOnly and HasRange.
func shouldAttemptStreamWrite(hasLocalVolume bool, ext string, r *http.Request) (shouldAttempt bool, mustMetaOnly bool, hasRange bool) {
	if !hasLocalVolume {
		return false, false, false
	}
	if len(ext) > 0 {
		ext = strings.ToLower(ext)
	}
	_, _, _, shouldResize := shouldResizeImages(ext, r)
	_, _, _, _, shouldCrop := shouldCropImages(ext, r)
	if shouldResize || shouldCrop {
		// A transformed image's length is not the needle's; the payload is
		// needed to answer — also true for HEAD, which reports it.
		return false, false, false
	}
	if r.Method == http.MethodHead {
		// Attempt only, so a needle up to PagedReadLimit is still read whole:
		// on a spinning disk one read is cheaper than a header read followed
		// by a trailer read.
		return true, false, false
	}
	if ext == "" {
		// The stored name may still make this an image to resize or crop,
		// which needs the payload.
		_, _, _, resizeIfImage := shouldResizeImages(".jpg", r)
		_, _, _, _, cropIfImage := shouldCropImages(".jpg", r)
		if resizeIfImage || cropIfImage {
			return true, false, false
		}
	}
	// A range streams only its bytes, so a small needle is not read whole.
	return true, false, r.Header.Get("Range") != ""
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

	chunkedFileReader := operation.NewChunkedFileReader(chunkManifest.Chunks, vs.GetMaster(context.Background()), vs.grpcDialOption)
	defer chunkedFileReader.Close()

	rs := conditionallyCropImages(chunkedFileReader, ext, r)
	rs = conditionallyResizeImages(rs, ext, r)

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

func conditionallyCropImages(originalDataReaderSeeker io.ReadSeeker, ext string, r *http.Request) io.ReadSeeker {
	rs := originalDataReaderSeeker
	if len(ext) > 0 {
		ext = strings.ToLower(ext)
	}
	x1, y1, x2, y2, shouldCrop := shouldCropImages(ext, r)
	if shouldCrop {
		var err error
		rs, err = images.Cropped(ext, rs, x1, y1, x2, y2)
		if err != nil {
			glog.Errorf("Cropping images error: %s", err)
		}
	}
	return rs
}

func shouldCropImages(ext string, r *http.Request) (x1, y1, x2, y2 int, shouldCrop bool) {
	if ext == ".png" || ext == ".jpg" || ext == ".jpeg" || ext == ".gif" {
		if r.FormValue("crop_x1") != "" {
			x1, _ = strconv.Atoi(r.FormValue("crop_x1"))
		}
		if r.FormValue("crop_y1") != "" {
			y1, _ = strconv.Atoi(r.FormValue("crop_y1"))
		}
		if r.FormValue("crop_x2") != "" {
			x2, _ = strconv.Atoi(r.FormValue("crop_x2"))
		}
		if r.FormValue("crop_y2") != "" {
			y2, _ = strconv.Atoi(r.FormValue("crop_y2"))
		}
	}
	shouldCrop = x1 >= 0 && y1 >= 0 && x2 > x1 && y2 > y1
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

	AdjustPassthroughHeaders(w, r, filename)

	if r.Method == http.MethodHead {
		w.Header().Set("Content-Length", strconv.FormatInt(totalSize, 10))
		return nil
	}

	return ProcessRangeRequest(r, w, totalSize, mimeType, func(offset int64, size int64) (filer.DoStreamContent, error) {
		return func(writer io.Writer) error {
			if _, e = rs.Seek(offset, 0); e != nil {
				return e
			}
			_, e = io.CopyN(writer, rs, size)
			return e
		}, nil
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
	AdjustPassthroughHeaders(w, r, filename)

	if r.Method == http.MethodHead {
		// HEAD ends at the metadata read; record its success.
		if v := vs.store.GetVolume(volumeId); v != nil {
			v.NoteMetaOnlySuccess()
		}
		w.Header().Set("Content-Length", strconv.FormatInt(totalSize, 10))
		return
	}

	ProcessRangeRequest(r, w, totalSize, mimeType, func(offset int64, size int64) (filer.DoStreamContent, error) {
		return func(writer io.Writer) error {
			return vs.store.ReadVolumeNeedleDataInto(volumeId, n, readOption, writer, offset, size)
		}, nil
	})

}
