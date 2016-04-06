package weedserver

import (
	"bytes"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"net/url"

	"io/ioutil"

	"encoding/json"
	"errors"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/images"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/util"
)

var fileNameEscaper = strings.NewReplacer("\\", "\\\\", "\"", "\\\"")

func (vs *VolumeServer) GetOrHeadHandler(w http.ResponseWriter, r *http.Request) {
	n := new(storage.Needle)
	vid, nid, filename, ext, _ := parseURLPath(r.URL.Path)
	volumeId, err := storage.NewVolumeId(vid)
	if err != nil {
		glog.V(2).Infoln("parsing error:", err, r.URL.Path)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	err = n.ParseNid(nid)
	if err != nil {
		glog.V(2).Infoln("parsing fid error:", err, r.URL.Path)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	glog.V(4).Infoln("volume", volumeId, "reading", n)
	if vs.store.HasVolume(volumeId) {
		cookie := n.Cookie
		count, e := vs.store.ReadVolumeNeedle(volumeId, n)
		glog.V(4).Infoln("read local bytes", count, "error", e)
		if e != nil || count <= 0 {
			glog.V(0).Infoln("read local error:", e, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if n.Cookie != cookie {
			glog.V(0).Infoln("request", r.URL.Path, "with unmaching cookie seen:", cookie, "expected:", n.Cookie, "from", r.RemoteAddr, "agent", r.UserAgent())
			w.WriteHeader(http.StatusNotFound)
			return
		}
	} else if vs.ReadRemoteNeedle {
		count, e := vs.readRemoteNeedle(volumeId.String(), n, r.FormValue("collection"))
		glog.V(4).Infoln("read remote needle bytes", count, "error", e)
		if e != nil || count <= 0 {
			glog.V(2).Infoln("read remote needle error:", e, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
	} else if vs.ReadRedirect {
		lookupResult, err := operation.Lookup(vs.GetMasterNode(), volumeId.String(), r.FormValue("collection"))
		glog.V(2).Infoln("volume", volumeId, "found on", lookupResult, "error", err)
		if err == nil && len(lookupResult.Locations) > 0 {
			u, _ := url.Parse(util.NormalizeUrl(lookupResult.Locations.PickForRead().PublicUrl))
			u.Path = r.URL.Path
			http.Redirect(w, r, u.String(), http.StatusMovedPermanently)
		} else {
			glog.V(2).Infoln("lookup error:", err, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
		return
	} else {
		glog.V(2).Infoln("volume is not local:", err, r.URL.Path)
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
	etag := n.Etag()
	if inm := r.Header.Get("If-None-Match"); inm == etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.Header().Set("Etag", etag)

	if vs.tryHandleChunkedFile(volumeId, n, filename, w, r) {
		return
	}

	if n.NameSize > 0 && filename == "" {
		filename = string(n.Name)
		if ext == "" {
			ext = path.Ext(filename)
		}
	}
	mtype := ""
	if n.MimeSize > 0 {
		mt := string(n.Mime)
		if !strings.HasPrefix(mt, "application/octet-stream") {
			mtype = mt
		}
	}

	if ext != ".gz" {
		if n.IsGzipped() {
			if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
				w.Header().Set("Content-Encoding", "gzip")
			} else {
				if n.Data, err = operation.UnGzipData(n.Data); err != nil {
					glog.V(0).Infoln("ungzip error:", err, r.URL.Path)
				}
			}
		}
	}
	if ext == ".png" || ext == ".jpg" || ext == ".gif" {
		width, height := 0, 0
		if r.FormValue("width") != "" {
			width, _ = strconv.Atoi(r.FormValue("width"))
		}
		if r.FormValue("height") != "" {
			height, _ = strconv.Atoi(r.FormValue("height"))
		}
		n.Data, _, _ = images.Resized(ext, n.Data, width, height)
	}

	if e := writeResponseContent(filename, mtype, bytes.NewReader(n.Data), w, r); e != nil {
		glog.V(2).Infoln("response write error:", e)
	}
}

func (vs *VolumeServer) tryHandleChunkedFile(vid storage.VolumeId, n *storage.Needle, fileName string, w http.ResponseWriter, r *http.Request) (processed bool) {
	if !n.IsChunkedManifest() {
		return false
	}

	chunkManifest, e := operation.LoadChunkManifest(n.Data, n.IsGzipped())
	if e != nil {
		glog.V(0).Infof("load chunked manifest (%s) error: %v", r.URL.Path, e)
		return false
	}
	if fileName == "" && chunkManifest.Name != "" {
		fileName = chunkManifest.Name
	}
	mType := ""
	if chunkManifest.Mime != "" {
		mt := chunkManifest.Mime
		if !strings.HasPrefix(mt, "application/octet-stream") {
			mType = mt
		}
	}

	w.Header().Set("X-File-Store", "chunked")

	chunkedFileReader := &storage.ChunkedFileReader{
		Manifest: chunkManifest,
		Master:   vs.GetMasterNode(),
		Store:    vs.store,
	}
	if v := vs.store.GetVolume(vid); v != nil {
		chunkedFileReader.Collection = v.Collection
	}
	defer chunkedFileReader.Close()
	if e := writeResponseContent(fileName, mType, chunkedFileReader, w, r); e != nil {
		glog.V(2).Infoln("response write error:", e)
	}
	return true
}

func writeResponseContent(filename, mimeType string, rs io.ReadSeeker, w http.ResponseWriter, r *http.Request) error {
	totalSize, e := rs.Seek(0, 2)
	if mimeType == "" {
		if ext := path.Ext(filename); ext != "" {
			mimeType = mime.TypeByExtension(ext)
		}
	}
	if mimeType != "" {
		w.Header().Set("Content-Type", mimeType)
	}
	if filename != "" {
		w.Header().Set("Content-Disposition", `filename="`+fileNameEscaper.Replace(filename)+`"`)
	}
	w.Header().Set("Accept-Ranges", "bytes")
	if r.Method == "HEAD" {
		w.Header().Set("Content-Length", strconv.FormatInt(totalSize, 10))
		return nil
	}
	rangeReq := r.Header.Get("Range")
	if rangeReq == "" {
		w.Header().Set("Content-Length", strconv.FormatInt(totalSize, 10))
		if _, e = rs.Seek(0, 0); e != nil {
			return e
		}
		_, e = io.Copy(w, rs)
		return e
	}

	//the rest is dealing with partial content request
	//mostly copy from src/pkg/net/http/fs.go
	ranges, err := parseRange(rangeReq, totalSize)
	if err != nil {
		http.Error(w, err.Error(), http.StatusRequestedRangeNotSatisfiable)
		return nil
	}
	if sumRangesSize(ranges) > totalSize {
		// The total number of bytes in all the ranges
		// is larger than the size of the file by
		// itself, so this is probably an attack, or a
		// dumb client.  Ignore the range request.
		return nil
	}
	if len(ranges) == 0 {
		return nil
	}
	if len(ranges) == 1 {
		// RFC 2616, Section 14.16:
		// "When an HTTP message includes the content of a single
		// range (for example, a response to a request for a
		// single range, or to a request for a set of ranges
		// that overlap without any holes), this content is
		// transmitted with a Content-Range header, and a
		// Content-Length header showing the number of bytes
		// actually transferred.
		// ...
		// A response to a request for a single range MUST NOT
		// be sent using the multipart/byteranges media type."
		ra := ranges[0]
		w.Header().Set("Content-Length", strconv.FormatInt(ra.length, 10))
		w.Header().Set("Content-Range", ra.contentRange(totalSize))
		w.WriteHeader(http.StatusPartialContent)
		if _, e = rs.Seek(ra.start, 0); e != nil {
			return e
		}

		_, e = io.CopyN(w, rs, ra.length)
		return e
	}
	// process multiple ranges
	for _, ra := range ranges {
		if ra.start > totalSize {
			http.Error(w, "Out of Range", http.StatusRequestedRangeNotSatisfiable)
			return nil
		}
	}
	sendSize := rangesMIMESize(ranges, mimeType, totalSize)
	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)
	w.Header().Set("Content-Type", "multipart/byteranges; boundary="+mw.Boundary())
	sendContent := pr
	defer pr.Close() // cause writing goroutine to fail and exit if CopyN doesn't finish.
	go func() {
		for _, ra := range ranges {
			part, e := mw.CreatePart(ra.mimeHeader(mimeType, totalSize))
			if e != nil {
				pw.CloseWithError(e)
				return
			}
			if _, e = rs.Seek(ra.start, 0); e != nil {
				pw.CloseWithError(e)
				return
			}
			if _, e = io.CopyN(part, rs, ra.length); e != nil {
				pw.CloseWithError(e)
				return
			}
		}
		mw.Close()
		pw.Close()
	}()
	if w.Header().Get("Content-Encoding") == "" {
		w.Header().Set("Content-Length", strconv.FormatInt(sendSize, 10))
	}
	w.WriteHeader(http.StatusPartialContent)
	_, e = io.CopyN(w, sendContent, sendSize)
	return e
}

func (vs *VolumeServer) readRemoteNeedle(vid string, n *storage.Needle, collection string) (int, error) {
	lookupResult, err := operation.Lookup(vs.GetMasterNode(), vid, collection)
	glog.V(2).Infoln("volume", vid, "found on", lookupResult, "error", err)
	if err != nil || len(lookupResult.Locations) == 0 {
		return 0, errors.New("lookup error:" + err.Error())
	}
	u, _ := url.Parse(util.NormalizeUrl(lookupResult.Locations.PickForRead().Url))
	u.Path = "/admin/sync/needle"
	args := url.Values{
		"volume": {vid},
		"nid":    {n.Nid()},
	}
	u.RawQuery = args.Encode()
	req, _ := http.NewRequest("GET", u.String(), nil)
	resp, err := util.HttpDo(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	var buf []byte
	if buf, err = ioutil.ReadAll(resp.Body); err != nil {
		return 0, err
	}
	if resp.StatusCode != http.StatusOK {
		errMsg := strconv.Itoa(resp.StatusCode)
		m := map[string]string{}
		if e := json.Unmarshal(buf, &m); e == nil {
			if s, ok := m["error"]; ok {
				errMsg += ", " + s
			}
		}
		return 0, errors.New(errMsg)
	}
	n.Data = buf
	n.DataSize = uint32(len(n.Data))
	if h := resp.Header.Get("Seaweed-Flags"); h != "" {
		if i, err := strconv.ParseInt(h, 16, 64); err == nil {
			n.Flags = byte(i)
		}
	}
	if h := resp.Header.Get("Seaweed-Checksum"); h != "" {
		if i, err := strconv.ParseInt(h, 16, 64); err == nil {
			n.Checksum = storage.CRC(i)
		}
	}

	if h := resp.Header.Get("Seaweed-LastModified"); h != "" {
		if i, err := strconv.ParseUint(h, 16, 64); err == nil {
			n.LastModified = i
			n.SetHasLastModifiedDate()
		}
	}
	if h := resp.Header.Get("Seaweed-Name"); h != "" {
		n.Name = []byte(h)
		n.SetHasName()
	}
	if h := resp.Header.Get("Seaweed-Mime"); h != "" {
		n.Mime = []byte(h)
		n.SetHasMime()
	}
	return int(n.DataSize), nil
}
