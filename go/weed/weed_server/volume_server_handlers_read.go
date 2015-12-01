package weed_server

import (
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/chrislusf/seaweedfs/go/glog"
	"github.com/chrislusf/seaweedfs/go/images"
	"github.com/chrislusf/seaweedfs/go/operation"
	"github.com/chrislusf/seaweedfs/go/storage"
	"github.com/chrislusf/seaweedfs/go/util"
)

var fileNameEscaper = strings.NewReplacer("\\", "\\\\", "\"", "\\\"")

func (vs *VolumeServer) GetOrHeadHandler(w http.ResponseWriter, r *http.Request) {
	n := new(storage.Needle)
	vid, fid, filename, ext, _ := parseURLPath(r.URL.Path)
	volumeId, err := storage.NewVolumeId(vid)
	if err != nil {
		glog.V(2).Infoln("parsing error:", err, r.URL.Path)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	err = n.ParsePath(fid)
	if err != nil {
		glog.V(2).Infoln("parsing fid error:", err, r.URL.Path)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	glog.V(4).Infoln("volume", volumeId, "reading", n)
	if !vs.store.HasVolume(volumeId) {
		if !vs.ReadRedirect {
			glog.V(2).Infoln("volume is not local:", err, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		lookupResult, err := operation.Lookup(vs.GetMasterNode(), volumeId.String())
		glog.V(2).Infoln("volume", volumeId, "found on", lookupResult, "error", err)
		if err == nil && len(lookupResult.Locations) > 0 {
			http.Redirect(w, r, util.NormalizeUrl(lookupResult.Locations[0].PublicUrl)+r.URL.Path, http.StatusMovedPermanently)
		} else {
			glog.V(2).Infoln("lookup error:", err, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
		return
	}
	cookie := n.Cookie
	count, e := vs.store.ReadVolumeNeedle(volumeId, n)
	glog.V(4).Infoln("read bytes", count, "error", e)
	if e != nil || count <= 0 {
		glog.V(0).Infoln("read error:", e, r.URL.Path)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if n.Cookie != cookie {
		glog.V(0).Infoln("request", r.URL.Path, "with unmaching cookie seen:", cookie, "expected:", n.Cookie, "from", r.RemoteAddr, "agent", r.UserAgent())
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

	if vs.tryHandleChunkedFile(n, filename, w, r) {
		return
	}

	if n.NameSize > 0 && filename == "" {
		filename = string(n.Name)
		dotIndex := strings.LastIndex(filename, ".")
		if dotIndex > 0 {
			ext = filename[dotIndex:]
		}
	}
	mtype := ""
	if ext != "" {
		mtype = mime.TypeByExtension(ext)
	}
	if n.MimeSize > 0 {
		mt := string(n.Mime)
		if !strings.HasPrefix(mt, "application/octet-stream") {
			mtype = mt
		}
	}
	if mtype != "" {
		w.Header().Set("Content-Type", mtype)
	}
	if filename != "" {
		w.Header().Set("Content-Disposition", "filename=\""+fileNameEscaper.Replace(filename)+"\"")
	}
	if ext != ".gz" {
		if n.IsGzipped() {
			if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
				w.Header().Set("Content-Encoding", "gzip")
			} else {
				if n.Data, err = storage.UnGzipData(n.Data); err != nil {
					glog.V(0).Infoln("lookup error:", err, r.URL.Path)
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

	w.Header().Set("Accept-Ranges", "bytes")
	if r.Method == "HEAD" {
		w.Header().Set("Content-Length", strconv.Itoa(len(n.Data)))
		return
	}
	rangeReq := r.Header.Get("Range")
	if rangeReq == "" {
		w.Header().Set("Content-Length", strconv.Itoa(len(n.Data)))
		if _, e = w.Write(n.Data); e != nil {
			glog.V(0).Infoln("response write error:", e)
		}
		return
	}

	//the rest is dealing with partial content request
	//mostly copy from src/pkg/net/http/fs.go
	size := int64(len(n.Data))
	ranges, err := parseRange(rangeReq, size)
	if err != nil {
		http.Error(w, err.Error(), http.StatusRequestedRangeNotSatisfiable)
		return
	}
	if sumRangesSize(ranges) > size {
		// The total number of bytes in all the ranges
		// is larger than the size of the file by
		// itself, so this is probably an attack, or a
		// dumb client.  Ignore the range request.
		ranges = nil
		return
	}
	if len(ranges) == 0 {
		return
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
		w.Header().Set("Content-Range", ra.contentRange(size))
		w.WriteHeader(http.StatusPartialContent)
		if _, e = w.Write(n.Data[ra.start : ra.start+ra.length]); e != nil {
			glog.V(0).Infoln("response write error:", e)
		}
		return
	}
	// process multiple ranges
	for _, ra := range ranges {
		if ra.start > size {
			http.Error(w, "Out of Range", http.StatusRequestedRangeNotSatisfiable)
			return
		}
	}
	sendSize := rangesMIMESize(ranges, mtype, size)
	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)
	w.Header().Set("Content-Type", "multipart/byteranges; boundary="+mw.Boundary())
	sendContent := pr
	defer pr.Close() // cause writing goroutine to fail and exit if CopyN doesn't finish.
	go func() {
		for _, ra := range ranges {
			part, err := mw.CreatePart(ra.mimeHeader(mtype, size))
			if err != nil {
				pw.CloseWithError(err)
				return
			}
			if _, err = part.Write(n.Data[ra.start : ra.start+ra.length]); err != nil {
				pw.CloseWithError(err)
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
	io.CopyN(w, sendContent, sendSize)

}

func (vs *VolumeServer) tryHandleChunkedFile(n *storage.Needle, fileName string, w http.ResponseWriter, r *http.Request) (processed bool) {
	if !n.IsChunkedFile() {
		return false
	}
	processed = true
	raw, _ := strconv.ParseBool(r.FormValue("raw"))
	if raw {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Length", strconv.Itoa(len(n.Data)))
		if _, e := w.Write(n.Data); e != nil {
			glog.V(0).Infoln("response write error:", e)
		}
		return true
	}
	chunkManifest, e := operation.LoadChunkedManifest(n.Data)
	if e != nil {
		return false
	}
	ext := ""
	if fileName == "" && chunkManifest.Name != "" {
		fileName = chunkManifest.Name
		dotIndex := strings.LastIndex(fileName, ".")
		if dotIndex > 0 {
			ext = fileName[dotIndex:]
		}
	}
	mtype := ""
	if ext != "" {
		mtype = mime.TypeByExtension(ext)
	}
	if chunkManifest.Mime != "" {
		mt := chunkManifest.Mime
		if !strings.HasPrefix(mt, "application/octet-stream") {
			mtype = mt
		}
	}
	if mtype != "" {
		w.Header().Set("Content-Type", mtype)
	}
	if fileName != "" {
		w.Header().Set("Content-Disposition", `filename="`+fileNameEscaper.Replace(fileName)+`"`)
	}
	w.Header().Set("X-File-Store", "chunked")
	w.Header().Set("Accept-Ranges", "bytes")
	if r.Method == "HEAD" {
		w.Header().Set("Content-Length", strconv.FormatInt(chunkManifest.Size, 10))
		return true
	}

	chunkedFileReader := operation.ChunkedFileReader{
		Manifest: chunkManifest,
		Master:   vs.GetMasterNode(),
	}
	defer chunkedFileReader.Close()
	rangeReq := r.Header.Get("Range")
	if rangeReq == "" {
		w.Header().Set("Content-Length", strconv.FormatInt(chunkManifest.Size, 10))
		if _, e = io.Copy(w, chunkedFileReader); e != nil {
			glog.V(0).Infoln("response write error:", e)
		}
		return true
	}

	//the rest is dealing with partial content request
	//mostly copy from src/pkg/net/http/fs.go
	size := chunkManifest.Size
	ranges, err := parseRange(rangeReq, size)
	if err != nil {
		http.Error(w, err.Error(), http.StatusRequestedRangeNotSatisfiable)
		return
	}
	if sumRangesSize(ranges) > size {
		// The total number of bytes in all the ranges
		// is larger than the size of the file by
		// itself, so this is probably an attack, or a
		// dumb client.  Ignore the range request.
		ranges = nil
		return
	}
	if len(ranges) == 0 {
		return
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
		w.Header().Set("Content-Range", ra.contentRange(size))
		w.WriteHeader(http.StatusPartialContent)
		if _, e = chunkedFileReader.Seek(ra.start, 0); e != nil {
			glog.V(0).Infoln("response write error:", e)
		}
		if _, e = io.CopyN(w, chunkedFileReader, ra.length); e != nil {
			glog.V(0).Infoln("response write error:", e)
		}
		return
	}
	// process multiple ranges
	for _, ra := range ranges {
		if ra.start > size {
			http.Error(w, "Out of Range", http.StatusRequestedRangeNotSatisfiable)
			return
		}
	}
	sendSize := rangesMIMESize(ranges, mtype, size)
	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)
	w.Header().Set("Content-Type", "multipart/byteranges; boundary="+mw.Boundary())
	sendContent := pr
	defer pr.Close() // cause writing goroutine to fail and exit if CopyN doesn't finish.
	go func() {
		for _, ra := range ranges {
			part, err := mw.CreatePart(ra.mimeHeader(mtype, size))
			if err != nil {
				pw.CloseWithError(err)
				return
			}
			if _, e = chunkedFileReader.Seek(ra.start, 0); e != nil {
				glog.V(0).Infoln("response write error:", e)
			}
			if _, err = io.CopyN(part, chunkedFileReader, ra.length); err != nil {
				pw.CloseWithError(err)
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
	io.CopyN(w, sendContent, sendSize)
	return
}
