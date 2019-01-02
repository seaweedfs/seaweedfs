package weed_server

import (
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/filer2"
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func (fs *FilerServer) GetOrHeadHandler(w http.ResponseWriter, r *http.Request, isGetMethod bool) {
	path := r.URL.Path
	if strings.HasSuffix(path, "/") && len(path) > 1 {
		path = path[:len(path)-1]
	}

	entry, err := fs.filer.FindEntry(filer2.FullPath(path))
	if err != nil {
		if path == "/" {
			fs.listDirectoryHandler(w, r)
			return
		}
		glog.V(1).Infof("Not found %s: %v", path, err)
		w.WriteHeader(http.StatusNotFound)
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

	if len(entry.Chunks) == 0 {
		glog.V(1).Infof("no file chunks for %s, attr=%+v", path, entry.Attr)
		w.WriteHeader(http.StatusNoContent)
		return
	}

	w.Header().Set("Accept-Ranges", "bytes")
	if r.Method == "HEAD" {
		w.Header().Set("Content-Length", strconv.FormatInt(int64(filer2.TotalSize(entry.Chunks)), 10))
		w.Header().Set("Last-Modified", entry.Attr.Mtime.Format(http.TimeFormat))
		return
	}

	if len(entry.Chunks) == 1 {
		fs.handleSingleChunk(w, r, entry)
		return
	}

	fs.handleMultipleChunks(w, r, entry)

}

func (fs *FilerServer) handleSingleChunk(w http.ResponseWriter, r *http.Request, entry *filer2.Entry) {

	fileId := entry.Chunks[0].FileId

	urlString, err := fs.filer.MasterClient.LookupFileId(fileId)
	if err != nil {
		glog.V(1).Infof("operation LookupFileId %s failed, err: %v", fileId, err)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	if fs.option.RedirectOnRead {
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

func (fs *FilerServer) handleMultipleChunks(w http.ResponseWriter, r *http.Request, entry *filer2.Entry) {

	mimeType := entry.Mime
	if mimeType == "" {
		if ext := path.Ext(entry.Name()); ext != "" {
			mimeType = mime.TypeByExtension(ext)
		}
	}
	if mimeType != "" {
		w.Header().Set("Content-Type", mimeType)
	}
	setEtag(w, filer2.ETag(entry.Chunks))

	totalSize := int64(filer2.TotalSize(entry.Chunks))

	rangeReq := r.Header.Get("Range")

	if rangeReq == "" {
		w.Header().Set("Content-Length", strconv.FormatInt(totalSize, 10))
		if err := fs.writeContent(w, entry, 0, int(totalSize)); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		return
	}

	//the rest is dealing with partial content request
	//mostly copy from src/pkg/net/http/fs.go
	ranges, err := parseRange(rangeReq, totalSize)
	if err != nil {
		http.Error(w, err.Error(), http.StatusRequestedRangeNotSatisfiable)
		return
	}
	if sumRangesSize(ranges) > totalSize {
		// The total number of bytes in all the ranges
		// is larger than the size of the file by
		// itself, so this is probably an attack, or a
		// dumb client.  Ignore the range request.
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
		w.Header().Set("Content-Range", ra.contentRange(totalSize))
		w.WriteHeader(http.StatusPartialContent)

		err = fs.writeContent(w, entry, ra.start, int(ra.length))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		return
	}

	// process multiple ranges
	for _, ra := range ranges {
		if ra.start > totalSize {
			http.Error(w, "Out of Range", http.StatusRequestedRangeNotSatisfiable)
			return
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
			if e = fs.writeContent(part, entry, ra.start, int(ra.length)); e != nil {
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
	if _, err := io.CopyN(w, sendContent, sendSize); err != nil {
		http.Error(w, "Internal Error", http.StatusInternalServerError)
		return
	}

}

func (fs *FilerServer) writeContent(w io.Writer, entry *filer2.Entry, offset int64, size int) error {

	chunkViews := filer2.ViewFromChunks(entry.Chunks, offset, size)

	fileId2Url := make(map[string]string)

	for _, chunkView := range chunkViews {

		urlString, err := fs.filer.MasterClient.LookupFileId(chunkView.FileId)
		if err != nil {
			glog.V(1).Infof("operation LookupFileId %s failed, err: %v", chunkView.FileId, err)
			return err
		}
		fileId2Url[chunkView.FileId] = urlString
	}

	for _, chunkView := range chunkViews {
		urlString := fileId2Url[chunkView.FileId]
		_, err := util.ReadUrlAsStream(urlString, chunkView.Offset, int(chunkView.Size), func(data []byte) {
			w.Write(data)
		})
		if err != nil {
			glog.V(1).Infof("read %s failed, err: %v", chunkView.FileId, err)
			return err
		}
	}

	return nil

}
