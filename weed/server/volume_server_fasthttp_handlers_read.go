package weed_server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/valyala/fasthttp"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/images"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"github.com/chrislusf/seaweedfs/weed/stats"
	"github.com/chrislusf/seaweedfs/weed/storage/needle"
	"github.com/chrislusf/seaweedfs/weed/util"
)

func (vs *VolumeServer) fastGetOrHeadHandler(ctx *fasthttp.RequestCtx) {

	stats.VolumeServerRequestCounter.WithLabelValues("get").Inc()
	start := time.Now()
	defer func() { stats.VolumeServerRequestHistogram.WithLabelValues("get").Observe(time.Since(start).Seconds()) }()

	requestPath := string(ctx.Path())
	n := new(needle.Needle)
	vid, fid, filename, ext, _ := parseURLPath(requestPath)

	if !vs.maybeCheckJwtAuthorization(ctx, vid, fid, false) {
		writeJsonError(ctx, http.StatusUnauthorized, errors.New("wrong jwt"))
		return
	}

	volumeId, err := needle.NewVolumeId(vid)
	if err != nil {
		glog.V(2).Infof("parsing volumd id %s error: %v", err, requestPath)
		ctx.SetStatusCode(http.StatusBadRequest)
		return
	}
	err = n.ParsePath(fid)
	if err != nil {
		glog.V(2).Infof("parsing fid %s error: %v", err, requestPath)
		ctx.SetStatusCode(http.StatusBadRequest)
		return
	}

	// glog.V(4).Infoln("volume", volumeId, "reading", n)
	hasVolume := vs.store.HasVolume(volumeId)
	_, hasEcVolume := vs.store.FindEcVolume(volumeId)
	if !hasVolume && !hasEcVolume {
		if !vs.ReadRedirect {
			glog.V(2).Infoln("volume is not local:", err, requestPath)
			ctx.SetStatusCode(http.StatusNotFound)
			return
		}
		lookupResult, err := operation.Lookup(vs.GetMaster(), volumeId.String())
		glog.V(2).Infoln("volume", volumeId, "found on", lookupResult, "error", err)
		if err == nil && len(lookupResult.Locations) > 0 {
			u, _ := url.Parse(util.NormalizeUrl(lookupResult.Locations[0].PublicUrl))
			u.Path = fmt.Sprintf("%s/%s,%s", u.Path, vid, fid)
			arg := url.Values{}
			if c := ctx.FormValue("collection"); c != nil {
				arg.Set("collection", string(c))
			}
			u.RawQuery = arg.Encode()
			ctx.Redirect(u.String(), http.StatusMovedPermanently)

		} else {
			glog.V(2).Infof("lookup %s error: %v", requestPath, err)
			ctx.SetStatusCode(http.StatusNotFound)
		}
		return
	}

	cookie := n.Cookie
	var count int
	if hasVolume {
		count, err = vs.store.ReadVolumeNeedle(volumeId, n)
	} else if hasEcVolume {
		count, err = vs.store.ReadEcShardNeedle(context.Background(), volumeId, n)
	}
	// glog.V(4).Infoln("read bytes", count, "error", err)
	if err != nil || count < 0 {
		glog.V(0).Infof("read %s isNormalVolume %v error: %v", requestPath, hasVolume, err)
		ctx.SetStatusCode(http.StatusNotFound)
		return
	}
	if n.Cookie != cookie {
		glog.V(0).Infof("request %s with cookie:%x expected:%x agent %s", requestPath, cookie, n.Cookie, string(ctx.UserAgent()))
		ctx.SetStatusCode(http.StatusNotFound)
		return
	}
	if n.LastModified != 0 {
		ctx.Response.Header.Set("Last-Modified", time.Unix(int64(n.LastModified), 0).UTC().Format(http.TimeFormat))
		if ctx.Response.Header.Peek("If-Modified-Since") != nil {
			if t, parseError := time.Parse(http.TimeFormat, string(ctx.Response.Header.Peek("If-Modified-Since"))); parseError == nil {
				if t.Unix() >= int64(n.LastModified) {
					ctx.SetStatusCode(http.StatusNotModified)
					return
				}
			}
		}
	}
	if inm := ctx.Response.Header.Peek("If-None-Match"); inm != nil && string(inm) == "\""+n.Etag()+"\"" {
		ctx.SetStatusCode(http.StatusNotModified)
		return
	}
	eTagMd5 := ctx.Response.Header.Peek("ETag-MD5")
	if eTagMd5 != nil && string(eTagMd5) == "True" {
		fastSetEtag(ctx, n.MD5())
	} else {
		fastSetEtag(ctx, n.Etag())
	}

	if n.HasPairs() {
		pairMap := make(map[string]string)
		err = json.Unmarshal(n.Pairs, &pairMap)
		if err != nil {
			glog.V(0).Infoln("Unmarshal pairs error:", err)
		}
		for k, v := range pairMap {
			ctx.Response.Header.Set(k, v)
		}
	}

	if vs.fastTryHandleChunkedFile(n, filename, ctx) {
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
			acceptEncoding := ctx.Request.Header.Peek("Accept-Encoding")
			if acceptEncoding != nil && strings.Contains(string(acceptEncoding), "gzip") {
				ctx.Response.Header.Set("Content-Encoding", "gzip")
			} else {
				if n.Data, err = util.UnGzipData(n.Data); err != nil {
					glog.V(0).Infoln("ungzip error:", err, requestPath)
				}
			}
		}
	}

	rs := fastConditionallyResizeImages(bytes.NewReader(n.Data), ext, ctx)

	if e := fastWriteResponseContent(filename, mtype, rs, ctx); e != nil {
		glog.V(2).Infoln("response write error:", e)
	}

}

func (vs *VolumeServer) fastTryHandleChunkedFile(n *needle.Needle, fileName string, ctx *fasthttp.RequestCtx) (processed bool) {
	if !n.IsChunkedManifest() || string(ctx.FormValue("cm")) == "false" {
		return false
	}

	chunkManifest, e := operation.LoadChunkManifest(n.Data, n.IsGzipped())
	if e != nil {
		glog.V(0).Infof("load chunked manifest (%s) error: %v", string(ctx.Path()), e)
		return false
	}
	if fileName == "" && chunkManifest.Name != "" {
		fileName = chunkManifest.Name
	}

	ext := path.Ext(fileName)

	mType := ""
	if chunkManifest.Mime != "" {
		mt := chunkManifest.Mime
		if !strings.HasPrefix(mt, "application/octet-stream") {
			mType = mt
		}
	}

	ctx.Response.Header.Set("X-File-Store", "chunked")

	chunkedFileReader := &operation.ChunkedFileReader{
		Manifest: chunkManifest,
		Master:   vs.GetMaster(),
	}
	defer chunkedFileReader.Close()

	rs := fastConditionallyResizeImages(chunkedFileReader, ext, ctx)

	if e := fastWriteResponseContent(fileName, mType, rs, ctx); e != nil {
		glog.V(2).Infoln("response write error:", e)
	}
	return true
}

func fastConditionallyResizeImages(originalDataReaderSeeker io.ReadSeeker, ext string, ctx *fasthttp.RequestCtx) io.ReadSeeker {
	rs := originalDataReaderSeeker
	if len(ext) > 0 {
		ext = strings.ToLower(ext)
	}
	if ext == ".png" || ext == ".jpg" || ext == ".jpeg" || ext == ".gif" {
		width, height := 0, 0
		formWidth, formHeight := ctx.FormValue("width"), ctx.FormValue("height")
		if formWidth != nil {
			width, _ = strconv.Atoi(string(formWidth))
		}
		if formHeight != nil {
			height, _ = strconv.Atoi(string(formHeight))
		}
		formMode := ctx.FormValue("mode")
		rs, _, _ = images.Resized(ext, originalDataReaderSeeker, width, height, string(formMode))
	}
	return rs
}

func fastWriteResponseContent(filename, mimeType string, rs io.ReadSeeker, ctx *fasthttp.RequestCtx) error {
	totalSize, e := rs.Seek(0, 2)
	if mimeType == "" {
		if ext := path.Ext(filename); ext != "" {
			mimeType = mime.TypeByExtension(ext)
		}
	}
	if mimeType != "" {
		ctx.Response.Header.Set("Content-Type", mimeType)
	}
	if filename != "" {
		contentDisposition := "inline"
		dlFormValue := ctx.FormValue("dl")
		if dlFormValue != nil {
			if dl, _ := strconv.ParseBool(string(dlFormValue)); dl {
				contentDisposition = "attachment"
			}
		}
		ctx.Response.Header.Set("Content-Disposition", contentDisposition+`; filename="`+fileNameEscaper.Replace(filename)+`"`)
	}
	ctx.Response.Header.Set("Accept-Ranges", "bytes")
	if ctx.IsHead() {
		ctx.Response.Header.Set("Content-Length", strconv.FormatInt(totalSize, 10))
		return nil
	}
	rangeReq := ctx.FormValue("Range")
	if rangeReq == nil {
		ctx.Response.Header.Set("Content-Length", strconv.FormatInt(totalSize, 10))
		if _, e = rs.Seek(0, 0); e != nil {
			return e
		}
		_, e = io.Copy(ctx.Response.BodyWriter(), rs)
		return e
	}

	//the rest is dealing with partial content request
	//mostly copy from src/pkg/net/http/fs.go
	ranges, err := parseRange(string(rangeReq), totalSize)
	if err != nil {
		ctx.Response.SetStatusCode(http.StatusRequestedRangeNotSatisfiable)
		ctxError(ctx, err.Error(), http.StatusRequestedRangeNotSatisfiable)
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
		ctx.Response.Header.Set("Content-Length", strconv.FormatInt(ra.length, 10))
		ctx.Response.Header.Set("Content-Range", ra.contentRange(totalSize))
		ctx.Response.SetStatusCode(http.StatusPartialContent)
		if _, e = rs.Seek(ra.start, 0); e != nil {
			return e
		}

		_, e = io.CopyN(ctx.Response.BodyWriter(), rs, ra.length)
		return e
	}
	// process multiple ranges
	for _, ra := range ranges {
		if ra.start > totalSize {
			ctxError(ctx, "Out of Range", http.StatusRequestedRangeNotSatisfiable)
			return nil
		}
	}
	sendSize := rangesMIMESize(ranges, mimeType, totalSize)
	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)
	ctx.Response.Header.Set("Content-Type", "multipart/byteranges; boundary="+mw.Boundary())
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
	if ctx.Response.Header.Peek("Content-Encoding") == nil {
		ctx.Response.Header.Set("Content-Length", strconv.FormatInt(sendSize, 10))
	}
	ctx.Response.Header.SetStatusCode(http.StatusPartialContent)
	_, e = io.CopyN(ctx.Response.BodyWriter(), sendContent, sendSize)
	return e
}

func fastSetEtag(ctx *fasthttp.RequestCtx, etag string) {
	if etag != "" {
		if strings.HasPrefix(etag, "\"") {
			ctx.Response.Header.Set("ETag", etag)
		} else {
			ctx.Response.Header.Set("ETag", "\""+etag+"\"")
		}
	}
}

func ctxError(ctx *fasthttp.RequestCtx, error string, code int) {
	ctx.Response.Header.Set("Content-Type", "text/plain; charset=utf-8")
	ctx.Response.Header.Set("X-Content-Type-Options", "nosniff")
	ctx.Response.SetStatusCode(code)
	fmt.Fprintln(ctx.Response.BodyWriter(), error)
}
