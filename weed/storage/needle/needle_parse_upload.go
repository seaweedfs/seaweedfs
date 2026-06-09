package needle

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io"
	"mime"
	"net/http"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

// maxEagerPreGrow caps how many bytes ParseUpload is willing to pre-allocate
// from a request's announced Content-Length. Large enough to skip a few
// rounds of bytes.Buffer.ReadFrom geometric grow on typical small uploads;
// small enough that a misreported Content-Length or a slow/idle client can
// only ever waste this much memory per request. Bigger uploads fall back to
// the standard ReadFrom-grow path for the remainder.
const maxEagerPreGrow = 4 * 1024 * 1024

// eagerPreGrow ensures bytesBuffer has at least min(contentLength, sizeLimit,
// maxEagerPreGrow) bytes of capacity, so the bytes.Buffer.ReadFrom pumps
// inside parseUpload below skip the first round(s) of geometric grow on the
// common upload sizes — see #6541. The cap policy is the load-bearing piece
// here; it's extracted so unit tests can exercise the policy directly
// without spinning a real http upload.
func eagerPreGrow(bytesBuffer *bytes.Buffer, contentLength, sizeLimit int64) {
	if contentLength <= 0 || contentLength > sizeLimit {
		return
	}
	grow := contentLength
	if grow > maxEagerPreGrow {
		grow = maxEagerPreGrow
	}
	bytesBuffer.Grow(int(grow))
}

type ParsedUpload struct {
	FileName    string
	Data        []byte
	bytesBuffer *bytes.Buffer
	MimeType    string
	PairMap     map[string]string
	IsGzipped   bool
	// IsZstd           bool
	OriginalDataSize int
	ModifiedTime     uint64
	Ttl              *TTL
	IsChunkedFile    bool
	UncompressedData []byte
	ContentMd5       string
}

func ParseUpload(r *http.Request, sizeLimit int64, bytesBuffer *bytes.Buffer) (pu *ParsedUpload, e error) {
	bytesBuffer.Reset()
	eagerPreGrow(bytesBuffer, r.ContentLength, sizeLimit)
	pu = &ParsedUpload{bytesBuffer: bytesBuffer}
	pu.PairMap = make(map[string]string)
	for k, v := range r.Header {
		if len(v) > 0 && strings.HasPrefix(k, PairNamePrefix) {
			pu.PairMap[k] = v[0]
		}
	}
	// glog.V(4).Infof("ParseUpload: r.URL=%s, Content-MD5 header=%s", r.URL.String(), r.Header.Get("Content-MD5"))

	e = parseUpload(r, sizeLimit, pu)

	if e != nil {
		return
	}

	pu.ModifiedTime, _ = strconv.ParseUint(r.FormValue("ts"), 10, 64)
	pu.Ttl, _ = ReadTTL(r.FormValue("ttl"))

	pu.OriginalDataSize = len(pu.Data)
	pu.UncompressedData = pu.Data
	// println("received data", len(pu.Data), "isGzipped", pu.IsGzipped, "mime", pu.MimeType, "name", pu.FileName)
	if pu.IsGzipped {
		// MD5 check needs the uncompressed bytes; otherwise just count
		// the gunzip stream — see #6541.
		needMD5 := r.Header.Get("Content-MD5") != "" || pu.ContentMd5 != ""
		if needMD5 {
			if unzipped, err := util.DecompressData(pu.Data); err == nil {
				pu.OriginalDataSize = len(unzipped)
				pu.UncompressedData = unzipped
			}
		} else if n, err := util.GunzipStream(io.Discard, bytes.NewReader(pu.Data)); err == nil {
			pu.OriginalDataSize = int(n)
		}
	} else {
		ext := filepath.Base(pu.FileName)
		mimeType := pu.MimeType
		if mimeType == "" {
			mimeType = http.DetectContentType(pu.Data)
		}
		// println("detected mimetype to", pu.MimeType)
		if mimeType == "application/octet-stream" {
			mimeType = ""
		}
		if shouldBeCompressed, iAmSure := util.IsCompressableFileType(ext, mimeType); shouldBeCompressed && iAmSure {
			// println("ext", ext, "iAmSure", iAmSure, "shouldBeCompressed", shouldBeCompressed, "mimeType", pu.MimeType)
			if compressedData, err := util.GzipData(pu.Data); err == nil {
				if len(compressedData)*10 < len(pu.Data)*9 {
					pu.Data = compressedData
					pu.IsGzipped = true
				}
				// println("gzipped data size", len(compressedData))
			}
		}
	}

	// verify Content-MD5
	expectedChecksum := r.Header.Get("Content-MD5")
	if expectedChecksum == "" {
		expectedChecksum = pu.ContentMd5
	}
	if expectedChecksum != "" {
		h := md5.New()
		h.Write(pu.UncompressedData)
		pu.ContentMd5 = base64.StdEncoding.EncodeToString(h.Sum(nil))
		if expectedChecksum != pu.ContentMd5 {
			e = fmt.Errorf("Content-MD5 did not match md5 of file data expected [%s] received [%s] size %d", expectedChecksum, pu.ContentMd5, len(pu.UncompressedData))
			return
		}
	}

	return
}

func parseUpload(r *http.Request, sizeLimit int64, pu *ParsedUpload) (e error) {

	defer func() {
		if e != nil && r.Body != nil {
			io.Copy(io.Discard, r.Body)
			r.Body.Close()
		}
	}()

	contentType := r.Header.Get("Content-Type")
	var dataSize int64

	if r.Method == http.MethodPost && (contentType == "" || strings.Contains(contentType, "form-data")) {
		form, fe := r.MultipartReader()

		if fe != nil {
			glog.V(0).Infoln("MultipartReader [ERROR]", fe)
			e = fe
			return
		}

		// first multi-part item
		part, fe := form.NextPart()
		if fe != nil {
			glog.V(0).Infoln("Reading Multi part [ERROR]", fe)
			e = fe
			return
		}

		pu.FileName = part.FileName()
		if pu.FileName != "" {
			pu.FileName = util.CleanWindowsPathBase(pu.FileName)
		}

		dataSize, e = pu.bytesBuffer.ReadFrom(io.LimitReader(part, sizeLimit+1))
		if e != nil {
			glog.V(0).Infoln("Reading Content [ERROR]", e)
			return
		}
		if dataSize == sizeLimit+1 {
			e = fmt.Errorf("file over the limited %d bytes", sizeLimit)
			return
		}
		pu.Data = pu.bytesBuffer.Bytes()

		contentType = part.Header.Get("Content-Type")
		pu.ContentMd5 = part.Header.Get("Content-MD5")

		// if the filename is empty string, do a search on the other multi-part items
		for pu.FileName == "" {
			part2, fe := form.NextPart()
			if fe != nil {
				break // no more or on error, just safely break
			}

			fName := part2.FileName()

			// found the first <file type> multi-part has filename
			if fName != "" {
				pu.bytesBuffer.Reset()
				dataSize2, fe2 := pu.bytesBuffer.ReadFrom(io.LimitReader(part2, sizeLimit+1))
				if fe2 != nil {
					glog.V(0).Infoln("Reading Content [ERROR]", fe2)
					e = fe2
					return
				}
				if dataSize2 == sizeLimit+1 {
					e = fmt.Errorf("file over the limited %d bytes", sizeLimit)
					return
				}

				// update
				pu.Data = pu.bytesBuffer.Bytes()
				pu.FileName = util.CleanWindowsPathBase(fName)
				contentType = part2.Header.Get("Content-Type")
				pu.ContentMd5 = part2.Header.Get("Content-MD5")
				part = part2
				break
			}
		}

		pu.IsGzipped = part.Header.Get("Content-Encoding") == "gzip"
		// pu.IsZstd = part.Header.Get("Content-Encoding") == "zstd"

	} else {
		disposition := r.Header.Get("Content-Disposition")

		if strings.Contains(disposition, "name=") {

			if !strings.HasPrefix(disposition, "inline") && !strings.HasPrefix(disposition, "attachment") {
				disposition = "attachment; " + disposition
			}

			_, mediaTypeParams, err := mime.ParseMediaType(disposition)

			if err == nil {
				dpFilename, hasFilename := mediaTypeParams["filename"]
				dpName, hasName := mediaTypeParams["name"]

				if hasFilename {
					pu.FileName = dpFilename
				} else if hasName {
					pu.FileName = dpName
				}

			}

		} else {
			pu.FileName = ""
		}

		if pu.FileName != "" {
			pu.FileName = util.CleanWindowsPathBase(pu.FileName)
		} else {
			pu.FileName = path.Base(r.URL.Path)
		}

		dataSize, e = pu.bytesBuffer.ReadFrom(io.LimitReader(r.Body, sizeLimit+1))

		if e != nil {
			glog.V(0).Infoln("Reading Content [ERROR]", e)
			return
		}
		if dataSize == sizeLimit+1 {
			e = fmt.Errorf("file over the limited %d bytes", sizeLimit)
			return
		}

		pu.Data = pu.bytesBuffer.Bytes()
		pu.MimeType = contentType
		pu.IsGzipped = r.Header.Get("Content-Encoding") == "gzip"
		// pu.IsZstd = r.Header.Get("Content-Encoding") == "zstd"
	}

	pu.IsChunkedFile, _ = strconv.ParseBool(r.FormValue("cm"))

	if !pu.IsChunkedFile {

		dotIndex := strings.LastIndex(pu.FileName, ".")
		ext, mtype := "", ""

		if dotIndex > 0 {
			ext = strings.ToLower(pu.FileName[dotIndex:])
			mtype = mime.TypeByExtension(ext)
		}

		if contentType != "" && contentType != "application/octet-stream" && mtype != contentType {
			pu.MimeType = contentType // only return mime type if not deducible
		} else if mtype != "" && pu.MimeType == "" && mtype != "application/octet-stream" {
			pu.MimeType = mtype
		}

	}

	return
}
