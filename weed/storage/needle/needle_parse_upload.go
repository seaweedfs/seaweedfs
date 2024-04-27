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

func isMultiPartFormData(contentType string) bool {
	return contentType == "multipart/form-data" || contentType == "multipart/mixed"
}

func ParseUpload(r *http.Request, sizeLimit int64, bytesBuffer *bytes.Buffer) (pu *ParsedUpload, e error) {
	bytesBuffer.Reset()
	pu = &ParsedUpload{bytesBuffer: bytesBuffer}
	pu.PairMap = make(map[string]string)
	for k, v := range r.Header {
		if len(v) > 0 && strings.HasPrefix(k, PairNamePrefix) {
			pu.PairMap[k] = v[0]
		}
	}

	switch {
	case r.Method == http.MethodPost:
		contentType := r.Header.Get("Content-Type")
		if isMultiPartFormData(contentType) {
			e = parseMultipart(r, sizeLimit, pu)
		} else {
			// If content-type is explicitly set, upload the file without parsing form-data
			e = parseRawPost(r, sizeLimit, pu)
		}
	case r.Method == http.MethodPut:
		e = parsePut(r, sizeLimit, pu)
	default:
		e = fmt.Errorf("unsupported method: %s", r.Method)
	}
	if e != nil {
		return
	}

	pu.ModifiedTime, _ = strconv.ParseUint(r.FormValue("ts"), 10, 64)
	pu.Ttl, _ = ReadTTL(r.FormValue("ttl"))

	pu.OriginalDataSize = len(pu.Data)
	pu.UncompressedData = pu.Data
	// println("received data", len(pu.Data), "isGzipped", pu.IsGzipped, "mime", pu.MimeType, "name", pu.FileName)
	if pu.IsGzipped {
		if unzipped, e := util.DecompressData(pu.Data); e == nil {
			pu.OriginalDataSize = len(unzipped)
			pu.UncompressedData = unzipped
			// println("ungzipped data size", len(unzipped))
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

	// md5
	h := md5.New()
	h.Write(pu.UncompressedData)
	pu.ContentMd5 = base64.StdEncoding.EncodeToString(h.Sum(nil))
	if expectedChecksum := r.Header.Get("Content-MD5"); expectedChecksum != "" {
		if expectedChecksum != pu.ContentMd5 {
			e = fmt.Errorf("Content-MD5 did not match md5 of file data expected [%s] received [%s] size %d", expectedChecksum, pu.ContentMd5, len(pu.UncompressedData))
			return
		}
	}

	return
}

func parsePut(r *http.Request, sizeLimit int64, pu *ParsedUpload) error {
	pu.IsGzipped = r.Header.Get("Content-Encoding") == "gzip"
	// pu.IsZstd = r.Header.Get("Content-Encoding") == "zstd"
	pu.MimeType = r.Header.Get("Content-Type")
	pu.FileName = ""
	dataSize, err := pu.bytesBuffer.ReadFrom(io.LimitReader(r.Body, sizeLimit+1))
	if err == io.EOF || dataSize == sizeLimit+1 {
		io.Copy(io.Discard, r.Body)
	}
	pu.Data = pu.bytesBuffer.Bytes()
	r.Body.Close()
	return nil
}

func parseMultipart(r *http.Request, sizeLimit int64, pu *ParsedUpload) (e error) {
	defer func() {
		if e != nil && r.Body != nil {
			io.Copy(io.Discard, r.Body)
			r.Body.Close()
		}
	}()
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
		pu.FileName = path.Base(pu.FileName)
	}

	var dataSize int64
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
			pu.FileName = path.Base(fName)
			break
		}
	}

	pu.IsChunkedFile, _ = strconv.ParseBool(r.FormValue("cm"))

	if !pu.IsChunkedFile {

		dotIndex := strings.LastIndex(pu.FileName, ".")
		ext, mtype := "", ""
		if dotIndex > 0 {
			ext = strings.ToLower(pu.FileName[dotIndex:])
			mtype = mime.TypeByExtension(ext)
		}
		contentType := part.Header.Get("Content-Type")
		if contentType != "" && contentType != "application/octet-stream" && mtype != contentType {
			pu.MimeType = contentType // only return mime type if not deducible
			mtype = contentType
		}

	}
	pu.IsGzipped = part.Header.Get("Content-Encoding") == "gzip"
	// pu.IsZstd = part.Header.Get("Content-Encoding") == "zstd"

	return
}

func parseRawPost(r *http.Request, sizeLimit int64, pu *ParsedUpload) (e error) {

	defer func() {
		if e != nil && r.Body != nil {
			io.Copy(io.Discard, r.Body)
			r.Body.Close()
		}
	}()

	pu.FileName = r.Header.Get("Content-Disposition")

	if pu.FileName != "" && strings.Contains(pu.FileName, "filename=") {
		parts := strings.Split(pu.FileName, "filename=")
		parts = strings.Split(parts[1], "\"")

		pu.FileName = parts[1]
	} else {
		pu.FileName = ""
	}

	if pu.FileName != "" {
		pu.FileName = path.Base(pu.FileName)
	} else {
		pu.FileName = path.Base(r.URL.Path)
	}

	var dataSize int64
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
	pu.IsChunkedFile, _ = strconv.ParseBool(r.FormValue("cm"))

	if !pu.IsChunkedFile {

		dotIndex := strings.LastIndex(pu.FileName, ".")
		ext, mtype := "", ""
		if dotIndex > 0 {
			ext = strings.ToLower(pu.FileName[dotIndex:])
			mtype = mime.TypeByExtension(ext)
		}
		contentType := r.Header.Get("Content-Type")

		if contentType != "" && contentType != "application/octet-stream" && mtype != contentType {
			pu.MimeType = contentType // only return mime type if not deducible
			mtype = contentType
		}

	}
	pu.IsGzipped = r.Header.Get("Content-Encoding") == "gzip"
	// pu.IsZstd = r.Header.Get("Content-Encoding") == "zstd"

	return
}
