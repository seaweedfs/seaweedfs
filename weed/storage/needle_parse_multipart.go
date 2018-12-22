package storage

import (
	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/operation"
	"io/ioutil"
	"mime"
	"net/http"
	"path"
	"strconv"
	"strings"
)

func parseMultipart(r *http.Request) (
	fileName string, data []byte, mimeType string, isGzipped bool, originalDataSize int, isChunkedFile bool, e error) {
	form, fe := r.MultipartReader()
	if fe != nil {
		glog.V(0).Infoln("MultipartReader [ERROR]", fe)
		e = fe
		return
	}

	//first multi-part item
	part, fe := form.NextPart()
	if fe != nil {
		glog.V(0).Infoln("Reading Multi part [ERROR]", fe)
		e = fe
		return
	}

	fileName = part.FileName()
	if fileName != "" {
		fileName = path.Base(fileName)
	}

	data, e = ioutil.ReadAll(part)
	if e != nil {
		glog.V(0).Infoln("Reading Content [ERROR]", e)
		return
	}

	//if the filename is empty string, do a search on the other multi-part items
	for fileName == "" {
		part2, fe := form.NextPart()
		if fe != nil {
			break // no more or on error, just safely break
		}

		fName := part2.FileName()

		//found the first <file type> multi-part has filename
		if fName != "" {
			data2, fe2 := ioutil.ReadAll(part2)
			if fe2 != nil {
				glog.V(0).Infoln("Reading Content [ERROR]", fe2)
				e = fe2
				return
			}

			//update
			data = data2
			fileName = path.Base(fName)
			break
		}
	}

	originalDataSize = len(data)

	isChunkedFile, _ = strconv.ParseBool(r.FormValue("cm"))

	if !isChunkedFile {

		dotIndex := strings.LastIndex(fileName, ".")
		ext, mtype := "", ""
		if dotIndex > 0 {
			ext = strings.ToLower(fileName[dotIndex:])
			mtype = mime.TypeByExtension(ext)
		}
		contentType := part.Header.Get("Content-Type")
		if contentType != "" && mtype != contentType {
			mimeType = contentType //only return mime type if not deductable
			mtype = contentType
		}

		if part.Header.Get("Content-Encoding") == "gzip" {
			if unzipped, e := operation.UnGzipData(data); e == nil {
				originalDataSize = len(unzipped)
			}
			isGzipped = true
		} else if operation.IsGzippable(ext, mtype, data) {
			if data, e = operation.GzipData(data); e != nil {
				return
			}
			isGzipped = true
		}
	}

	return
}
