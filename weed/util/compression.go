package util

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"io/ioutil"
	"strings"

	"golang.org/x/tools/godoc/util"

	"github.com/chrislusf/seaweedfs/weed/glog"
)

func GzipData(input []byte) ([]byte, error) {
	buf := new(bytes.Buffer)
	w, _ := gzip.NewWriterLevel(buf, flate.BestSpeed)
	if _, err := w.Write(input); err != nil {
		glog.V(2).Infoln("error compressing data:", err)
		return nil, err
	}
	if err := w.Close(); err != nil {
		glog.V(2).Infoln("error closing compressed data:", err)
		return nil, err
	}
	return buf.Bytes(), nil
}
func UnGzipData(input []byte) ([]byte, error) {
	buf := bytes.NewBuffer(input)
	r, _ := gzip.NewReader(buf)
	defer r.Close()
	output, err := ioutil.ReadAll(r)
	if err != nil {
		glog.V(2).Infoln("error uncompressing data:", err)
	}
	return output, err
}

/*
* Default more not to gzip since gzip can be done on client side.
 */
func IsGzippable(ext, mtype string, data []byte) bool {

	shouldBeZipped, iAmSure := IsGzippableFileType(ext, mtype)
	if iAmSure {
		return shouldBeZipped
	}

	isMostlyText := util.IsText(data)

	return isMostlyText
}

/*
* Default more not to gzip since gzip can be done on client side.
 */func IsGzippableFileType(ext, mtype string) (shouldBeZipped, iAmSure bool) {

	// text
	if strings.HasPrefix(mtype, "text/") {
		return true, true
	}

	// images
	switch ext {
	case ".svg", ".bmp", ".wav":
		return true, true
	}
	if strings.HasPrefix(mtype, "image/") {
		return false, true
	}

	// by file name extension
	switch ext {
	case ".zip", ".rar", ".gz", ".bz2", ".xz":
		return false, true
	case ".pdf", ".txt", ".html", ".htm", ".css", ".js", ".json":
		return true, true
	case ".php", ".java", ".go", ".rb", ".c", ".cpp", ".h", ".hpp":
		return true, true
	case ".png", ".jpg", ".jpeg":
		return false, true
	}

	// by mime type
	if strings.HasPrefix(mtype, "application/") {
		if strings.HasSuffix(mtype, "xml") {
			return true, true
		}
		if strings.HasSuffix(mtype, "script") {
			return true, true
		}

	}

	if strings.HasPrefix(mtype, "audio/") {
		switch strings.TrimPrefix(mtype, "audio/") {
		case "wave", "wav", "x-wav", "x-pn-wav":
			return true, true
		}
	}

	return false, false
}
