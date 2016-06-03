package operation

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"io/ioutil"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/glog"
)

/*
* Default more not to gzip since gzip can be done on client side.
 */
func IsGzippable(ext, mtype string) bool {
	if strings.HasPrefix(mtype, "text/") {
		return true
	}
	switch ext {
	case ".zip", ".rar", ".gz", ".bz2", ".xz":
		return false
	case ".pdf", ".txt", ".html", ".htm", ".css", ".js", ".json":
		return true
	}
	if strings.HasPrefix(mtype, "application/") {
		if strings.HasSuffix(mtype, "xml") {
			return true
		}
		if strings.HasSuffix(mtype, "script") {
			return true
		}
	}
	return false
}

func GzipData(input []byte) ([]byte, error) {
	buf := new(bytes.Buffer)
	w, _ := gzip.NewWriterLevel(buf, flate.BestCompression)
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
