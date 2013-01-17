package storage

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"io/ioutil"
	"strings"
)

/*
* Default more not to gzip since gzip can be done on client side.
*/
func IsGzippable(ext, mtype string) bool {
    if strings.HasPrefix(mtype, "text/"){
        return true
    }
	if ext == ".zip" {
		return false
	}
	if ext == ".rar" {
		return false
	}
	if ext == ".gz" {
		return false
	}
	if ext == ".pdf" {
		return true
	}
	if ext == ".css" {
		return true
	}
	if ext == ".js" {
		return true
	}
    if ext == ".json" {
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
func GzipData(input []byte) []byte {
	buf := new(bytes.Buffer)
	w, _ := gzip.NewWriterLevel(buf, flate.BestCompression)
	if _, err := w.Write(input); err != nil {
		println("error compressing data:", err)
	}
	if err := w.Close(); err != nil {
		println("error closing compressed data:", err)
	}
	return buf.Bytes()
}
func UnGzipData(input []byte) []byte {
	buf := bytes.NewBuffer(input)
	r, _ := gzip.NewReader(buf)
	defer r.Close()
	output, err := ioutil.ReadAll(r)
	if err != nil {
		println("error uncompressing data:", err)
	}
	return output
}
