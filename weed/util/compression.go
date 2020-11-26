package util

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/chrislusf/seaweedfs/weed/glog"
	// "github.com/klauspost/compress/zstd"
)

var (
	UnsupportedCompression = fmt.Errorf("unsupported compression")
)

func MaybeGzipData(input []byte) []byte {
	if IsGzippedContent(input) {
		return input
	}
	gzipped, err := GzipData(input)
	if err != nil {
		return input
	}
	if len(gzipped)*10 > len(input)*9 {
		return input
	}
	return gzipped
}

func MaybeDecompressData(input []byte) []byte {
	uncompressed, err := DecompressData(input)
	if err != nil {
		if err != UnsupportedCompression {
			glog.Errorf("decompressed data: %v", err)
		}
		return input
	}
	return uncompressed
}

func GzipData(input []byte) ([]byte, error) {
	buf := new(bytes.Buffer)
	w, _ := gzip.NewWriterLevel(buf, flate.BestSpeed)
	if _, err := w.Write(input); err != nil {
		glog.V(2).Infof("error gzip data: %v", err)
		return nil, err
	}
	if err := w.Close(); err != nil {
		glog.V(2).Infof("error closing gzipped data: %v", err)
		return nil, err
	}
	return buf.Bytes(), nil
}

func DecompressData(input []byte) ([]byte, error) {
	if IsGzippedContent(input) {
		return ungzipData(input)
	}
	/*
		if IsZstdContent(input) {
			return unzstdData(input)
		}
	*/
	return input, UnsupportedCompression
}

func ungzipData(input []byte) ([]byte, error) {
	buf := bytes.NewBuffer(input)
	r, _ := gzip.NewReader(buf)
	defer r.Close()
	output, err := ioutil.ReadAll(r)
	if err != nil {
		glog.V(2).Infof("error ungzip data: %v", err)
	}
	return output, err
}

func IsGzippedContent(data []byte) bool {
	if len(data) < 2 {
		return false
	}
	return data[0] == 31 && data[1] == 139
}

/*
var zstdEncoder, _ = zstd.NewWriter(nil)

func ZstdData(input []byte) ([]byte, error) {
	return zstdEncoder.EncodeAll(input, nil), nil
}

var decoder, _ = zstd.NewReader(nil)

func unzstdData(input []byte) ([]byte, error) {
	return decoder.DecodeAll(input, nil)
}

func IsZstdContent(data []byte) bool {
	if len(data) < 4 {
		return false
	}
	return data[3] == 0xFD && data[2] == 0x2F && data[1] == 0xB5 && data[0] == 0x28
}
*/

/*
* Default not to compressed since compression can be done on client side.
 */func IsCompressableFileType(ext, mtype string) (shouldBeCompressed, iAmSure bool) {

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
	case ".zip", ".rar", ".gz", ".bz2", ".xz", ".zst":
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
		if strings.HasSuffix(mtype, "zstd") {
			return false, true
		}
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
