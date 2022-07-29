package util

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/seaweedfs/seaweedfs/weed/glog"
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
	w := new(bytes.Buffer)
	_, err := GzipStream(w, bytes.NewReader(input))
	if err != nil {
		return nil, err
	}
	return w.Bytes(), nil
}

func ungzipData(input []byte) ([]byte, error) {
	w := new(bytes.Buffer)
	_, err := GunzipStream(w, bytes.NewReader(input))
	if err != nil {
		return nil, err
	}
	return w.Bytes(), nil
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
	case ".zip", ".rar", ".gz", ".bz2", ".xz", ".zst", ".br":
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
		if strings.HasSuffix(mtype, "vnd.rar") {
			return false, true
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
