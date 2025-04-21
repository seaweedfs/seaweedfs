package hash

import (
	"crypto/md5"
	"encoding/hex"
	"io"
)

func Md5(r io.Reader, bufferSize ...int64) (string, error) {
	h := md5.New()
	if len(bufferSize) > 0 && bufferSize[0] > 0 {
		buffer := make([]byte, bufferSize[0])
		_, err := io.CopyBuffer(h, r, buffer)
		if err != nil {
			return "", err
		}
	} else {
		_, err := io.Copy(h, r)
		if err != nil {
			return "", err
		}
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}
