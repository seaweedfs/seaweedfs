package abstract_sql

import (
	"crypto/md5"
	"io"
)

// returns a 128 bit hash
func md5hash(dir string) []byte {
	h := md5.New()
	io.WriteString(h, dir)
	return h.Sum(nil)
}
