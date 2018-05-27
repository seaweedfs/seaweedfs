package abstract_sql

import (
	"crypto/md5"
	"io"
)

// returns a 64 bit big int
func hashToLong(dir string) (v int64) {
	h := md5.New()
	io.WriteString(h, dir)

	b := h.Sum(nil)

	v += int64(b[0])
	v <<= 8
	v += int64(b[1])
	v <<= 8
	v += int64(b[2])
	v <<= 8
	v += int64(b[3])
	v <<= 8
	v += int64(b[4])
	v <<= 8
	v += int64(b[5])
	v <<= 8
	v += int64(b[6])
	v <<= 8
	v += int64(b[7])

	return
}
