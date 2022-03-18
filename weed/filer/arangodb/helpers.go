package arangodb

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"io"
)

//convert a string into arango-key safe hex bytes hash
func hashString(dir string) string {
	h := md5.New()
	io.WriteString(h, dir)
	b := h.Sum(nil)
	return hex.EncodeToString(b)
}

// convert slice of bytes into slice of uint64
// the first uint64 indicates the length in bytes
func bytesToArray(bs []byte) []uint64 {
	out := make([]uint64, 0, 2+len(bs)/8)
	out = append(out, uint64(len(bs)))
	for len(bs)%8 != 0 {
		bs = append(bs, 0)
	}
	for i := 0; i < len(bs); i = i + 8 {
		out = append(out, binary.BigEndian.Uint64(bs[i:]))
	}
	return out
}

// convert from slice of uint64 back to bytes
// if input length is 0 or 1, will return nil
func arrayToBytes(xs []uint64) []byte {
	if len(xs) < 2 {
		return nil
	}
	first := xs[0]
	out := make([]byte, len(xs)*8) // i think this can actually be len(xs)*8-8, but i dont think an extra 8 bytes hurts...
	for i := 1; i < len(xs); i = i + 1 {
		binary.BigEndian.PutUint64(out[((i-1)*8):], xs[i])
	}
	return out[:first]
}
