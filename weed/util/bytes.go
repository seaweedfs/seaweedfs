package util

import (
	"bytes"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
)

// BytesToHumanReadable returns the converted human readable representation of the bytes.
func BytesToHumanReadable(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}

	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}

	return fmt.Sprintf("%.2f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

// big endian

func BytesToUint64(b []byte) (v uint64) {
	length := uint(len(b))
	for i := uint(0); i < length-1; i++ {
		v += uint64(b[i])
		v <<= 8
	}
	v += uint64(b[length-1])
	return
}
func BytesToUint32(b []byte) (v uint32) {
	length := uint(len(b))
	for i := uint(0); i < length-1; i++ {
		v += uint32(b[i])
		v <<= 8
	}
	v += uint32(b[length-1])
	return
}
func BytesToUint16(b []byte) (v uint16) {
	v += uint16(b[0])
	v <<= 8
	v += uint16(b[1])
	return
}
func Uint64toBytes(b []byte, v uint64) {
	for i := uint(0); i < 8; i++ {
		b[7-i] = byte(v >> (i * 8))
	}
}
func Uint32toBytes(b []byte, v uint32) {
	for i := uint(0); i < 4; i++ {
		b[3-i] = byte(v >> (i * 8))
	}
}
func Uint16toBytes(b []byte, v uint16) {
	b[0] = byte(v >> 8)
	b[1] = byte(v)
}
func Uint8toBytes(b []byte, v uint8) {
	b[0] = byte(v)
}

// returns a 64 bit big int
func HashStringToLong(dir string) (v int64) {
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

func HashToInt32(data []byte) (v int32) {
	h := md5.New()
	h.Write(data)

	b := h.Sum(nil)

	v += int32(b[0])
	v <<= 8
	v += int32(b[1])
	v <<= 8
	v += int32(b[2])
	v <<= 8
	v += int32(b[3])

	return
}

func Base64Encode(data []byte) string {
	return base64.StdEncoding.EncodeToString(data)
}

func Base64Md5(data []byte) string {
	return Base64Encode(Md5(data))
}

func Md5(data []byte) []byte {
	hash := md5.New()
	hash.Write(data)
	return hash.Sum(nil)
}

func Md5String(data []byte) string {
	return fmt.Sprintf("%x", Md5(data))
}

func Base64Md5ToBytes(contentMd5 string) []byte {
	data, err := base64.StdEncoding.DecodeString(contentMd5)
	if err != nil {
		return nil
	}
	return data
}

func RandomInt32() int32 {
	buf := make([]byte, 4)
	rand.Read(buf)
	return int32(BytesToUint32(buf))
}

func RandomBytes(byteCount int) []byte {
	buf := make([]byte, byteCount)
	rand.Read(buf)
	return buf
}

type BytesReader struct {
	Bytes []byte
	*bytes.Reader
}

func NewBytesReader(b []byte) *BytesReader {
	return &BytesReader{
		Bytes:  b,
		Reader: bytes.NewReader(b),
	}
}
