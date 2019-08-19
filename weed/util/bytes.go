package util

import "encoding/binary"

// big endian

func BytesToUint64Old(b []byte) (v uint64) {
	length := uint(len(b))
	for i := uint(0); i < length-1; i++ {
		v += uint64(b[i])
		v <<= 8
	}
	v += uint64(b[length-1])
	return
}
func BytesToUint32Old(b []byte) (v uint32) {
	length := uint(len(b))
	for i := uint(0); i < length-1; i++ {
		v += uint32(b[i])
		v <<= 8
	}
	v += uint32(b[length-1])
	return
}
func BytesToUint16Old(b []byte) (v uint16) {
	v += uint16(b[0])
	v <<= 8
	v += uint16(b[1])
	return
}
func Uint64toBytesOld(b []byte, v uint64) {
	for i := uint(0); i < 8; i++ {
		b[7-i] = byte(v >> (i * 8))
	}
}
func Uint32toBytesOld(b []byte, v uint32) {
	for i := uint(0); i < 4; i++ {
		b[3-i] = byte(v >> (i * 8))
	}
}
func Uint16toBytesOld(b []byte, v uint16) {
	b[0] = byte(v >> 8)
	b[1] = byte(v)
}

func BytesToUint64(b []byte) (v uint64) {
	return binary.BigEndian.Uint64(b)
}
func BytesToUint32(b []byte) (v uint32) {
	return binary.BigEndian.Uint32(b)
}
func BytesToUint16(b []byte) (v uint16) {
	return binary.BigEndian.Uint16(b)
}
func Uint64toBytes(b []byte, v uint64) {
	binary.BigEndian.PutUint64(b, v)
}
func Uint32toBytes(b []byte, v uint32) {
	binary.BigEndian.PutUint32(b, v)
}
func Uint16toBytes(b []byte, v uint16) {
	binary.BigEndian.PutUint16(b, v)
}
func Uint8toBytes(b []byte, v uint8) {
	b[0] = byte(v)
}
