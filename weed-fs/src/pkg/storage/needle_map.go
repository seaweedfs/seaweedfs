package storage

import (
	"os"
)

type NeedleKey struct {
	Key          uint64 "file id"
}

func (k *NeedleKey) String() string {
	var tmp [12]byte
	for i := uint(0); i < 8; i++ {
		tmp[i] = byte(k.Key >> (8 * i))
	}
	return string(tmp[:])
}

type NeedleValue struct {
	Offset uint32 "Volume offset" //since aligned to 8 bytes, range is 4G*8=32G
	Size   uint32 "Size of the data portion"
}

type NeedleMap struct {
	m map[uint64]*NeedleValue //mapping NeedleKey(Key,AlternateKey) to NeedleValue
}

func NewNeedleMap() *NeedleMap {
	return &NeedleMap{m: make(map[uint64]*NeedleValue)}
}
func (nm *NeedleMap) load(file *os.File) {
}
func makeKey(key uint64) uint64 {
	return key
}
func (nm *NeedleMap) put(key uint64, offset uint32, size uint32) {
	nm.m[makeKey(key)] = &NeedleValue{Offset: offset, Size: size}
}
func (nm *NeedleMap) get(key uint64) (element *NeedleValue, ok bool) {
	element, ok = nm.m[makeKey(key)]
	return
}
