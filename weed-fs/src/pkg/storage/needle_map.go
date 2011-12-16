package storage

import (
    "os"
)

type NeedleKey struct{
  Key uint64 "file id"
  AlternateKey uint32 "supplemental id"
}
func (k *NeedleKey) String() string {
  var tmp [12]byte
  for i :=uint(0);i<8;i++{
    tmp[i] = byte(k.Key >> (8*i));
  }
  for i :=uint(0);i<4;i++{
    tmp[i+8] = byte(k.AlternateKey >> (8*i));
  }
  return string(tmp[:])
}

type NeedleValue struct{
  Offset uint32 "Volume offset" //since aligned to 8 bytes, range is 4G*8=32G
  Size uint32 "Size of the data portion"
}

type NeedleMap struct{
  m map[string]*NeedleValue //mapping NeedleKey(Key,AlternateKey) to NeedleValue
}
func NewNeedleMap() (nm *NeedleMap){
  nm = new(NeedleMap)
  nm.m = make(map[string]*NeedleValue)
  return
}
func (nm *NeedleMap) load(file *os.File){
}
func makeKey(key uint64, altKey uint32) string {
  var tmp [12]byte
  for i :=uint(0);i<8;i++{
    tmp[i] = byte(key >> (8*i));
  }
  for i :=uint(0);i<4;i++{
    tmp[i+8] = byte(altKey >> (8*i));
  }
  return string(tmp[:])
}
func (nm *NeedleMap) put(key uint64, altKey uint32, offset uint32, size uint32){
  nm.m[makeKey(key,altKey)] = &NeedleValue{Offset:offset, Size:size}
}
func (nm *NeedleMap) get(key uint64, altKey uint32) (element *NeedleValue, ok bool){
  element, ok = nm.m[makeKey(key,altKey)]
  return
}
