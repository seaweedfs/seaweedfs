package storage

import (
    "http"
    "io/ioutil"
    "url"
    "log"
)

func BytesToUint64(b []byte)(v uint64){
  length := uint(len(b))
  for i :=uint(0);i<length-1;i++ {
    v += uint64(b[i])
    v <<= 8
  }
  v+=uint64(b[length-1])
  return
}
func BytesToUint32(b []byte)(v uint32){
  length := uint(len(b))
  for i :=uint(0);i<length-1;i++ {
    v += uint32(b[i])
    v <<= 8
  }
  v+=uint32(b[length-1])
  return
}
func Uint64toBytes(b []byte, v uint64){
  for i :=uint(0);i<8;i++ {
    b[7-i] = byte(v>>(i*8))
  }
}
func Uint32toBytes(b []byte, v uint32){
  for i :=uint(0);i<4;i++ {
    b[3-i] = byte(v>>(i*8))
  }
}

func post(url string, values url.Values)[]byte{
    r, err := http.PostForm(url, values)
    if err != nil {
        log.Println("post:", err)
        return nil
    }
    defer r.Body.Close()
    b, err := ioutil.ReadAll(r.Body)
    if err != nil {
        log.Println("post:", err)
        return nil
    }
    return b
}