package storage

import (
    "http"
    "io/ioutil"
    "url"
    "log"
)

func bytesToUint64(b []byte)(v uint64){
  for i :=uint(7);i>0;i-- {
    v += uint64(b[i])
    v <<= 8
  }
  v+=uint64(b[0])
  return
}
func bytesToUint32(b []byte)(v uint32){
  for i :=uint(3);i>0;i-- {
    v += uint32(b[i])
    v <<= 8
  }
  v+=uint32(b[0])
  return
}
func uint64toBytes(b []byte, v uint64){
  for i :=uint(0);i<8;i++ {
    b[i] = byte(v>>(i*8))
  }
}
func uint32toBytes(b []byte, v uint32){
  for i :=uint(0);i<4;i++ {
    b[i] = byte(v>>(i*8))
  }
}

func post(url string, values url.Values)string{
    r, err := http.PostForm(url, values)
    if err != nil {
        log.Println("post:", err)
        return ""
    }
    defer r.Body.Close()
    b, err := ioutil.ReadAll(r.Body)
    if err != nil {
        log.Println("post:", err)
        return ""
    }
    return string(b)
}