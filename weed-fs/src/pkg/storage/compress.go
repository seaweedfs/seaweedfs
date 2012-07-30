package storage

import (
  "bytes"
  "compress/flate"
  "compress/gzip"
  "io/ioutil"
  "log"
  "strings"
)

func IsCompressable(ext, mtype string) bool {
  if ext == ".zip" {
    return true
  }
  if ext == ".rar" {
    return true
  }
  if strings.Index(mtype,"text/")==0 {
    return true
  }
  if strings.Index(mtype,"application/")==0 {
    return true
  }
  return false
}
func GzipData(input []byte) []byte {
  buf := new(bytes.Buffer)
  w, _ := gzip.NewWriterLevel(buf, flate.BestCompression)
  if _, err := w.Write(input); err!=nil {
    log.Printf("error compressing data:%s\n", err)
  }
  if err := w.Close(); err!=nil {
    log.Printf("error closing compressed data:%s\n", err)
  }
  return buf.Bytes()
}
func UnGzipData(input []byte) []byte {
  buf := bytes.NewBuffer(input)
  r, _ := gzip.NewReader(buf)
  defer r.Close()
  output, err := ioutil.ReadAll(r)
  if err!=nil {
    log.Printf("error uncompressing data:%s\n", err)
  }
  return output
}
