package storage

import (
  "bytes"
  "compress/flate"
  "compress/gzip"
  "io/ioutil"
  "strings"
)

func IsGzippable(ext, mtype string) bool {
  if ext == ".zip" {
    return false
  }
  if ext == ".rar" {
    return false
  }
  if ext == ".gz" {
    return false
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
    println("error compressing data:", err)
  }
  if err := w.Close(); err!=nil {
    println("error closing compressed data:", err)
  }
  return buf.Bytes()
}
func UnGzipData(input []byte) []byte {
  buf := bytes.NewBuffer(input)
  r, _ := gzip.NewReader(buf)
  defer r.Close()
  output, err := ioutil.ReadAll(r)
  if err!=nil {
    println("error uncompressing data:", err)
  }
  return output
}
