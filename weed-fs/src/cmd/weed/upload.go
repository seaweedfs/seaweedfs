package main

import (
  "bytes"
  "encoding/json"
  "errors"
  "flag"
  "fmt"
  "io"
  "io/ioutil"
  "mime/multipart"
  "net/http"
  "net/url"
  "os"
  "pkg/util"
  "strconv"
)

func init() {
  cmdUpload.Run = runUpload // break init cycle
  IsDebug = cmdUpload.Flag.Bool("debug", false, "verbose debug information")
  server  = cmdUpload.Flag.String("server", "localhost:9333", "weedfs master location")
}

var cmdUpload = &Command{
  UsageLine: "upload -server=localhost:9333 file1 file2 file2",
  Short:     "upload a set of files, using consecutive file keys",
  Long: `upload a set of files, using consecutive file keys.
  e.g. If the file1 uses key k, file2 can be read via k_1

  `,
}

type AssignResult struct {
  Fid       string "fid"
  Url       string "url"
  PublicUrl string "publicUrl"
  Count     int    `json:",string"`
  Error     string "error"
}

func assign(count int) (AssignResult, error) {
  values := make(url.Values)
  values.Add("count", strconv.Itoa(count))
  jsonBlob := util.Post("http://"+*server+"/dir/assign", values)
  var ret AssignResult
  err := json.Unmarshal(jsonBlob, &ret)
  if err != nil {
    return ret, err
  }
  if ret.Count <= 0 {
    return ret, errors.New(ret.Error)
  }
  return ret, nil
}

type UploadResult struct {
  Size int
}

func upload(filename string, uploadUrl string) (int, string) {
  body_buf := bytes.NewBufferString("")
  body_writer := multipart.NewWriter(body_buf)
  file_writer, err := body_writer.CreateFormFile("file", filename)
  if err != nil {
    panic(err.Error())
  }
  fh, err := os.Open(filename)
  if err != nil {
    panic(err.Error())
  }
  io.Copy(file_writer, fh)
  content_type := body_writer.FormDataContentType()
  body_writer.Close()
  resp, err := http.Post(uploadUrl, content_type, body_buf)
  if err != nil {
    panic(err.Error())
  }
  defer resp.Body.Close()
  resp_body, err := ioutil.ReadAll(resp.Body)
  if err != nil {
    panic(err.Error())
  }
  var ret UploadResult
  err = json.Unmarshal(resp_body, &ret)
  if err != nil {
    panic(err.Error())
  }
  //fmt.Println("Uploaded " + strconv.Itoa(ret.Size) + " Bytes to " + uploadUrl)
  return ret.Size, uploadUrl
}

type SubmitResult struct {
  Fid  string "fid"
  Size int    "size"
}

func submit(files []string)([]SubmitResult) {
  ret, err := assign(len(files))
  if err != nil {
    panic(err)
  }
  results := make([]SubmitResult, len(files))
  for index, file := range files {
    fid := ret.Fid
    if index > 0 {
      fid = fid + "_" + strconv.Itoa(index)
    }
    uploadUrl := "http://" + ret.PublicUrl + "/" + fid
    results[index].Size, _ = upload(file, uploadUrl)
    results[index].Fid = fid
  }
  return results
}

func runUpload(cmd *Command, args []string) bool {
  if len(cmdUpload.Flag.Args()) == 0 {
    return false
  }
  results := submit(flag.Args())
  bytes, _ := json.Marshal(results)
  fmt.Print(string(bytes))
  return true
}
