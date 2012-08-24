package main

import (
  "pkg/directory"
  "encoding/json"
  "log"
  "net/http"
  "pkg/topology"
  "strconv"
  "strings"
)

func init() {
  cmdMaster.Run = runMaster // break init cycle
  IsDebug     = cmdMaster.Flag.Bool("debug", false, "enable debug mode")
  port        = cmdMaster.Flag.Int("port", 8080, "http listen port")
}

var cmdMaster = &Command{
  UsageLine: "master -port=9333",
  Short:     "start a master server",
  Long: `start a master server to provide volume=>location mapping service
  and sequence number of file ids

  `,
}

var (
  metaFolder        = cmdMaster.Flag.String("mdir", "/tmp", "data directory to store mappings")
  capacity          = cmdMaster.Flag.Int("capacity", 100, "maximum number of volumes to hold")
  mapper            *directory.Mapper
  volumeSizeLimitMB = cmdMaster.Flag.Uint("volumeSizeLimitMB", 32*1024, "Default Volume Size in MegaBytes")
)

func dirLookupHandler(w http.ResponseWriter, r *http.Request) {
  vid := r.FormValue("volumeId")
  commaSep := strings.Index(vid, ",")
  if commaSep > 0 {
    vid = vid[0:commaSep]
  }
  volumeId, _ := strconv.ParseUint(vid, 10, 64)
  machine, e := mapper.Get(topology.VolumeId(volumeId))
  if e == nil {
    writeJson(w, r, machine.Server)
  } else {
    log.Println("Invalid volume id", volumeId)
    writeJson(w, r, map[string]string{"error": "volume id " + strconv.FormatUint(volumeId, 10) + " not found"})
  }
}
func dirAssignHandler(w http.ResponseWriter, r *http.Request) {
  c:=r.FormValue("count")
  fid, count, machine, err := mapper.PickForWrite(c)
  if err == nil {
    writeJson(w, r, map[string]string{"fid": fid, "url": machine.Url, "publicUrl":machine.PublicUrl, "count":strconv.Itoa(count)})
  } else {
    log.Println(err)
    writeJson(w, r, map[string]string{"error": err.Error()})
  }
}
func dirJoinHandler(w http.ResponseWriter, r *http.Request) {
  s := r.RemoteAddr[0:strings.Index(r.RemoteAddr, ":")+1] + r.FormValue("port")
  publicUrl := r.FormValue("publicUrl")
  volumes := new([]topology.VolumeInfo)
  json.Unmarshal([]byte(r.FormValue("volumes")), volumes)
  if *IsDebug {
    log.Println(s, "volumes", r.FormValue("volumes"))
  }
  mapper.Add(*directory.NewMachine(s, publicUrl, *volumes))
}
func dirStatusHandler(w http.ResponseWriter, r *http.Request) {
  writeJson(w, r, mapper)
}

func runMaster(cmd *Command, args []string) bool {
  log.Println("Volume Size Limit is", *volumeSizeLimitMB, "MB")
  mapper = directory.NewMapper(*metaFolder, "directory", uint64(*volumeSizeLimitMB)*1024*1024)
  http.HandleFunc("/dir/assign", dirAssignHandler)
  http.HandleFunc("/dir/lookup", dirLookupHandler)
  http.HandleFunc("/dir/join", dirJoinHandler)
  http.HandleFunc("/dir/status", dirStatusHandler)

  log.Println("Start directory service at http://127.0.0.1:" + strconv.Itoa(*port))
  e := http.ListenAndServe(":"+strconv.Itoa(*port), nil)
  if e != nil {
    log.Fatal("Fail to start:", e)
  }
  return true
}
