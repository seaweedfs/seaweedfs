package main

import (
	"pkg/directory"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"pkg/storage"
	"strconv"
	"strings"
)

var (
	port              = flag.Int("port", 9333, "http listen port")
	metaFolder        = flag.String("mdir", "/tmp", "data directory to store mappings")
	capacity          = flag.Int("capacity", 100, "maximum number of volumes to hold")
	mapper            *directory.Mapper
	IsDebug           = flag.Bool("debug", false, "verbose debug information")
	volumeSizeLimitMB = flag.Uint("volumeSizeLimitMB", 32*1024, "Default Volume Size in MegaBytes")
)

func dirLookupHandler(w http.ResponseWriter, r *http.Request) {
	vid := r.FormValue("volumeId")
	commaSep := strings.Index(vid, ",")
	if commaSep > 0 {
		vid = vid[0:commaSep]
	}
	volumeId, _ := strconv.ParseUint(vid, 10, 64)
	machine, e := mapper.Get(uint32(volumeId))
	if e == nil {
		writeJson(w, r, machine.Server)
	} else {
		log.Println("Invalid volume id", volumeId)
		writeJson(w, r, map[string]string{"error": "volume id " + strconv.FormatUint(volumeId, 10) + " not found"})
	}
}
func dirAssignHandler(w http.ResponseWriter, r *http.Request) {
	fid, machine, err := mapper.PickForWrite()
	if err == nil {
		writeJson(w, r, map[string]string{"fid": fid, "url": machine.Url, "publicUrl":machine.PublicUrl})
	} else {
		log.Println(err)
		writeJson(w, r, map[string]string{"error": err.Error()})
	}
}
func dirJoinHandler(w http.ResponseWriter, r *http.Request) {
	s := r.RemoteAddr[0:strings.Index(r.RemoteAddr, ":")+1] + r.FormValue("port")
	publicUrl := r.FormValue("publicUrl")
	volumes := new([]storage.VolumeInfo)
	json.Unmarshal([]byte(r.FormValue("volumes")), volumes)
	if *IsDebug {
		log.Println(s, "volumes", r.FormValue("volumes"))
	}
	mapper.Add(*directory.NewMachine(s, publicUrl, *volumes))
}
func dirStatusHandler(w http.ResponseWriter, r *http.Request) {
	writeJson(w, r, mapper)
}
func writeJson(w http.ResponseWriter, r *http.Request, obj interface{}) {
	w.Header().Set("Content-Type", "application/javascript")
	bytes, _ := json.Marshal(obj)
	callback := r.FormValue("callback")
	if callback == "" {
		w.Write(bytes)
	} else {
		w.Write([]uint8(callback))
		w.Write([]uint8("("))
		fmt.Fprint(w, string(bytes))
		w.Write([]uint8(")"))
	}
}

func main() {
	flag.Parse()
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

}
