package main

import (
	"storage"
	"directory"
	"flag"
	"fmt"
	"http"
	"json"
	"log"
	"strconv"
	"strings"
)

var (
	port       = flag.Int("port", 9333, "http listen port")
	metaFolder = flag.String("mdir", "/tmp", "data directory to store mappings")
	capacity   = flag.Int("capacity", 100, "maximum number of volumes to hold")
	mapper     *directory.Mapper
    IsDebug   = flag.Bool("debug", false, "verbose debug information")
)

func dirLookupHandler(w http.ResponseWriter, r *http.Request) {
	vid := r.FormValue("volumeId")
	commaSep := strings.Index(vid, ",")
	if commaSep > 0 {
		vid = vid[0:commaSep]
	}
	volumeId, _ := strconv.Atoui64(vid)
	machine := mapper.Get(uint32(volumeId))
	writeJson(w, r, machine.Server)
}
func dirWriteHandler(w http.ResponseWriter, r *http.Request) {
	_, machine := mapper.PickForWrite()
	writeJson(w, r, machine)
}
func dirAssignHandler(w http.ResponseWriter, r *http.Request) {
	fid, machine := mapper.PickForWrite()
	writeJson(w, r, map[string]string{"fid": fid, "url": machine.Url})
}
func dirJoinHandler(w http.ResponseWriter, r *http.Request) {
	s := r.RemoteAddr[0:strings.Index(r.RemoteAddr, ":")+1] + r.FormValue("port")
	publicUrl := r.FormValue("publicUrl")
	volumes := new([]storage.VolumeInfo)
	json.Unmarshal([]byte(r.FormValue("volumes")), volumes)
	log.Println("Recieved updates from", s, "volumes", r.FormValue("volumes"))
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
	//log.Println("JSON Response", string(bytes))
}

func main() {
	flag.Parse()
	mapper = directory.NewMapper(*metaFolder, "directory")
	http.HandleFunc("/dir/assign", dirAssignHandler)
	http.HandleFunc("/dir/lookup", dirLookupHandler)
	http.HandleFunc("/dir/write", dirWriteHandler)
	http.HandleFunc("/dir/join", dirJoinHandler)
	http.HandleFunc("/dir/status", dirStatusHandler)

	log.Println("Start directory service at http://127.0.0.1:" + strconv.Itoa(*port))
	e := http.ListenAndServe(":"+strconv.Itoa(*port), nil)
	if e != nil {
		log.Fatalf("Fail to start:", e.String())
	}

}
