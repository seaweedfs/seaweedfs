package main

import (
	"storage"
	"directory"
	"flag"
	"fmt"
	"http"
	"json"
	"log"
	"rand"
	"strconv"
	"strings"
)

var (
	port       = flag.Int("port", 9333, "http listen port")
	metaFolder = flag.String("mdir", "/tmp", "data directory to store mappings")
	capacity   = flag.Int("capacity", 100, "maximum number of volumes to hold")
	mapper     *directory.Mapper
	
)

func dirReadHandler(w http.ResponseWriter, r *http.Request) {
	volumeId, _ := strconv.Atoui64(r.FormValue("volumeId"))
	machine := mapper.Get(volumeId)
	writeJson(w, r, machine.Server)
}
func dirWriteHandler(w http.ResponseWriter, r *http.Request) {
	_, machine := mapper.PickForWrite()
	writeJson(w, r, machine)
}
func dirPickHandler(w http.ResponseWriter, r *http.Request) {
    vid, machine := mapper.PickForWrite()
    hashcode := rand.Uint32()
    fid := strconv.Uitoa64(vid) + "," + strconv.Uitoa64(mapper.NextFileId())+","+strconv.Uitoa64(uint64(hashcode))
    writeJson(w, r, map[string]string{"fid":fid,"url":machine.Url})
}
func dirJoinHandler(w http.ResponseWriter, r *http.Request) {
	s := r.RemoteAddr[0:strings.Index(r.RemoteAddr, ":")+1] + r.FormValue("port")
	publicServer := r.FormValue("publicServer")
	volumes := new([]storage.VolumeInfo)
	json.Unmarshal([]byte(r.FormValue("volumes")), volumes)
	capacity, _ := strconv.Atoi(r.FormValue("capacity"))
	log.Println("Recieved joining request from remote address", s, "capacity=", capacity, "volumes", r.FormValue("volumes"))
	vids := mapper.Add(*directory.NewMachine(s, publicServer, *volumes, capacity))
	writeJson(w, r, vids)
}
func dirStatusHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	bytes, _ := json.Marshal(mapper)
	fmt.Fprint(w, string(bytes))
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
	mapper = directory.NewMapper(*metaFolder, "directory", *capacity)
	defer mapper.Save()
	http.HandleFunc("/dir/read", dirReadHandler)
	http.HandleFunc("/dir/write", dirWriteHandler)
    http.HandleFunc("/dir/pick", dirPickHandler)
	http.HandleFunc("/dir/join", dirJoinHandler)
	http.HandleFunc("/dir/status", dirStatusHandler)

	log.Println("Start directory service at http://127.0.0.1:" + strconv.Itoa(*port))
	e := http.ListenAndServe(":"+strconv.Itoa(*port), nil)
	if e != nil {
		log.Fatalf("Fail to start:", e.String())
	}

}
