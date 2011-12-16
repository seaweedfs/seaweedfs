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
)

var (
	port         = flag.Int("port", 9333, "http listen port")
	metaFolder  = flag.String("mdir", "/tmp", "data directory to store mappings")
    mapper *directory.Mapper
)

func dirReadHandler(w http.ResponseWriter, r *http.Request) {
	volumeId, _ := strconv.Atoui64(r.FormValue("volumeId"))
	machineList := mapper.Get((uint32)(volumeId))
	x := rand.Intn(len(machineList))
	machine := machineList[x]
	bytes, _ := json.Marshal(machine)
	callback := r.FormValue("callback")
	w.Header().Set("Content-Type", "application/javascript")
	if callback == "" {
		w.Write(bytes)
	} else {
		w.Write([]uint8(callback))
		w.Write([]uint8("("))
		w.Write(bytes)
		w.Write([]uint8(")"))
	}
}
func dirWriteHandler(w http.ResponseWriter, r *http.Request) {
	machineList := mapper.PickForWrite()
	bytes, _ := json.Marshal(machineList)
	callback := r.FormValue("callback")
	w.Header().Set("Content-Type", "application/javascript")
	if callback == "" {
		w.Write(bytes)
	} else {
		w.Write([]uint8(callback))
		w.Write([]uint8("("))
		w.Write(bytes)
		w.Write([]uint8(")"))
	}
}
func dirJoinHandler(w http.ResponseWriter, r *http.Request) {
	s := r.FormValue("server")
	publicServer := r.FormValue("publicServer")
	volumes := make([]storage.VolumeStat, 0)
	json.Unmarshal([]byte(r.FormValue("volumes")), volumes)
	mapper.Add(directory.NewMachine(s, publicServer), volumes)
}
func dirStatusHandler(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "text/plain")
    bytes, _ := json.Marshal(mapper)
    fmt.Fprint(w, bytes)
}

func main() {
	flag.Parse()
	mapper = directory.NewMapper(*metaFolder, "directory")
	defer mapper.Save()
	http.HandleFunc("/dir/read", dirReadHandler)
	http.HandleFunc("/dir/write", dirWriteHandler)
	http.HandleFunc("/dir/join", dirJoinHandler)
    http.HandleFunc("/dir/status", dirStatusHandler)

	log.Println("Start directory service at http://127.0.0.1:" + strconv.Itoa(*port))
	e := http.ListenAndServe(":"+strconv.Itoa(*port), nil)
    if e!=nil {
        log.Fatalf("Fail to start:",e.String())
    }

}
