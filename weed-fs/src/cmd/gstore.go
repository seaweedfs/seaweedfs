package main

import (
	"store"
	"directory"
	"flag"
	"fmt"
	"http"
	"json"
	"log"
	"mime"
	"os"
	"rand"
	"strconv"
	"strings"
)

var (
	port         = flag.Int("port", 8080, "http listen port")
	chunkFolder  = flag.String("dir", "/tmp", "data directory to store files")
	chunkCount   = flag.Int("chunks", 5, "data chunks to store files")
	chunkEnabled = flag.Bool("data", false, "act as a store server")
    chunkServer   = flag.String("cserver", "localhost:8080", "chunk server to store data")
    publicServer   = flag.String("pserver", "localhost:8080", "public server to serve data read")
    metaServer   = flag.String("mserver", "localhost:8080", "metadata server to store mappings")

	metaEnabled  = flag.Bool("meta", false, "act as a directory server")
	metaFolder   = flag.String("mdir", "/tmp", "data directory to store mappings")
)

type Haystack struct {
	store     *store.Store
	directory *directory.Mapper
}

func storeHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		server.GetHandler(w, r)
	case "DELETE":
		server.DeleteHandler(w, r)
	case "POST":
		server.PostHandler(w, r)
	}
}
func (s *Haystack) GetHandler(w http.ResponseWriter, r *http.Request) {
	n := new(store.Needle)
	path := r.URL.Path
	sepIndex := strings.Index(path[1:], "/") + 1
	volumeId, _ := strconv.Atoui64(path[1:sepIndex])
	dotIndex := strings.LastIndex(path, ".")
	n.ParsePath(path[sepIndex+1 : dotIndex])
	ext := path[dotIndex:]

	s.store.Read(volumeId, n)
	w.Header().Set("Content-Type", mime.TypeByExtension(ext))
	w.Write(n.Data)
}
func (s *Haystack) PostHandler(w http.ResponseWriter, r *http.Request) {
    volumeId, _ := strconv.Atoui64(r.FormValue("volumeId"))
	s.store.Write(volumeId, store.NewNeedle(r))
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprint(w, "volumeId=", volumeId, "\n")
}
func (s *Haystack) DeleteHandler(w http.ResponseWriter, r *http.Request) {

}
func dirReadHandler(w http.ResponseWriter, r *http.Request) {
    volumeId, _ := strconv.Atoui64(r.FormValue("volumeId"))
    machineList := server.directory.Get((uint32)(volumeId))
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
    machineList := server.directory.PickForWrite()
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

}

var server *Haystack

func main() {
	flag.Parse()
	if !*chunkEnabled && !*metaEnabled {
		fmt.Fprintf(os.Stdout, "Act as both a store server and a directory server\n")
	}
	server = new(Haystack)
	if *chunkEnabled {
		fmt.Fprintf(os.Stdout, "Chunk data stored in %s\n", *chunkFolder)
		server.store = store.NewStore(*chunkServer, *publicServer, *chunkFolder)
		defer server.store.Close()
		http.HandleFunc("/", storeHandler)
	}
	if *metaEnabled {
		server.directory = directory.NewMapper(*metaFolder, "directory")
		defer server.directory.Save()
        http.HandleFunc("/dir/read", dirReadHandler)
        http.HandleFunc("/dir/write", dirWriteHandler)
        http.HandleFunc("/dir/join", dirJoinHandler)
	}
	
	server.store.Join(*metaServer)

	log.Println("Serving at http://127.0.0.1:" + strconv.Itoa(*port))
	http.ListenAndServe(":"+strconv.Itoa(*port), nil)
}
