package main

import (
	"storage"
	"flag"
	"fmt"
	"http"
	"log"
	"mime"
	"strconv"
	"strings"
)

var (
	port         = flag.Int("port", 8080, "http listen port")
	chunkFolder  = flag.String("dir", "/tmp", "data directory to store files")
	chunkCount   = flag.Int("chunks", 5, "data chunks to store files")
	publicServer = flag.String("pserver", "localhost:8080", "public server to serve data read")
	metaServer   = flag.String("mserver", "localhost:9333", "metadata server to store mappings")

    store     *storage.Store
)


func storeHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		GetHandler(w, r)
	case "DELETE":
		DeleteHandler(w, r)
	case "POST":
		PostHandler(w, r)
	}
}
func GetHandler(w http.ResponseWriter, r *http.Request) {
	n := new(storage.Needle)
	path := r.URL.Path
	sepIndex := strings.Index(path[1:], "/") + 1
	volumeId, _ := strconv.Atoui64(path[1:sepIndex])
	dotIndex := strings.LastIndex(path, ".")
	n.ParsePath(path[sepIndex+1 : dotIndex])
	ext := path[dotIndex:]

	store.Read(volumeId, n)
	w.Header().Set("Content-Type", mime.TypeByExtension(ext))
	w.Write(n.Data)
}
func PostHandler(w http.ResponseWriter, r *http.Request) {
	volumeId, _ := strconv.Atoui64(r.FormValue("volumeId"))
	store.Write(volumeId, storage.NewNeedle(r))
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprint(w, "volumeId=", volumeId, "\n")
}
func DeleteHandler(w http.ResponseWriter, r *http.Request) {

}

func main() {
	flag.Parse()
	store = storage.NewStore(*port, *publicServer, *chunkFolder)
	defer store.Close()
	http.HandleFunc("/", storeHandler)

	store.Join(*metaServer)
	log.Println("store joined at", *metaServer)

	log.Println("Start storage service at http://127.0.0.1:" + strconv.Itoa(*port))
	e := http.ListenAndServe(":"+strconv.Itoa(*port), nil)
	if e!=nil {
	    log.Fatalf("Fail to start:",e.String())
	}

}
