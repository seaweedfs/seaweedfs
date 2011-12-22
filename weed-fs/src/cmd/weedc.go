package main

import (
	"storage"
	"flag"
	"fmt"
	"http"
	"json"
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

	store *storage.Store
)

func statusHandler(w http.ResponseWriter, r *http.Request) {
    writeJson(w, r, store.Status())
}
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
	sepIndex := strings.LastIndex(path, "/")
	commaIndex := strings.LastIndex(path[sepIndex:], ",")
	dotIndex := strings.LastIndex(path[sepIndex:], ".")
	fid := path[commaIndex+1:]
	if dotIndex > 0 {
		fid = path[commaIndex+1 : dotIndex]
	}
	if commaIndex <= 0 {
        log.Println("unknown file id", path[sepIndex+1 : commaIndex])
        return
	}
	volumeId, _ := strconv.Atoui64(path[sepIndex+1 : commaIndex])
	n.ParsePath(fid)

	store.Read(volumeId, n)
	if dotIndex > 0 {
		ext := path[dotIndex:]
		w.Header().Set("Content-Type", mime.TypeByExtension(ext))
	}
	w.Write(n.Data)
}
func PostHandler(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	commaIndex := strings.LastIndex(path, ",")
	sepIndex := strings.LastIndex(path[:commaIndex], "/")
	volumeId, e := strconv.Atoui64(path[sepIndex+1 : commaIndex])
	if e != nil {
		writeJson(w, r, e)
	} else {
		store.Write(volumeId, storage.NewNeedle(r))
		writeJson(w, r, make(map[string]string))
	}
}
func DeleteHandler(w http.ResponseWriter, r *http.Request) {

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
	//TODO: now default to 1G, this value should come from server?
	store = storage.NewStore(*port, *publicServer, *chunkFolder, 1024*1024*1024, *chunkCount)
	defer store.Close()
	http.HandleFunc("/", storeHandler)
    http.HandleFunc("/status", statusHandler)

	store.Join(*metaServer)
	log.Println("store joined at", *metaServer)

	log.Println("Start storage service at http://127.0.0.1:" + strconv.Itoa(*port))
	e := http.ListenAndServe(":"+strconv.Itoa(*port), nil)
	if e != nil {
		log.Fatalf("Fail to start:", e.String())
	}

}
