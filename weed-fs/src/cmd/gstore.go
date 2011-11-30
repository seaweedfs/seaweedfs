package main

import (
	"store"
	"directory"
	"flag"
	"fmt"
	"http"
	"log"
	"mime"
	"os"
	"strconv"
	"strings"
)

var (
	port         = flag.Int("port", 8080, "http listen port")
	chunkFolder  = flag.String("dir", "/tmp", "data directory to store files")
	chunkCount   = flag.Int("chunks", 5, "data chunks to store files")
	chunkEnabled = flag.Bool("data", false, "act as a store server")
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
	volumeId, _ := strconv.Atoi(path[1:sepIndex])
	dotIndex := strings.LastIndex(path, ".")
	n.ParsePath(path[sepIndex+1 : dotIndex])
	ext := path[dotIndex:]

	s.store.Read(volumeId, n)
	w.Header().Set("Content-Type", mime.TypeByExtension(ext))
	w.Write(n.Data)
}
func (s *Haystack) PostHandler(w http.ResponseWriter, r *http.Request) {
	volumeId := s.store.Write(store.NewNeedle(r))
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprint(w, "volumeId=", volumeId, "\n")
}
func (s *Haystack) DeleteHandler(w http.ResponseWriter, r *http.Request) {

}
func directoryHandler(w http.ResponseWriter, r *http.Request) {
}

var server *Haystack

func main() {
	flag.Parse()
	if !*chunkEnabled && !*metaEnabled {
		fmt.Fprintf(os.Stderr, "Need to act as either a store server or a directory server, or both\n")
		flag.PrintDefaults()
		os.Exit(-1)
	}
	server = new(Haystack)
	if *chunkEnabled {
		fmt.Fprintf(os.Stdout, "Chunk data stored in %s\n", *chunkFolder)
		server.store = store.NewStore(*chunkFolder, *chunkCount)
		defer server.store.Close()
		http.HandleFunc("/", storeHandler)
	}
	if *metaEnabled {
		server.directory = directory.NewMapper(*metaFolder, "directory")
		defer server.directory.Save()
        http.HandleFunc("/directory", directoryHandler)
	}

	log.Println("Serving at http://127.0.0.1:" + strconv.Itoa(*port))
	http.ListenAndServe(":"+strconv.Itoa(*port), nil)
}
