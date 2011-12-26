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
	"time"
)

var (
	port         = flag.Int("port", 8080, "http listen port")
	chunkFolder  = flag.String("dir", "/tmp", "data directory to store files")
	volumes   = flag.String("volumes", "0,1-3,4", "comma-separated list of volume ids or range of ids")
	publicUrl = flag.String("publicUrl", "localhost:8080", "public url to serve data read")
	metaServer   = flag.String("mserver", "localhost:9333", "master directory server to store mappings")
	IsDebug      = flag.Bool("debug", false, "enable debug mode")
	pulse        = flag.Int("pulseSeconds", 5, "number of seconds between heartbeats")

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
	vid, fid, ext := parseURLPath(r.URL.Path)
	volumeId, _ := strconv.Atoui64(vid)
	n.ParsePath(fid)

	if *IsDebug {
		log.Println("volume", volumeId, "reading", n)
	}
	cookie := n.Cookie
	count, e := store.Read(volumeId, n)
	if *IsDebug {
		log.Println("read bytes", count, "error", e)
	}
	if n.Cookie != cookie {
		log.Println("request with unmaching cookie from ", r.RemoteAddr, "agent", r.UserAgent())
		return
	}
	if ext != "" {
		w.Header().Set("Content-Type", mime.TypeByExtension(ext))
	}
	w.Write(n.Data)
}
func PostHandler(w http.ResponseWriter, r *http.Request) {
	vid, _, _ := parseURLPath(r.URL.Path)
	volumeId, e := strconv.Atoui64(vid)
	if e != nil {
		writeJson(w, r, e)
	} else {
		ret := store.Write(volumeId, storage.NewNeedle(r))
		m := make(map[string]uint32)
		m["size"] = ret
		writeJson(w, r, m)
	}
}
func DeleteHandler(w http.ResponseWriter, r *http.Request) {
	n := new(storage.Needle)
	vid, fid, _ := parseURLPath(r.URL.Path)
	volumeId, _ := strconv.Atoui64(vid)
	n.ParsePath(fid)

	cookie := n.Cookie
	count, _ := store.Read(volumeId, n)

	if n.Cookie != cookie {
		log.Println("delete with unmaching cookie from ", r.RemoteAddr, "agent", r.UserAgent())
		return
	}

	n.Size = 0
	store.Write(volumeId, n)
	m := make(map[string]uint32)
	m["size"] = uint32(count)
	writeJson(w, r, m)
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
func parseURLPath(path string) (vid, fid, ext string) {
	sepIndex := strings.LastIndex(path, "/")
	commaIndex := strings.LastIndex(path[sepIndex:], ",")
	dotIndex := strings.LastIndex(path[sepIndex:], ".")
	vid = path[sepIndex+1 : commaIndex]
	fid = path[commaIndex+1:]
	ext = ""
	if dotIndex > 0 {
		fid = path[commaIndex+1 : dotIndex]
		ext = path[dotIndex+1:]
	}
	if commaIndex <= 0 {
		log.Println("unknown file id", path[sepIndex+1:commaIndex])
		return
	}
	return
}

func main() {
	flag.Parse()
	//TODO: now default to 1G, this value should come from server?
	store = storage.NewStore(*port, *publicUrl, *chunkFolder, *volumes)
	defer store.Close()
	http.HandleFunc("/", storeHandler)
	http.HandleFunc("/status", statusHandler)

	go func() {
		for {
			store.Join(*metaServer)
			time.Sleep(int64(*pulse) * 1e9)
		}
	}()
	log.Println("store joined at", *metaServer)

	log.Println("Start storage service at http://127.0.0.1:" + strconv.Itoa(*port))
	e := http.ListenAndServe(":"+strconv.Itoa(*port), nil)
	if e != nil {
		log.Fatalf("Fail to start:", e.String())
	}

}
