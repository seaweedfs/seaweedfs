package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"mime"
	"net/http"
	"pkg/storage"
	"strconv"
	"strings"
	"time"
)

func init() {
	cmdVolume.Run = runVolume // break init cycle
	IsDebug = cmdVolume.Flag.Bool("debug", false, "enable debug mode")
}

var cmdVolume = &Command{
	UsageLine: "volume -port=8080 -dir=/tmp -volumes=0-99 -publicUrl=server_name:8080 -mserver=localhost:9333",
	Short:     "start a volume server",
	Long: `start a volume server to provide storage spaces

  `,
}

var (
	vport       = cmdVolume.Flag.Int("port", 8080, "http listen port")
	chunkFolder = cmdVolume.Flag.String("dir", "/tmp", "data directory to store files")
	volumes     = cmdVolume.Flag.String("volumes", "0,1-3,4", "comma-separated list of volume ids or range of ids")
	publicUrl   = cmdVolume.Flag.String("publicUrl", "localhost:8080", "public url to serve data read")
	masterNode  = cmdVolume.Flag.String("mserver", "localhost:9333", "master directory server to store mappings")
	vpulse      = cmdVolume.Flag.Int("pulseSeconds", 5, "number of seconds between heartbeats")

	store *storage.Store
)

func statusHandler(w http.ResponseWriter, r *http.Request) {
	writeJson(w, r, store.Status())
}
func assignVolumeHandler(w http.ResponseWriter, r *http.Request) {
	err := store.AddVolume(r.FormValue("volume"), r.FormValue("replicationType"))
	if err == nil {
		writeJson(w, r, map[string]string{"error": ""})
	} else {
		writeJson(w, r, map[string]string{"error": err.Error()})
	}
  if *IsDebug {
    log.Println("volume =", r.FormValue("volume"), ", replicationType =", r.FormValue("replicationType"), ", error =", err)
  }
}
func setVolumeLocationsHandler(w http.ResponseWriter, r *http.Request) {
	if *IsDebug {
		log.Println("volumeLocationsList =", r.FormValue("volumeLocationsList"))
	}
	volumeLocationsList := new([]storage.VolumeLocations)
	err := json.Unmarshal([]byte(r.FormValue("volumeLocationsList")), volumeLocationsList)
	if err == nil {
		err = store.SetVolumeLocations(*volumeLocationsList)
	}
	if err == nil {
		writeJson(w, r, map[string]string{"error": ""})
	} else {
		writeJson(w, r, map[string]string{"error": err.Error()})
	}
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
	volumeId, _ := storage.NewVolumeId(vid)
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
		mtype := mime.TypeByExtension(ext)
		w.Header().Set("Content-Type", mtype)
		if storage.IsCompressable(ext, mtype) {
			if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
				w.Header().Set("Content-Encoding", "gzip")
			} else {
				n.Data = storage.UnGzipData(n.Data)
			}
		}
	}
	w.Write(n.Data)
}
func PostHandler(w http.ResponseWriter, r *http.Request) {
	vid, _, _ := parseURLPath(r.URL.Path)
	volumeId, e := storage.NewVolumeId(vid)
	if e != nil {
		writeJson(w, r, e)
	} else {
		needle, ne := storage.NewNeedle(r)
		if ne != nil {
			writeJson(w, r, ne)
		} else {
			ret := store.Write(volumeId, needle)
			m := make(map[string]uint32)
			m["size"] = ret
			writeJson(w, r, m)
		}
	}
}
func DeleteHandler(w http.ResponseWriter, r *http.Request) {
	n := new(storage.Needle)
	vid, fid, _ := parseURLPath(r.URL.Path)
	volumeId, _ := storage.NewVolumeId(vid)
	n.ParsePath(fid)

	if *IsDebug {
		log.Println("deleting", n)
	}

	cookie := n.Cookie
	count, ok := store.Read(volumeId, n)

	if ok != nil {
		m := make(map[string]uint32)
		m["size"] = 0
		writeJson(w, r, m)
		return
	}

	if n.Cookie != cookie {
		log.Println("delete with unmaching cookie from ", r.RemoteAddr, "agent", r.UserAgent())
		return
	}

	n.Size = 0
	store.Delete(volumeId, n)
	m := make(map[string]uint32)
	m["size"] = uint32(count)
	writeJson(w, r, m)
}
func parseURLPath(path string) (vid, fid, ext string) {
	sepIndex := strings.LastIndex(path, "/")
	commaIndex := strings.LastIndex(path[sepIndex:], ",")
	if commaIndex <= 0 {
		if "favicon.ico" != path[sepIndex+1:] {
			log.Println("unknown file id", path[sepIndex+1:])
		}
		return
	}
	dotIndex := strings.LastIndex(path[sepIndex:], ".")
	vid = path[sepIndex+1 : commaIndex]
	fid = path[commaIndex+1:]
	ext = ""
	if dotIndex > 0 {
		fid = path[commaIndex+1 : dotIndex]
		ext = path[dotIndex:]
	}
	return
}

func runVolume(cmd *Command, args []string) bool {
	//TODO: now default to 1G, this value should come from server?
	store = storage.NewStore(*vport, *publicUrl, *chunkFolder, *volumes)
	defer store.Close()
	http.HandleFunc("/", storeHandler)
	http.HandleFunc("/status", statusHandler)
	http.HandleFunc("/admin/assign_volume", assignVolumeHandler)
	http.HandleFunc("/admin/set_volume_locations_list", setVolumeLocationsHandler)

	go func() {
		for {
			store.Join(*masterNode)
			time.Sleep(time.Duration(float32(*vpulse*1e3)*(1+rand.Float32())) * time.Millisecond)
		}
	}()
	log.Println("store joined at", *masterNode)

	log.Println("Start storage service at http://127.0.0.1:"+strconv.Itoa(*vport), "public url", *publicUrl)
	e := http.ListenAndServe(":"+strconv.Itoa(*vport), nil)
	if e != nil {
		log.Fatalf("Fail to start:", e)
	}
	return true
}
