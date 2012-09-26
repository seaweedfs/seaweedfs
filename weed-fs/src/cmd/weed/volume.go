package main

import (
	"bytes"
	"log"
	"math/rand"
	"mime"
	"net/http"
	"os"
	"pkg/operation"
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
	UsageLine: "volume -port=8080 -dir=/tmp -max=5 -ip=server_name -mserver=localhost:9333",
	Short:     "start a volume server",
	Long: `start a volume server to provide storage spaces

  `,
}

var (
	vport          = cmdVolume.Flag.Int("port", 8080, "http listen port")
	volumeFolder   = cmdVolume.Flag.String("dir", "/tmp", "directory to store data files")
	ip             = cmdVolume.Flag.String("ip", "localhost", "ip or server name")
	publicUrl      = cmdVolume.Flag.String("publicUrl", "", "Publicly accessible <ip|server_name>:<port>")
	masterNode     = cmdVolume.Flag.String("mserver", "localhost:9333", "master server location")
	vpulse         = cmdVolume.Flag.Int("pulseSeconds", 5, "number of seconds between heartbeats")
	maxVolumeCount = cmdVolume.Flag.Int("max", 5, "maximum number of volumes")

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
	volumeId, err := storage.NewVolumeId(vid)
	if err != nil {
		if *IsDebug {
			log.Println("parsing error:", err, r.URL.Path)
		}
		return
	}
	n.ParsePath(fid)

	if *IsDebug {
		log.Println("volume", volumeId, "reading", n)
	}
	if !store.HasVolume(volumeId) {
		lookupResult, err := operation.Lookup(*masterNode, volumeId)
		if *IsDebug {
			log.Println("volume", volumeId, "found on", lookupResult, "error", err)
		}
		if err == nil {
			http.Redirect(w, r, "http://"+lookupResult.Locations[0].PublicUrl+r.URL.Path, http.StatusMovedPermanently)
		} else {
			if *IsDebug {
				log.Println("lookup error:", err, r.URL.Path)
			}
			w.WriteHeader(http.StatusNotFound)
		}
		return
	}
	cookie := n.Cookie
	count, e := store.Read(volumeId, n)
	if *IsDebug {
		log.Println("read bytes", count, "error", e)
	}
	if e != nil || count <= 0 {
		log.Println("read error:", e)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if n.Cookie != cookie {
		log.Println("request with unmaching cookie from ", r.RemoteAddr, "agent", r.UserAgent())
		w.WriteHeader(http.StatusNotFound)
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
	r.ParseForm()
	vid, _, _ := parseURLPath(r.URL.Path)
	volumeId, e := storage.NewVolumeId(vid)
	if e != nil {
		writeJson(w, r, e)
	} else {
		needle, filename, ne := storage.NewNeedle(r)
		if ne != nil {
			writeJson(w, r, ne)
		} else {
			ret := store.Write(volumeId, needle)
			if ret > 0 || !store.HasVolume(volumeId) { //send to other replica locations
				if r.FormValue("type") != "standard" {
					if lookupResult, lookupErr := operation.Lookup(*masterNode, volumeId); lookupErr == nil {
						sendFunc := func(background bool) {
							postContentFunc := func(location operation.Location) bool {
								operation.Upload("http://"+location.Url+r.URL.Path+"?type=standard", filename, bytes.NewReader(needle.Data))
								return true
							}
							for _, location := range lookupResult.Locations {
								if location.Url != (*ip+":"+strconv.Itoa(*vport)) {
									if background {
										go postContentFunc(location)
									} else {
										postContentFunc(location)
									}
								}
							}
						}
						waitTime, err := strconv.Atoi(r.FormValue("wait"))
						sendFunc(err == nil && waitTime > 0)
					} else {
						log.Println("Failed to lookup for", volumeId, lookupErr.Error())
					}
				}
				w.WriteHeader(http.StatusCreated)
			}
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
	fileInfo, err := os.Stat(*volumeFolder)
	//TODO: now default to 1G, this value should come from server?
	if err != nil {
		log.Fatalf("No Existing Folder:%s", *volumeFolder)
	}
	if !fileInfo.IsDir() {
		log.Fatalf("Volume Folder should not be a file:%s", *volumeFolder)
	}
	perm := fileInfo.Mode().Perm()
	log.Println("Volume Folder permission:", perm)
	
	if *publicUrl == "" {
	  *publicUrl = *ip + ":" + strconv.Itoa(*vport)
	}

	store = storage.NewStore(*vport, *ip, *publicUrl, *volumeFolder, *maxVolumeCount)
	defer store.Close()
	http.HandleFunc("/", storeHandler)
	http.HandleFunc("/status", statusHandler)
	http.HandleFunc("/admin/assign_volume", assignVolumeHandler)

	go func() {
		for {
			store.Join(*masterNode)
			time.Sleep(time.Duration(float32(*vpulse*1e3)*(1+rand.Float32())) * time.Millisecond)
		}
	}()
	log.Println("store joined at", *masterNode)

	log.Println("Start storage service at http://"+*ip+":"+strconv.Itoa(*vport))
	e := http.ListenAndServe(":"+strconv.Itoa(*vport), nil)
	if e != nil {
		log.Fatalf("Fail to start:%s", e.Error())
	}
	return true
}
