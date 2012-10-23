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
	"runtime"
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
	vpulse         = cmdVolume.Flag.Int("pulseSeconds", 5, "number of seconds between heartbeats, must be smaller than the master's setting")
	maxVolumeCount = cmdVolume.Flag.Int("max", 5, "maximum number of volumes")
	vReadTimeout   = cmdVolume.Flag.Int("readTimeout", 1, "connection read timeout in seconds")
	vMaxCpu        = cmdVolume.Flag.Int("maxCpu", 0, "maximum number of CPUs. 0 means all available CPUs")

	store *storage.Store
)

func statusHandler(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]interface{})
	m["Version"] = VERSION
	m["Volumes"] = store.Status()
	writeJson(w, r, m)
}
func assignVolumeHandler(w http.ResponseWriter, r *http.Request) {
	err := store.AddVolume(r.FormValue("volume"), r.FormValue("replicationType"))
	if err == nil {
		writeJson(w, r, map[string]string{"error": ""})
	} else {
		writeJson(w, r, map[string]string{"error": err.Error()})
	}
	debug("volume =", r.FormValue("volume"), ", replicationType =", r.FormValue("replicationType"), ", error =", err)
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
		debug("parsing error:", err, r.URL.Path)
		return
	}
	n.ParsePath(fid)

	debug("volume", volumeId, "reading", n)
	if !store.HasVolume(volumeId) {
		lookupResult, err := operation.Lookup(*masterNode, volumeId)
		debug("volume", volumeId, "found on", lookupResult, "error", err)
		if err == nil {
			http.Redirect(w, r, "http://"+lookupResult.Locations[0].PublicUrl+r.URL.Path, http.StatusMovedPermanently)
		} else {
			debug("lookup error:", err, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
		return
	}
	cookie := n.Cookie
	count, e := store.Read(volumeId, n)
	debug("read bytes", count, "error", e)
	if e != nil || count <= 0 {
		debug("read error:", e, r.URL.Path)
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
		if storage.IsGzippable(ext, mtype) {
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
			errorStatus := ""
			if ret > 0 || !store.HasVolume(volumeId) { //send to other replica locations
				if r.FormValue("type") != "standard" {
					if !distributedOperation(volumeId, func(location operation.Location) bool {
						_, err := operation.Upload("http://"+location.Url+r.URL.Path+"?type=standard", filename, bytes.NewReader(needle.Data))
						return err == nil
					}) {
						ret = 0
						errorStatus = "Failed to write to replicas for volume " + volumeId.String()
					}
				}
			} else {
				errorStatus = "Failed to write to local disk"
			}
			m := make(map[string]interface{})
			if errorStatus == "" {
				w.WriteHeader(http.StatusCreated)
			} else {
				store.Delete(volumeId, needle)
				distributedOperation(volumeId, func(location operation.Location) bool {
					return nil == operation.Delete("http://"+location.Url+r.URL.Path+"?type=standard")
				})
				w.WriteHeader(http.StatusInternalServerError)
				m["error"] = errorStatus
			}
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

	debug("deleting", n)

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
	ret := store.Delete(volumeId, n)

	if ret > 0 || !store.HasVolume(volumeId) { //send to other replica locations
		if r.FormValue("type") != "standard" {
			if !distributedOperation(volumeId, func(location operation.Location) bool {
				return nil == operation.Delete("http://"+location.Url+r.URL.Path+"?type=standard")
			}) {
				ret = 0
			}
		}
	}

	if ret != 0 {
		w.WriteHeader(http.StatusAccepted)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
	}

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

func distributedOperation(volumeId storage.VolumeId, op func(location operation.Location) bool) bool {
	if lookupResult, lookupErr := operation.Lookup(*masterNode, volumeId); lookupErr == nil {
		length := 0
		selfUrl := (*ip + ":" + strconv.Itoa(*vport))
		results := make(chan bool)
		for _, location := range lookupResult.Locations {
			if location.Url != selfUrl {
				length++
				go func(location operation.Location, results chan bool) {
					results <- op(location)
				}(location, results)
			}
		}
		ret := true
		for i := 0; i < length; i++ {
			ret = ret && <-results
		}
		return ret
	} else {
		log.Println("Failed to lookup for", volumeId, lookupErr.Error())
	}
	return false
}

func runVolume(cmd *Command, args []string) bool {
  if *vMaxCpu < 1 {
    *vMaxCpu = runtime.NumCPU()
  }
	runtime.GOMAXPROCS(*vMaxCpu)
	fileInfo, err := os.Stat(*volumeFolder)
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

	log.Println("Start Weed volume server", VERSION, "at http://"+*ip+":"+strconv.Itoa(*vport))
	srv := &http.Server{
		Addr:        ":" + strconv.Itoa(*vport),
		Handler:     http.DefaultServeMux,
		ReadTimeout: (time.Duration(*vReadTimeout) * time.Second),
	}
	e := srv.ListenAndServe()
	if e != nil {
		log.Fatalf("Fail to start:%s", e.Error())
	}
	return true
}
