package main

import (
	"bytes"
	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/operation"
	"code.google.com/p/weed-fs/go/replication"
	"code.google.com/p/weed-fs/go/storage"
	"code.google.com/p/weed-fs/go/topology"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

func init() {
	cmdMaster.Run = runMaster // break init cycle
	cmdMaster.IsDebug = cmdMaster.Flag.Bool("debug", false, "enable debug mode")
}

var cmdMaster = &Command{
	UsageLine: "master -port=9333",
	Short:     "start a master server",
	Long: `start a master server to provide volume=>location mapping service
  and sequence number of file ids

  `,
}

var (
	mport                 = cmdMaster.Flag.Int("port", 9333, "http listen port")
	metaFolder            = cmdMaster.Flag.String("mdir", os.TempDir(), "data directory to store mappings")
	volumeSizeLimitMB     = cmdMaster.Flag.Uint("volumeSizeLimitMB", 32*1024, "Default Volume Size in MegaBytes")
	mpulse                = cmdMaster.Flag.Int("pulseSeconds", 5, "number of seconds between heartbeats")
	confFile              = cmdMaster.Flag.String("conf", "/etc/weedfs/weedfs.conf", "xml configuration file")
	defaultRepType        = cmdMaster.Flag.String("defaultReplicationType", "000", "Default replication type if not specified.")
	mReadTimeout          = cmdMaster.Flag.Int("readTimeout", 3, "connection read timeout in seconds")
	mMaxCpu               = cmdMaster.Flag.Int("maxCpu", 0, "maximum number of CPUs. 0 means all available CPUs")
	garbageThreshold      = cmdMaster.Flag.String("garbageThreshold", "0.3", "threshold to vacuum and reclaim spaces")
	masterWhiteListOption = cmdMaster.Flag.String("whiteList", "", "comma separated Ip addresses having write permission. No limit if empty.")

	masterWhiteList []string
)

var topo *topology.Topology
var vg *replication.VolumeGrowth

func dirLookupHandler(w http.ResponseWriter, r *http.Request) {
	vid := r.FormValue("volumeId")
	commaSep := strings.Index(vid, ",")
	if commaSep > 0 {
		vid = vid[0:commaSep]
	}
	volumeId, err := storage.NewVolumeId(vid)
	if err == nil {
		machines := topo.Lookup(volumeId)
		if machines != nil {
			ret := []map[string]string{}
			for _, dn := range machines {
				ret = append(ret, map[string]string{"url": dn.Url(), "publicUrl": dn.PublicUrl})
			}
			writeJsonQuiet(w, r, map[string]interface{}{"locations": ret})
		} else {
			w.WriteHeader(http.StatusNotFound)
			writeJsonQuiet(w, r, map[string]string{"error": "volume id " + volumeId.String() + " not found. "})
		}
	} else {
		w.WriteHeader(http.StatusNotAcceptable)
		writeJsonQuiet(w, r, map[string]string{"error": "unknown volumeId format " + vid})
	}
}

func dirAssignHandler(w http.ResponseWriter, r *http.Request) {
	c, e := strconv.Atoi(r.FormValue("count"))
	if e != nil {
		c = 1
	}
	repType := r.FormValue("replication")
	if repType == "" {
		repType = *defaultRepType
	}
	dataCenter := r.FormValue("dataCenter")
	rt, err := storage.NewReplicationTypeFromString(repType)
	if err != nil {
		w.WriteHeader(http.StatusNotAcceptable)
		writeJsonQuiet(w, r, map[string]string{"error": err.Error()})
		return
	}

	if topo.GetVolumeLayout(rt).GetActiveVolumeCount(dataCenter) <= 0 {
		if topo.FreeSpace() <= 0 {
			w.WriteHeader(http.StatusNotFound)
			writeJsonQuiet(w, r, map[string]string{"error": "No free volumes left!"})
			return
		} else {
			if _, err = vg.AutomaticGrowByType(rt, dataCenter, topo); err != nil {
				writeJsonQuiet(w, r, map[string]string{"error": "Cannot grow volume group! " + err.Error()})
				return
			}
		}
	}
	fid, count, dn, err := topo.PickForWrite(rt, c, dataCenter)
	if err == nil {
		writeJsonQuiet(w, r, map[string]interface{}{"fid": fid, "url": dn.Url(), "publicUrl": dn.PublicUrl, "count": count})
	} else {
		w.WriteHeader(http.StatusNotAcceptable)
		writeJsonQuiet(w, r, map[string]string{"error": err.Error()})
	}
}

func dirJoinHandler(w http.ResponseWriter, r *http.Request) {
	init := r.FormValue("init") == "true"
	ip := r.FormValue("ip")
	if ip == "" {
		ip = r.RemoteAddr[0:strings.Index(r.RemoteAddr, ":")]
	}
	port, _ := strconv.Atoi(r.FormValue("port"))
	maxVolumeCount, _ := strconv.Atoi(r.FormValue("maxVolumeCount"))
	s := r.RemoteAddr[0:strings.Index(r.RemoteAddr, ":")+1] + r.FormValue("port")
	publicUrl := r.FormValue("publicUrl")
	volumes := new([]storage.VolumeInfo)
	if err := json.Unmarshal([]byte(r.FormValue("volumes")), volumes); err != nil {
		writeJsonQuiet(w, r, map[string]string{"error": "Cannot unmarshal \"volumes\": " + err.Error()})
		return
	}
	debug(s, "volumes", r.FormValue("volumes"))
	topo.RegisterVolumes(init, *volumes, ip, port, publicUrl, maxVolumeCount, r.FormValue("dataCenter"), r.FormValue("rack"))
	m := make(map[string]interface{})
	m["VolumeSizeLimit"] = uint64(*volumeSizeLimitMB) * 1024 * 1024
	writeJsonQuiet(w, r, m)
}

func dirStatusHandler(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]interface{})
	m["Version"] = VERSION
	m["Topology"] = topo.ToMap()
	writeJsonQuiet(w, r, m)
}

func volumeVacuumHandler(w http.ResponseWriter, r *http.Request) {
	gcThreshold := r.FormValue("garbageThreshold")
	if gcThreshold == "" {
		gcThreshold = *garbageThreshold
	}
	debug("garbageThreshold =", gcThreshold)
	topo.Vacuum(gcThreshold)
	dirStatusHandler(w, r)
}

func volumeGrowHandler(w http.ResponseWriter, r *http.Request) {
	count := 0
	rt, err := storage.NewReplicationTypeFromString(r.FormValue("replication"))
	if err == nil {
		if count, err = strconv.Atoi(r.FormValue("count")); err == nil {
			if topo.FreeSpace() < count*rt.GetCopyCount() {
				err = errors.New("Only " + strconv.Itoa(topo.FreeSpace()) + " volumes left! Not enough for " + strconv.Itoa(count*rt.GetCopyCount()))
			} else {
				count, err = vg.GrowByCountAndType(count, rt, r.FormValue("dataCneter"), topo)
			}
		} else {
			err = errors.New("parameter count is not found")
		}
	}
	if err != nil {
		w.WriteHeader(http.StatusNotAcceptable)
		writeJsonQuiet(w, r, map[string]string{"error": "parameter replication " + err.Error()})
	} else {
		w.WriteHeader(http.StatusNotAcceptable)
		writeJsonQuiet(w, r, map[string]interface{}{"count": count})
	}
}

func volumeStatusHandler(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]interface{})
	m["Version"] = VERSION
	m["Volumes"] = topo.ToVolumeMap()
	writeJsonQuiet(w, r, m)
}

func redirectHandler(w http.ResponseWriter, r *http.Request) {
	vid, _, _, _, _ := parseURLPath(r.URL.Path)
	volumeId, err := storage.NewVolumeId(vid)
	if err != nil {
		debug("parsing error:", err, r.URL.Path)
		return
	}
	machines := topo.Lookup(volumeId)
	if machines != nil && len(machines) > 0 {
		http.Redirect(w, r, "http://"+machines[0].PublicUrl+r.URL.Path, http.StatusMovedPermanently)
	} else {
		w.WriteHeader(http.StatusNotFound)
		writeJsonQuiet(w, r, map[string]string{"error": "volume id " + volumeId.String() + " not found. "})
	}
}

func submitFromMasterServerHandler(w http.ResponseWriter, r *http.Request) {
	submitForClientHandler(w, r, "localhost:"+strconv.Itoa(*mport))
}

func runMaster(cmd *Command, args []string) bool {
	if *mMaxCpu < 1 {
		*mMaxCpu = runtime.NumCPU()
	}
	runtime.GOMAXPROCS(*mMaxCpu)
	if *masterWhiteListOption != "" {
		masterWhiteList = strings.Split(*masterWhiteListOption, ",")
	}
	var e error
	if topo, e = topology.NewTopology("topo", *confFile, *metaFolder, "weed",
		uint64(*volumeSizeLimitMB)*1024*1024, *mpulse); e != nil {
		glog.Fatalf("cannot create topology:%s", e)
	}
	vg = replication.NewDefaultVolumeGrowth()
	glog.V(0).Infoln("Volume Size Limit is", *volumeSizeLimitMB, "MB")
	http.HandleFunc("/dir/assign", secure(masterWhiteList, dirAssignHandler))
	http.HandleFunc("/dir/lookup", secure(masterWhiteList, dirLookupHandler))
	http.HandleFunc("/dir/join", secure(masterWhiteList, dirJoinHandler))
	http.HandleFunc("/dir/status", secure(masterWhiteList, dirStatusHandler))
	http.HandleFunc("/vol/grow", secure(masterWhiteList, volumeGrowHandler))
	http.HandleFunc("/vol/status", secure(masterWhiteList, volumeStatusHandler))
	http.HandleFunc("/vol/vacuum", secure(masterWhiteList, volumeVacuumHandler))

	http.HandleFunc("/submit", secure(masterWhiteList, submitFromMasterServerHandler))
	http.HandleFunc("/", redirectHandler)

	topo.StartRefreshWritableVolumes(*garbageThreshold)

	glog.V(0).Infoln("Start Weed Master", VERSION, "at port", strconv.Itoa(*mport))
	srv := &http.Server{
		Addr:        ":" + strconv.Itoa(*mport),
		Handler:     http.DefaultServeMux,
		ReadTimeout: time.Duration(*mReadTimeout) * time.Second,
	}
	e = srv.ListenAndServe()
	if e != nil {
		glog.Fatalf("Fail to start:%s", e)
	}
	return true
}

func submitForClientHandler(w http.ResponseWriter, r *http.Request, masterUrl string) {
	m := make(map[string]interface{})
	if r.Method != "POST" {
		m["error"] = "Only submit via POST!"
		writeJsonQuiet(w, r, m)
		return
	}

	debug("parsing upload file...")
	fname, data, mimeType, isGzipped, lastModified, pe := storage.ParseUpload(r)
	if pe != nil {
		writeJsonError(w, r, pe)
		return
	}

	debug("assigning file id for", fname)
	assignResult, ae := operation.Assign(masterUrl, 1, r.FormValue("replication"))
	if ae != nil {
		writeJsonError(w, r, ae)
		return
	}

	url := "http://" + assignResult.PublicUrl + "/" + assignResult.Fid
	if lastModified != 0 {
		url = url + "?ts=" + strconv.FormatUint(lastModified, 10)
	}

	debug("upload file to store", url)
	uploadResult, err := operation.Upload(url, fname, bytes.NewReader(data), isGzipped, mimeType)
	if err != nil {
		writeJsonError(w, r, err)
		return
	}

	m["fileName"] = fname
	m["fid"] = assignResult.Fid
	m["fileUrl"] = assignResult.PublicUrl + "/" + assignResult.Fid
	m["size"] = uploadResult.Size
	writeJsonQuiet(w, r, m)
	return
}
