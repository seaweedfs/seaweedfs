package main

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"pkg/replication"
	"pkg/storage"
	"pkg/topology"
	"strconv"
	"strings"
	"time"
)

func init() {
	cmdMaster.Run = runMaster // break init cycle
	IsDebug = cmdMaster.Flag.Bool("debug", false, "enable debug mode")
}

var cmdMaster = &Command{
	UsageLine: "master -port=9333",
	Short:     "start a master server",
	Long: `start a master server to provide volume=>location mapping service
  and sequence number of file ids

  `,
}

var (
	mport             = cmdMaster.Flag.Int("port", 9333, "http listen port")
	metaFolder        = cmdMaster.Flag.String("mdir", "/tmp", "data directory to store mappings")
	volumeSizeLimitMB = cmdMaster.Flag.Uint("volumeSizeLimitMB", 32*1024, "Default Volume Size in MegaBytes")
	mpulse            = cmdMaster.Flag.Int("pulseSeconds", 5, "number of seconds between heartbeats")
	confFile          = cmdMaster.Flag.String("conf", "/etc/weedfs/weedfs.conf", "xml configuration file")
	defaultRepType    = cmdMaster.Flag.String("defaultReplicationType", "00", "Default replication type if not specified.")
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
			for _, dn := range *machines {
				ret = append(ret, map[string]string{"url": dn.Url(), "publicUrl":dn.PublicUrl})
			}
			writeJson(w, r, map[string]interface{}{"locations": ret})
		} else {
			writeJson(w, r, map[string]string{"error": "volume id " + volumeId.String() + " not found. "})
		}
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
	rt, err := storage.NewReplicationType(repType)
	if err != nil {
		writeJson(w, r, map[string]string{"error": err.Error()})
		return
	}
	if topo.GetVolumeLayout(rt).GetActiveVolumeCount() <= 0 {
		if topo.FreeSpace() <= 0 {
			writeJson(w, r, map[string]string{"error": "No free volumes left!"})
			return
		} else {
			vg.GrowByType(rt, topo)
		}
	}
	fid, count, dn, err := topo.PickForWrite(rt, c)
	if err == nil {
		writeJson(w, r, map[string]interface{}{"fid": fid, "url": dn.Url(), "publicUrl":dn.PublicUrl, "count": count})
	} else {
		writeJson(w, r, map[string]string{"error": err.Error()})
	}
}

func dirJoinHandler(w http.ResponseWriter, r *http.Request) {
  ip := r.FormValue("ip")
  if ip == ""{
    ip = r.RemoteAddr[0:strings.Index(r.RemoteAddr, ":")]
  }
	port, _ := strconv.Atoi(r.FormValue("port"))
	maxVolumeCount, _ := strconv.Atoi(r.FormValue("maxVolumeCount"))
	s := r.RemoteAddr[0:strings.Index(r.RemoteAddr, ":")+1] + r.FormValue("port")
	publicUrl := r.FormValue("publicUrl")
	volumes := new([]storage.VolumeInfo)
	json.Unmarshal([]byte(r.FormValue("volumes")), volumes)
	debug(s, "volumes", r.FormValue("volumes"))
	topo.RegisterVolumes(*volumes, ip, port, publicUrl, maxVolumeCount)
}

func dirStatusHandler(w http.ResponseWriter, r *http.Request) {
	writeJson(w, r, topo.ToMap())
}

func volumeGrowHandler(w http.ResponseWriter, r *http.Request) {
	count := 0
	rt, err := storage.NewReplicationType(r.FormValue("replication"))
	if err == nil {
		if count, err = strconv.Atoi(r.FormValue("count")); err == nil {
			if topo.FreeSpace() < count*rt.GetCopyCount() {
				err = errors.New("Only " + strconv.Itoa(topo.FreeSpace()) + " volumes left! Not enough for " + strconv.Itoa(count*rt.GetCopyCount()))
			} else {
				count, err = vg.GrowByCountAndType(count, rt, topo)
			}
		}
	}
	if err != nil {
		writeJson(w, r, map[string]string{"error": err.Error()})
	} else {
		writeJson(w, r, map[string]interface{}{"count": count})
	}
}

func runMaster(cmd *Command, args []string) bool {
	topo = topology.NewTopology("topo", *confFile, *metaFolder, "weed", uint64(*volumeSizeLimitMB)*1024*1024, *mpulse)
	vg = replication.NewDefaultVolumeGrowth()
	log.Println("Volume Size Limit is", *volumeSizeLimitMB, "MB")
	http.HandleFunc("/dir/assign", dirAssignHandler)
	http.HandleFunc("/dir/lookup", dirLookupHandler)
	http.HandleFunc("/dir/join", dirJoinHandler)
	http.HandleFunc("/dir/status", dirStatusHandler)
	http.HandleFunc("/vol/grow", volumeGrowHandler)

	topo.StartRefreshWritableVolumes()

	log.Println("Start Weed Master", VERSION, "at port", strconv.Itoa(*mport))
  srv := &http.Server{
    Addr:":"+strconv.Itoa(*mport),
    Handler: http.DefaultServeMux,
    ReadTimeout: 30*time.Second,
  }
  e := srv.ListenAndServe()
	if e != nil {
		log.Fatalf("Fail to start:%s", e.Error())
	}
	return true
}
