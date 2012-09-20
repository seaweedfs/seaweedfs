package main

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"pkg/directory"
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
	capacity          = cmdMaster.Flag.Int("capacity", 100, "maximum number of volumes to hold")
	volumeSizeLimitMB = cmdMaster.Flag.Uint("volumeSizeLimitMB", 32*1024, "Default Volume Size in MegaBytes")
	mpulse            = cmdMaster.Flag.Int("pulseSeconds", 5, "number of seconds between heartbeats")
)

var mapper *directory.Mapper
var topo *topology.Topology
var vg *replication.VolumeGrowth

func dirLookupHandler(w http.ResponseWriter, r *http.Request) {
	vid := r.FormValue("volumeId")
	commaSep := strings.Index(vid, ",")
	if commaSep > 0 {
		vid = vid[0:commaSep]
	}
	volumeId, _ := storage.NewVolumeId(vid)
	machines, e := mapper.Get(volumeId)
	if e == nil {
		ret := []map[string]string{}
		for _, machine := range machines {
			ret = append(ret, map[string]string{"url": machine.Url, "publicUrl": machine.PublicUrl})
		}
		writeJson(w, r, map[string]interface{}{"locations":ret})
	} else {
		log.Println("Invalid volume id", volumeId)
		writeJson(w, r, map[string]string{"error": "volume id " + volumeId.String() + " not found. " + e.Error()})
	}
}
func dirAssignHandler(w http.ResponseWriter, r *http.Request) {
	c := r.FormValue("count")
	fid, count, machine, err := mapper.PickForWrite(c)
	if err == nil {
		writeJson(w, r, map[string]interface{}{"fid": fid, "url": machine.Url, "publicUrl": machine.PublicUrl, "count": count})
	} else {
		writeJson(w, r, map[string]string{"error": err.Error()})
	}
}
func dirAssign2Handler(w http.ResponseWriter, r *http.Request) {
	c, _ := strconv.Atoi(r.FormValue("count"))
	rt, err := storage.NewReplicationType(r.FormValue("replication"))
	if err != nil {
		writeJson(w, r, map[string]string{"error": err.Error()})
		return
	}
	if topo.GetVolumeLayout(rt).GetActiveVolumeCount() <= 0 {
		if topo.FreeSpace() <= 0 {
			writeJson(w, r, map[string]string{"error": "No free volumes left!"})
		} else {
			vg.GrowByType(rt, topo)
		}
	}
	fid, count, dn, err := topo.PickForWrite(rt, c)
	if err == nil {
		writeJson(w, r, map[string]interface{}{"fid": fid, "url": dn.Ip + ":" + strconv.Itoa(dn.Port), "publicUrl": dn.PublicUrl, "count": count})
	} else {
		writeJson(w, r, map[string]string{"error": err.Error()})
	}
}
func dirJoinHandler(w http.ResponseWriter, r *http.Request) {
	ip := r.RemoteAddr[0:strings.Index(r.RemoteAddr, ":")]
	port, _ := strconv.Atoi(r.FormValue("port"))
	maxVolumeCount, _ := strconv.Atoi(r.FormValue("maxVolumeCount"))
	s := r.RemoteAddr[0:strings.Index(r.RemoteAddr, ":")+1] + r.FormValue("port")
	publicUrl := r.FormValue("publicUrl")
	volumes := new([]storage.VolumeInfo)
	json.Unmarshal([]byte(r.FormValue("volumes")), volumes)
	if *IsDebug {
		log.Println(s, "volumes", r.FormValue("volumes"))
	}
	mapper.Add(directory.NewMachine(s, publicUrl, *volumes, time.Now().Unix()))

	//new ways
	topo.RegisterVolumes(*volumes, ip, port, publicUrl, maxVolumeCount)
}
func dirOldStatusHandler(w http.ResponseWriter, r *http.Request) {
	writeJson(w, r, mapper)
}
func dirNewStatusHandler(w http.ResponseWriter, r *http.Request) {
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
	topo = topology.NewTopology("topo", *metaFolder, "toposequence", uint64(*volumeSizeLimitMB)*1024*1024, *mpulse)
	vg = replication.NewDefaultVolumeGrowth()
	log.Println("Volume Size Limit is", *volumeSizeLimitMB, "MB")
	mapper = directory.NewMapper(*metaFolder, "directory", uint64(*volumeSizeLimitMB)*1024*1024, *mpulse)
	http.HandleFunc("/dir/assign", dirAssignHandler)
	http.HandleFunc("/dir/assign2", dirAssign2Handler)
	http.HandleFunc("/dir/lookup", dirLookupHandler)
	http.HandleFunc("/dir/join", dirJoinHandler)
	http.HandleFunc("/dir/status", dirOldStatusHandler)
	http.HandleFunc("/dir/status2", dirNewStatusHandler) //temporary
	http.HandleFunc("/vol/grow", volumeGrowHandler)

	mapper.StartRefreshWritableVolumes()
	topo.StartRefreshWritableVolumes()

	log.Println("Start directory service at http://127.0.0.1:" + strconv.Itoa(*mport))
	e := http.ListenAndServe(":"+strconv.Itoa(*mport), nil)
	if e != nil {
		log.Fatal("Fail to start:", e)
	}
	return true
}
