package weed_server

import (
	"code.google.com/p/weed-fs/go/storage"
	"code.google.com/p/weed-fs/go/util"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
)

func (ms *MasterServer) dirLookupHandler(w http.ResponseWriter, r *http.Request) {
	vid := r.FormValue("volumeId")
	collection := r.FormValue("collection") //optional, but can be faster if too many collections
	commaSep := strings.Index(vid, ",")
	if commaSep > 0 {
		vid = vid[0:commaSep]
	}
	volumeId, err := storage.NewVolumeId(vid)
	if err == nil {
		machines := ms.topo.Lookup(collection, volumeId)
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

func (ms *MasterServer) dirAssignHandler(w http.ResponseWriter, r *http.Request) {
	c, e := strconv.Atoi(r.FormValue("count"))
	if e != nil {
		c = 1
	}
	replication := r.FormValue("replication")
	if replication == "" {
		replication = ms.defaultReplicaPlacement
	}
	collection := r.FormValue("collection")
	dataCenter := r.FormValue("dataCenter")
	replicaPlacement, err := storage.NewReplicaPlacementFromString(replication)
	if err != nil {
		w.WriteHeader(http.StatusNotAcceptable)
		writeJsonQuiet(w, r, map[string]string{"error": err.Error()})
		return
	}

	if ms.topo.GetVolumeLayout(collection, replicaPlacement).GetActiveVolumeCount(dataCenter) <= 0 {
		if ms.topo.FreeSpace() <= 0 {
			w.WriteHeader(http.StatusNotFound)
			writeJsonQuiet(w, r, map[string]string{"error": "No free volumes left!"})
			return
		} else {
			ms.vgLock.Lock()
			defer ms.vgLock.Unlock()
			if ms.topo.GetVolumeLayout(collection, replicaPlacement).GetActiveVolumeCount(dataCenter) <= 0 {
				if _, err = ms.vg.AutomaticGrowByType(collection, replicaPlacement, dataCenter, ms.topo); err != nil {
					writeJsonQuiet(w, r, map[string]string{"error": "Cannot grow volume group! " + err.Error()})
					return
				}
			}
		}
	}
	fid, count, dn, err := ms.topo.PickForWrite(collection, replicaPlacement, c, dataCenter)
	if err == nil {
		writeJsonQuiet(w, r, map[string]interface{}{"fid": fid, "url": dn.Url(), "publicUrl": dn.PublicUrl, "count": count})
	} else {
		w.WriteHeader(http.StatusNotAcceptable)
		writeJsonQuiet(w, r, map[string]string{"error": err.Error()})
	}
}

func (ms *MasterServer) collectionDeleteHandler(w http.ResponseWriter, r *http.Request) {
	collection, ok := ms.topo.GetCollection(r.FormValue("collection"))
	if !ok {
		writeJsonQuiet(w, r, map[string]interface{}{"error": "collection " + r.FormValue("collection") + "does not exist!"})
		return
	}
	for _, server := range collection.ListVolumeServers() {
		_, err := util.Get("http://" + server.Ip + ":" + strconv.Itoa(server.Port) + "/admin/delete_collection?collection=" + r.FormValue("collection"))
		if err != nil {
			writeJsonQuiet(w, r, map[string]string{"error": err.Error()})
			return
		}
	}
	ms.topo.DeleteCollection(r.FormValue("collection"))
}

func (ms *MasterServer) dirJoinHandler(w http.ResponseWriter, r *http.Request) {
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
	ms.topo.RegisterVolumes(init, *volumes, ip, port, publicUrl, maxVolumeCount, r.FormValue("dataCenter"), r.FormValue("rack"))
	m := make(map[string]interface{})
	m["VolumeSizeLimit"] = uint64(ms.volumeSizeLimitMB) * 1024 * 1024
	writeJsonQuiet(w, r, m)
}

func (ms *MasterServer) dirStatusHandler(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]interface{})
	m["Version"] = ms.version
	m["Topology"] = ms.topo.ToMap()
	writeJsonQuiet(w, r, m)
}

func (ms *MasterServer) volumeVacuumHandler(w http.ResponseWriter, r *http.Request) {
	gcThreshold := r.FormValue("garbageThreshold")
	if gcThreshold == "" {
		gcThreshold = ms.garbageThreshold
	}
	debug("garbageThreshold =", gcThreshold)
	ms.topo.Vacuum(gcThreshold)
	ms.dirStatusHandler(w, r)
}

func (ms *MasterServer) volumeGrowHandler(w http.ResponseWriter, r *http.Request) {
	count := 0
	replicaPlacement, err := storage.NewReplicaPlacementFromString(r.FormValue("replication"))
	if err == nil {
		if count, err = strconv.Atoi(r.FormValue("count")); err == nil {
			if ms.topo.FreeSpace() < count*replicaPlacement.GetCopyCount() {
				err = errors.New("Only " + strconv.Itoa(ms.topo.FreeSpace()) + " volumes left! Not enough for " + strconv.Itoa(count*replicaPlacement.GetCopyCount()))
			} else {
				count, err = ms.vg.GrowByCountAndType(count, r.FormValue("collection"), replicaPlacement, r.FormValue("dataCenter"), ms.topo)
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

func (ms *MasterServer) volumeStatusHandler(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]interface{})
	m["Version"] = ms.version
	m["Volumes"] = ms.topo.ToVolumeMap()
	writeJsonQuiet(w, r, m)
}

func (ms *MasterServer) redirectHandler(w http.ResponseWriter, r *http.Request) {
	vid, _, _, _, _ := parseURLPath(r.URL.Path)
	volumeId, err := storage.NewVolumeId(vid)
	if err != nil {
		debug("parsing error:", err, r.URL.Path)
		return
	}
	machines := ms.topo.Lookup("", volumeId)
	if machines != nil && len(machines) > 0 {
		http.Redirect(w, r, "http://"+machines[0].PublicUrl+r.URL.Path, http.StatusMovedPermanently)
	} else {
		w.WriteHeader(http.StatusNotFound)
		writeJsonQuiet(w, r, map[string]string{"error": "volume id " + volumeId.String() + " not found. "})
	}
}

func (ms *MasterServer) submitFromMasterServerHandler(w http.ResponseWriter, r *http.Request) {
	if ms.IsLeader() {
		submitForClientHandler(w, r, "localhost:"+strconv.Itoa(ms.port))
	} else {
		submitForClientHandler(w, r, ms.raftServer.Leader())
	}
}
