package weed_server

import (
	"code.google.com/p/weed-fs/go/stats"
	"code.google.com/p/weed-fs/go/storage"
	"code.google.com/p/weed-fs/go/topology"
	"code.google.com/p/weed-fs/go/util"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
)

type LookupResultLocation struct {
	Url       string `json:"url,omitempty"`
	PublicUrl string `json:"publicUrl,omitempty"`
}
type LookupResult struct {
	VolumeId  string                 `json:"volumeId,omitempty"`
	Locations []LookupResultLocation `json:"locations,omitempty"`
	Error     string                 `json:"error,omitempty"`
}

func (ms *MasterServer) lookupVolumeId(vids []string, collection string) (volumeLocations map[string]LookupResult) {
	volumeLocations = make(map[string]LookupResult)
	for _, vid := range vids {
		commaSep := strings.Index(vid, ",")
		if commaSep > 0 {
			vid = vid[0:commaSep]
		}
		if _, ok := volumeLocations[vid]; ok {
			continue
		}
		volumeId, err := storage.NewVolumeId(vid)
		if err == nil {
			machines := ms.Topo.Lookup(collection, volumeId)
			if machines != nil {
				var ret []LookupResultLocation
				for _, dn := range machines {
					ret = append(ret, LookupResultLocation{Url: dn.Url(), PublicUrl: dn.PublicUrl})
				}
				volumeLocations[vid] = LookupResult{VolumeId: vid, Locations: ret}
			} else {
				volumeLocations[vid] = LookupResult{VolumeId: vid, Error: "volumeId not found."}
			}
		} else {
			volumeLocations[vid] = LookupResult{VolumeId: vid, Error: "Unknown volumeId format."}
		}
	}
	return
}

// Takes one volumeId only, can not do batch lookup
func (ms *MasterServer) dirLookupHandler(w http.ResponseWriter, r *http.Request) {
	vid := r.FormValue("volumeId")
	commaSep := strings.Index(vid, ",")
	if commaSep > 0 {
		vid = vid[0:commaSep]
	}
	vids := []string{vid}
	collection := r.FormValue("collection") //optional, but can be faster if too many collections
	volumeLocations := ms.lookupVolumeId(vids, collection)
	location := volumeLocations[vid]
	if location.Error != "" {
		w.WriteHeader(http.StatusNotFound)
	}
	writeJsonQuiet(w, r, location)
}

// This can take batched volumeIds, &volumeId=x&volumeId=y&volumeId=z
func (ms *MasterServer) volumeLookupHandler(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	vids := r.Form["volumeId"]
	collection := r.FormValue("collection") //optional, but can be faster if too many collections
	volumeLocations := ms.lookupVolumeId(vids, collection)
	var ret []LookupResult
	for _, volumeLocation := range volumeLocations {
		ret = append(ret, volumeLocation)
	}
	writeJsonQuiet(w, r, ret)
}

func (ms *MasterServer) dirAssignHandler(w http.ResponseWriter, r *http.Request) {
	stats.AssignRequest()
	requestedCount, e := strconv.Atoi(r.FormValue("count"))
	if e != nil {
		requestedCount = 1
	}

	option, err := ms.getVolumeGrowOption(r)
	if err != nil {
		w.WriteHeader(http.StatusNotAcceptable)
		writeJsonQuiet(w, r, map[string]string{"error": err.Error()})
		return
	}

	if !ms.Topo.HasWriableVolume(option) {
		if ms.Topo.FreeSpace() <= 0 {
			w.WriteHeader(http.StatusNotFound)
			writeJsonQuiet(w, r, map[string]string{"error": "No free volumes left!"})
			return
		} else {
			ms.vgLock.Lock()
			defer ms.vgLock.Unlock()
			if !ms.Topo.HasWriableVolume(option) {
				if _, err = ms.vg.AutomaticGrowByType(option, ms.Topo); err != nil {
					writeJsonQuiet(w, r, map[string]string{"error": "Cannot grow volume group! " + err.Error()})
					return
				}
			}
		}
	}
	fid, count, dn, err := ms.Topo.PickForWrite(requestedCount, option)
	if err == nil {
		writeJsonQuiet(w, r, map[string]interface{}{"fid": fid, "url": dn.Url(), "publicUrl": dn.PublicUrl, "count": count})
	} else {
		w.WriteHeader(http.StatusNotAcceptable)
		writeJsonQuiet(w, r, map[string]string{"error": err.Error()})
	}
}

func (ms *MasterServer) collectionDeleteHandler(w http.ResponseWriter, r *http.Request) {
	collection, ok := ms.Topo.GetCollection(r.FormValue("collection"))
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
	ms.Topo.DeleteCollection(r.FormValue("collection"))
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
	ms.Topo.RegisterVolumes(init, *volumes, ip, port, publicUrl, maxVolumeCount, r.FormValue("dataCenter"), r.FormValue("rack"))
	m := make(map[string]interface{})
	m["VolumeSizeLimit"] = uint64(ms.volumeSizeLimitMB) * 1024 * 1024
	writeJsonQuiet(w, r, m)
}

func (ms *MasterServer) dirStatusHandler(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]interface{})
	m["Version"] = util.VERSION
	m["Topology"] = ms.Topo.ToMap()
	writeJsonQuiet(w, r, m)
}

func (ms *MasterServer) volumeVacuumHandler(w http.ResponseWriter, r *http.Request) {
	gcThreshold := r.FormValue("garbageThreshold")
	if gcThreshold == "" {
		gcThreshold = ms.garbageThreshold
	}
	debug("garbageThreshold =", gcThreshold)
	ms.Topo.Vacuum(gcThreshold)
	ms.dirStatusHandler(w, r)
}

func (ms *MasterServer) volumeGrowHandler(w http.ResponseWriter, r *http.Request) {
	count := 0
	option, err := ms.getVolumeGrowOption(r)
	if err != nil {
		w.WriteHeader(http.StatusNotAcceptable)
		writeJsonQuiet(w, r, map[string]string{"error": err.Error()})
		return
	}
	if err == nil {
		if count, err = strconv.Atoi(r.FormValue("count")); err == nil {
			if ms.Topo.FreeSpace() < count*option.ReplicaPlacement.GetCopyCount() {
				err = errors.New("Only " + strconv.Itoa(ms.Topo.FreeSpace()) + " volumes left! Not enough for " + strconv.Itoa(count*option.ReplicaPlacement.GetCopyCount()))
			} else {
				count, err = ms.vg.GrowByCountAndType(count, option, ms.Topo)
			}
		} else {
			err = errors.New("parameter count is not found")
		}
	}
	if err != nil {
		w.WriteHeader(http.StatusNotAcceptable)
		writeJsonQuiet(w, r, map[string]string{"error": err.Error()})
	} else {
		w.WriteHeader(http.StatusNotAcceptable)
		writeJsonQuiet(w, r, map[string]interface{}{"count": count})
	}
}

func (ms *MasterServer) volumeStatusHandler(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]interface{})
	m["Version"] = util.VERSION
	m["Volumes"] = ms.Topo.ToVolumeMap()
	writeJsonQuiet(w, r, m)
}

func (ms *MasterServer) redirectHandler(w http.ResponseWriter, r *http.Request) {
	vid, _, _, _, _ := parseURLPath(r.URL.Path)
	volumeId, err := storage.NewVolumeId(vid)
	if err != nil {
		debug("parsing error:", err, r.URL.Path)
		return
	}
	machines := ms.Topo.Lookup("", volumeId)
	if machines != nil && len(machines) > 0 {
		http.Redirect(w, r, "http://"+machines[0].PublicUrl+r.URL.Path, http.StatusMovedPermanently)
	} else {
		w.WriteHeader(http.StatusNotFound)
		writeJsonQuiet(w, r, map[string]string{"error": "volume id " + volumeId.String() + " not found. "})
	}
}

func (ms *MasterServer) submitFromMasterServerHandler(w http.ResponseWriter, r *http.Request) {
	if ms.Topo.IsLeader() {
		submitForClientHandler(w, r, "localhost:"+strconv.Itoa(ms.port))
	} else {
		submitForClientHandler(w, r, ms.Topo.RaftServer.Leader())
	}
}

func (ms *MasterServer) deleteFromMasterServerHandler(w http.ResponseWriter, r *http.Request) {
	if ms.Topo.IsLeader() {
		deleteForClientHandler(w, r, "localhost:"+strconv.Itoa(ms.port))
	} else {
		deleteForClientHandler(w, r, ms.Topo.RaftServer.Leader())
	}
}

func (ms *MasterServer) hasWriableVolume(option *topology.VolumeGrowOption) bool {
	vl := ms.Topo.GetVolumeLayout(option.Collection, option.ReplicaPlacement)
	return vl.GetActiveVolumeCount(option) > 0
}

func (ms *MasterServer) getVolumeGrowOption(r *http.Request) (*topology.VolumeGrowOption, error) {
	replicationString := r.FormValue("replication")
	if replicationString == "" {
		replicationString = ms.defaultReplicaPlacement
	}
	replicaPlacement, err := storage.NewReplicaPlacementFromString(replicationString)
	if err != nil {
		return nil, err
	}
	volumeGrowOption := &topology.VolumeGrowOption{
		Collection:       r.FormValue("collection"),
		ReplicaPlacement: replicaPlacement,
		DataCenter:       r.FormValue("dataCenter"),
		Rack:             r.FormValue("rack"),
		DataNode:         r.FormValue("dataNode"),
	}
	return volumeGrowOption, nil
}
