package weed_server

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	proto "code.google.com/p/goprotobuf/proto"
	"github.com/mcqueenorama/weed-fs/go/glog"
	"github.com/mcqueenorama/weed-fs/go/operation"
	"github.com/mcqueenorama/weed-fs/go/storage"
	"github.com/mcqueenorama/weed-fs/go/topology"
	"github.com/mcqueenorama/weed-fs/go/util"
)

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
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		writeJsonError(w, r, err)
		return
	}
	joinMessage := &operation.JoinMessage{}
	if err = proto.Unmarshal(body, joinMessage); err != nil {
		writeJsonError(w, r, err)
		return
	}
	if *joinMessage.Ip == "" {
		*joinMessage.Ip = r.RemoteAddr[0:strings.Index(r.RemoteAddr, ":")]
	}
	if glog.V(4) {
		if jsonData, jsonError := json.Marshal(joinMessage); jsonError != nil {
			glog.V(0).Infoln("json marshaling error: ", jsonError)
			writeJsonError(w, r, jsonError)
			return
		} else {
			glog.V(4).Infoln("Proto size", len(body), "json size", len(jsonData), string(jsonData))
		}
	}

	ms.Topo.ProcessJoinMessage(joinMessage)
	writeJsonQuiet(w, r, operation.JoinResult{VolumeSizeLimit: uint64(ms.volumeSizeLimitMB) * 1024 * 1024})
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
		w.WriteHeader(http.StatusOK)
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

func (ms *MasterServer) HasWritableVolume(option *topology.VolumeGrowOption) bool {
	vl := ms.Topo.GetVolumeLayout(option.Collection, option.ReplicaPlacement, option.Ttl)
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
	ttl, err := storage.ReadTTL(r.FormValue("ttl"))
	if err != nil {
		return nil, err
	}
	volumeGrowOption := &topology.VolumeGrowOption{
		Collection:       r.FormValue("collection"),
		ReplicaPlacement: replicaPlacement,
		Ttl:              ttl,
		DataCenter:       r.FormValue("dataCenter"),
		Rack:             r.FormValue("rack"),
		DataNode:         r.FormValue("dataNode"),
	}
	return volumeGrowOption, nil
}
