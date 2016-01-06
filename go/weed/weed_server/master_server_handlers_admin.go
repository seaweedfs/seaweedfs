package weed_server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	"net/url"
	"sync"

	"github.com/chrislusf/seaweedfs/go/glog"
	"github.com/chrislusf/seaweedfs/go/operation"
	"github.com/chrislusf/seaweedfs/go/storage"
	"github.com/chrislusf/seaweedfs/go/topology"
	"github.com/chrislusf/seaweedfs/go/util"
	"github.com/golang/protobuf/proto"
)

func (ms *MasterServer) collectionDeleteHandler(w http.ResponseWriter, r *http.Request) {
	collection, ok := ms.Topo.GetCollection(r.FormValue("collection"))
	if !ok {
		writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("collection %s does not exist", r.FormValue("collection")))
		return
	}
	for _, server := range collection.ListVolumeServers() {
		_, err := util.Get(server.Ip+":"+strconv.Itoa(server.Port), "/admin/delete_collection", url.Values{"collection": r.Form["collection"]})
		if err != nil {
			writeJsonError(w, r, http.StatusInternalServerError, err)
			return
		}
	}
	ms.Topo.DeleteCollection(r.FormValue("collection"))
}

func (ms *MasterServer) dirJoinHandler(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}
	joinMessage := &operation.JoinMessage{}
	if err = proto.Unmarshal(body, joinMessage); err != nil {
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}
	if *joinMessage.Ip == "" {
		*joinMessage.Ip = r.RemoteAddr[0:strings.Index(r.RemoteAddr, ":")]
	}
	if glog.V(4) {
		if jsonData, jsonError := json.Marshal(joinMessage); jsonError != nil {
			glog.V(0).Infoln("json marshaling error: ", jsonError)
			writeJsonError(w, r, http.StatusBadRequest, jsonError)
			return
		} else {
			glog.V(4).Infoln("Proto size", len(body), "json size", len(jsonData), string(jsonData))
		}
	}

	ms.Topo.ProcessJoinMessage(joinMessage)
	writeJsonQuiet(w, r, http.StatusOK, operation.JoinResult{
		VolumeSizeLimit: uint64(ms.volumeSizeLimitMB) * 1024 * 1024,
		SecretKey:       string(ms.guard.SecretKey),
	})
}

func (ms *MasterServer) dirStatusHandler(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]interface{})
	m["Version"] = util.VERSION
	m["Topology"] = ms.Topo.ToMap()
	writeJsonQuiet(w, r, http.StatusOK, m)
}

func (ms *MasterServer) volumeVacuumHandler(w http.ResponseWriter, r *http.Request) {
	gcThreshold := r.FormValue("garbageThreshold")
	if gcThreshold == "" {
		gcThreshold = ms.garbageThreshold
	}
	glog.Infoln("garbageThreshold =", gcThreshold)
	ms.Topo.Vacuum(gcThreshold)
	ms.dirStatusHandler(w, r)
}

func (ms *MasterServer) volumeGrowHandler(w http.ResponseWriter, r *http.Request) {
	count := 0
	option, err := ms.getVolumeGrowOption(r)
	if err != nil {
		writeJsonError(w, r, http.StatusNotAcceptable, err)
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
		writeJsonError(w, r, http.StatusNotAcceptable, err)
	} else {
		writeJsonQuiet(w, r, http.StatusOK, map[string]interface{}{"count": count})
	}
}

func (ms *MasterServer) volumeStatusHandler(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]interface{})
	m["Version"] = util.VERSION
	m["Volumes"] = ms.Topo.ToVolumeMap()
	writeJsonQuiet(w, r, http.StatusOK, m)
}

func (ms *MasterServer) redirectHandler(w http.ResponseWriter, r *http.Request) {
	vid, _, _, _, _ := parseURLPath(r.URL.Path)
	volumeId, err := storage.NewVolumeId(vid)
	if err != nil {
		debug("parsing error:", err, r.URL.Path)
		return
	}
	locations := ms.Topo.Lookup("", volumeId)
	if locations != nil && locations.Length() > 0 {
		http.Redirect(w, r, util.NormalizeUrl(locations.PickForRead().PublicUrl)+r.URL.Path, http.StatusMovedPermanently)
	} else {
		writeJsonError(w, r, http.StatusNotFound, fmt.Errorf("volume id %d not found", volumeId))
	}
}

func (ms *MasterServer) selfUrl(r *http.Request) string {
	if r.Host != "" {
		return r.Host
	}
	return "localhost:" + strconv.Itoa(ms.port)
}
func (ms *MasterServer) submitFromMasterServerHandler(w http.ResponseWriter, r *http.Request) {
	if ms.Topo.IsLeader() {
		submitForClientHandler(w, r, ms.selfUrl(r))
	} else {
		masterUrl, err := ms.Topo.Leader()
		if err != nil {
			writeJsonError(w, r, http.StatusInternalServerError, err)
		} else {
			submitForClientHandler(w, r, masterUrl)
		}
	}
}

func (ms *MasterServer) deleteFromMasterServerHandler(w http.ResponseWriter, r *http.Request) {
	if ms.Topo.IsLeader() {
		deleteForClientHandler(w, r, ms.selfUrl(r))
	} else {
		deleteForClientHandler(w, r, ms.Topo.RaftServer.Leader())
	}
}

func (ms *MasterServer) HasWritableVolume(option *topology.VolumeGrowOption) bool {
	vl := ms.Topo.GetVolumeLayout(option.Collection, option.Ttl)
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

func (ms *MasterServer) batchSetVolumeOption(settingKey, settingValue string, volumes, collections []string) (result map[string]interface{}) {
	forms := url.Values{}
	forms.Set("key", settingKey)
	forms.Set("value", settingValue)
	if len(volumes) == 0 && len(collections) == 0 {
		forms.Set("all", "true")
	} else {
		forms["volume"] = volumes
		forms["collection"] = collections
	}

	var wg sync.WaitGroup
	ms.Topo.WalkDataNode(func(dn *topology.DataNode) (e error) {
		wg.Add(1)
		go func(server string, values url.Values) {
			defer wg.Done()
			jsonBlob, e := util.Post(server, "/admin/setting", values)
			if e != nil {
				result[server] = map[string]interface{}{
					"error": e.Error() + " " + string(jsonBlob),
				}
			}
			var ret interface{}
			if e := json.Unmarshal(jsonBlob, ret); e == nil {
				result[server] = ret
			} else {
				result[server] = map[string]interface{}{
					"error": e.Error() + " " + string(jsonBlob),
				}
			}
		}(dn.Url(), forms)
		return nil
	})
	wg.Wait()
	return
}
