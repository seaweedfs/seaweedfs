package weedserver

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"

	"net/url"
	"sync"

	"net"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/topology"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/chrislusf/seaweedfs/weed/weedpb"
	"github.com/golang/protobuf/proto"
)

func (ms *MasterServer) collectionDeleteHandler(w http.ResponseWriter, r *http.Request) {
	collection, ok := ms.Topo.GetCollection(r.FormValue("collection"))
	if !ok {
		writeJsonError(w, r, http.StatusBadRequest, fmt.Errorf("collection %s does not exist", r.FormValue("collection")))
		return
	}
	for _, server := range collection.ListVolumeServers() {
		_, err := util.Get(net.JoinHostPort(server.Ip, strconv.Itoa(server.Port)), "/admin/delete_collection", url.Values{"collection": r.Form["collection"]})
		if err != nil {
			writeJsonError(w, r, http.StatusInternalServerError, err)
			return
		}
	}
	ms.Topo.DeleteCollection(r.FormValue("collection"))
}

// deprecated
func (ms *MasterServer) dirJoinHandler(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}
	joinMessage := &weedpb.JoinMessage{}
	if err = proto.Unmarshal(body, joinMessage); err != nil {
		writeJsonError(w, r, http.StatusBadRequest, err)
		return
	}
	if joinMessage.Ip == "" {
		if ip, _, e := net.SplitHostPort(r.RemoteAddr); e == nil {
			joinMessage.Ip = ip
		} else {
			glog.V(2).Infof("SplitHostPort (%s) error, %v", r.RemoteAddr, e)
			joinMessage.Ip = r.RemoteAddr
		}
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
	type JoinResult struct {
		VolumeSizeLimit uint64 `json:"VolumeSizeLimit,omitempty"`
		SecretKey       string `json:"secretKey,omitempty"`
		Error           string `json:"error,omitempty"`
	}
	writeJsonQuiet(w, r, http.StatusOK, JoinResult{
		VolumeSizeLimit: uint64(ms.volumeSizeLimitMB) * 1024 * 1024,
		SecretKey:       string(ms.guard.GetSecretKey()),
	})
}

func (ms *MasterServer) dirJoin2Handler(w http.ResponseWriter, r *http.Request) {
	joinResp := &weedpb.JoinResponse{}
	joinMsgV2 := &weedpb.JoinMessageV2{}

	if err := readObjRequest(r, joinMsgV2); err != nil {
		joinResp.Error = err.Error()
		writeObjResponse(w, r, http.StatusBadRequest, joinResp)
		return
	}
	if joinMsgV2.Ip == "" {
		if ip, _, e := net.SplitHostPort(r.RemoteAddr); e == nil {
			joinMsgV2.Ip = ip
		} else {
			glog.V(2).Infof("SplitHostPort (%s) error, %v", r.RemoteAddr, e)
			joinMsgV2.Ip = r.RemoteAddr
		}
	}
	if glog.V(4) {
		jsonData, _ := json.Marshal(joinMsgV2)
		glog.V(4).Infoln("join proto:", string(jsonData))
	}

	ms.Topo.ProcessJoinMessageV2(joinMsgV2)

	joinResp.JoinKey = ms.Topo.GetJoinKey()
	if joinMsgV2.JoinKey != joinResp.JoinKey {
		joinResp.JoinIp = joinMsgV2.Ip
		joinResp.VolumeSizeLimit = ms.Topo.GetVolumeSizeLimit()
		joinResp.SecretKey = string(ms.guard.GetSecretKey())
		joinResp.CollectionSettings = ms.Topo.CollectionSettings.ToPbMessage()
	}
	writeObjResponse(w, r, http.StatusOK, joinResp)
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

func (ms *MasterServer) volumeCheckReplicateHandler(w http.ResponseWriter, r *http.Request) {
	ms.Topo.StartCheckReplicate()
	writeJsonQuiet(w, r, http.StatusOK, map[string]interface{}{"status": "running"})
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
	machines := ms.Topo.Lookup("", volumeId)
	if machines != nil && machines.Length() > 0 {
		var url string
		if r.URL.RawQuery != "" {
			url = util.NormalizeUrl(machines.PickForRead().PublicUrl) + r.URL.Path + "?" + r.URL.RawQuery
		} else {
			url = util.NormalizeUrl(machines.PickForRead().PublicUrl) + r.URL.Path
		}
		http.Redirect(w, r, url, http.StatusMovedPermanently)
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
		deleteForClientHandler(w, r, ms.Topo.GetRaftServer().Leader())
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
