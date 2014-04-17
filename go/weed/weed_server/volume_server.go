package weed_server

import (
	"code.google.com/p/weed-fs/go/glog"
	"code.google.com/p/weed-fs/go/storage"
	"math/rand"
	"net/http"
	"time"
)

type VolumeServer struct {
	masterNode   string
	pulseSeconds int
	dataCenter   string
	rack         string
	whiteList    []string
	store        *storage.Store
}

func NewVolumeServer(r *http.ServeMux, ip string, port int, publicUrl string, folders []string, maxCounts []int,
	masterNode string, pulseSeconds int,
	dataCenter string, rack string,
	whiteList []string) *VolumeServer {
	vs := &VolumeServer{
		masterNode:   masterNode,
		pulseSeconds: pulseSeconds,
		dataCenter:   dataCenter,
		rack:         rack,
		whiteList:    whiteList,
	}
	vs.store = storage.NewStore(port, ip, publicUrl, folders, maxCounts)

	r.HandleFunc("/submit", secure(vs.whiteList, vs.submitFromVolumeServerHandler))
	r.HandleFunc("/status", secure(vs.whiteList, vs.statusHandler))
	r.HandleFunc("/admin/assign_volume", secure(vs.whiteList, vs.assignVolumeHandler))
	r.HandleFunc("/admin/vacuum_volume_check", secure(vs.whiteList, vs.vacuumVolumeCheckHandler))
	r.HandleFunc("/admin/vacuum_volume_compact", secure(vs.whiteList, vs.vacuumVolumeCompactHandler))
	r.HandleFunc("/admin/vacuum_volume_commit", secure(vs.whiteList, vs.vacuumVolumeCommitHandler))
	r.HandleFunc("/admin/freeze_volume", secure(vs.whiteList, vs.freezeVolumeHandler))
	r.HandleFunc("/admin/delete_collection", secure(vs.whiteList, vs.deleteCollectionHandler))
	r.HandleFunc("/stats/counter", secure(vs.whiteList, statsCounterHandler))
	r.HandleFunc("/stats/memory", secure(vs.whiteList, statsMemoryHandler))
	r.HandleFunc("/stats/disk", secure(vs.whiteList, vs.statsDiskHandler))
	r.HandleFunc("/delete", secure(vs.whiteList, vs.batchDeleteHandler))
	r.HandleFunc("/", vs.storeHandler)

	go func() {
		connected := true
		vs.store.SetBootstrapMaster(vs.masterNode)
		vs.store.SetDataCenter(vs.dataCenter)
		vs.store.SetRack(vs.rack)
		for {
			err := vs.store.Join()
			if err == nil {
				if !connected {
					connected = true
					glog.V(0).Infoln("Reconnected with master")
				}
			} else {
				glog.V(4).Infoln("Failing to talk with master:", err.Error())
				if connected {
					connected = false
				}
			}
			if connected {
				time.Sleep(time.Duration(float32(vs.pulseSeconds*1e3)*(1+rand.Float32())) * time.Millisecond)
			} else {
				time.Sleep(time.Duration(float32(vs.pulseSeconds*1e3)*0.25) * time.Millisecond)
			}
		}
	}()
	glog.V(0).Infoln("store joined at", vs.masterNode)

	return vs
}
