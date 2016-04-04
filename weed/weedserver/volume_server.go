package weedserver

import (
	"math/rand"
	"net/http"
	"time"

	"net/http/pprof"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/chrislusf/seaweedfs/weed/weedpb"
)

type VolumeServer struct {
	pulseSeconds int
	store        *storage.Store
	guard        *security.Guard

	FixJpgOrientation bool
	ReadRedirect      bool
	ReadRemoteNeedle  bool
}

func NewVolumeServer(adminMux, publicMux *http.ServeMux, ip string,
	port int, publicUrl string,
	folders []string, maxCounts []int,
	needleMapKind storage.NeedleMapType,
	masterNode string, pulseSeconds int,
	dataCenter string, rack string,
	whiteList []string,
	fixJpgOrientation bool,
	readRedirect, readRemoteNeedle bool) *VolumeServer {
	vs := &VolumeServer{
		pulseSeconds:      pulseSeconds,
		FixJpgOrientation: fixJpgOrientation,
		ReadRedirect:      readRedirect,
		ReadRemoteNeedle:  readRemoteNeedle,
	}
	vs.store = storage.NewStore(port, ip, publicUrl, folders, maxCounts, needleMapKind)
	vs.store.SetBootstrapMaster(masterNode)
	vs.store.SetDataCenter(dataCenter)
	vs.store.SetRack(rack)

	vs.guard = security.NewGuard(whiteList, "")

	adminMux.HandleFunc("/ui/index.html", vs.uiStatusHandler)
	adminMux.HandleFunc("/status", vs.guard.WhiteList(vs.statusHandler))
	adminMux.HandleFunc("/admin/assign_volume", vs.guard.WhiteList(vs.assignVolumeHandler))
	adminMux.HandleFunc("/admin/vacuum/check", vs.guard.WhiteList(vs.vacuumVolumeCheckHandler))
	adminMux.HandleFunc("/admin/vacuum/compact", vs.guard.WhiteList(vs.vacuumVolumeCompactHandler))
	adminMux.HandleFunc("/admin/vacuum/commit", vs.guard.WhiteList(vs.vacuumVolumeCommitHandler))
	adminMux.HandleFunc("/admin/setting", vs.guard.WhiteList(vs.setVolumeOptionHandler))
	adminMux.HandleFunc("/admin/delete_collection", vs.guard.WhiteList(vs.deleteCollectionHandler))
	adminMux.HandleFunc("/admin/sync/status", vs.guard.WhiteList(vs.getVolumeSyncStatusHandler))
	adminMux.HandleFunc("/admin/sync/index", vs.guard.WhiteList(vs.getVolumeIndexContentHandler))
	adminMux.HandleFunc("/admin/sync/data", vs.guard.WhiteList(vs.getVolumeDataContentHandler))
	adminMux.HandleFunc("/admin/sync/vol_data", vs.guard.WhiteList(vs.getVolumeCleanDataHandler))
	adminMux.HandleFunc("/admin/sync/needle", vs.guard.WhiteList(vs.getNeedleHandler))
	adminMux.HandleFunc("/admin/task/new", vs.guard.WhiteList(vs.newTaskHandler))
	adminMux.HandleFunc("/admin/task/query", vs.guard.WhiteList(vs.queryTaskHandler))
	adminMux.HandleFunc("/admin/task/commit", vs.guard.WhiteList(vs.commitTaskHandler))
	adminMux.HandleFunc("/admin/task/clean", vs.guard.WhiteList(vs.cleanTaskHandler))
	adminMux.HandleFunc("/admin/task/all", vs.guard.WhiteList(vs.allTaskHandler))
	adminMux.HandleFunc("/stats/counter", vs.guard.WhiteList(statsCounterHandler))
	adminMux.HandleFunc("/stats/memory", vs.guard.WhiteList(statsMemoryHandler))
	adminMux.HandleFunc("/stats/disk", vs.guard.WhiteList(vs.statsDiskHandler))
	adminMux.HandleFunc("/debug/pprof/", vs.guard.WhiteList(pprof.Index))
	adminMux.HandleFunc("/debug/pprof/{name}", vs.guard.WhiteList(pprof.Index))
	adminMux.HandleFunc("/delete", vs.guard.WhiteList(vs.batchDeleteHandler))
	adminMux.HandleFunc("/", vs.privateStoreHandler)
	if publicMux != adminMux {
		// separated admin and public port
		publicMux.HandleFunc("/", vs.publicReadOnlyHandler)
	}

	go func() {
		connected := true
		glog.V(0).Infof("Volume server bootstraps with master %s", masterNode)

		for {
			err := vs.store.SendHeartbeatToMaster(func(s *weedpb.JoinResponse) {
				vs.guard.SetSecretKey(s.SecretKey)
			})
			if err == nil {
				if !connected {
					connected = true
					glog.V(0).Infoln("Volume Server Connected with master at", vs.GetMasterNode())
				}
			} else {
				glog.V(1).Infof("Volume Server Failed to talk with master %s: %v", vs.GetMasterNode(), err)
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

	return vs
}

func (vs *VolumeServer) GetMasterNode() string {
	return vs.store.GetMaster()
}

func (vs *VolumeServer) Shutdown() {
	glog.V(0).Infoln("Shutting down volume server...")
	vs.store.Close()
	glog.V(0).Infoln("Shut down successfully!")
}

func (vs *VolumeServer) jwt(fileId string) security.EncodedJwt {
	return security.GenJwt(vs.guard.GetSecretKey(), fileId)
}
