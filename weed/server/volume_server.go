package weed_server

import (
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/storage"
	//"net/http/pprof"
)

type VolumeServer struct {
	masterNode   string
	mnLock       sync.RWMutex
	pulseSeconds int
	dataCenter   string
	rack         string
	store        *storage.Store
	read_guard   *security.Guard
	write_guard  *security.Guard

	needleMapKind     storage.NeedleMapType
	FixJpgOrientation bool
	ReadRedirect      bool
}

func NewVolumeServer(adminMux, publicMux *http.ServeMux, ip string,
	port int, publicUrl string,
	folders []string, maxCounts []int,
	needleMapKind storage.NeedleMapType,
	masterNode string, pulseSeconds int,
	dataCenter string, rack string,
	ipReadWhiteList []string,
	ipWriteWhiteList []string,
	rootWhiteList []string,
	fixJpgOrientation bool,
	readRedirect bool) *VolumeServer {
	vs := &VolumeServer{
		pulseSeconds:      pulseSeconds,
		dataCenter:        dataCenter,
		rack:              rack,
		needleMapKind:     needleMapKind,
		FixJpgOrientation: fixJpgOrientation,
		ReadRedirect:      readRedirect,
	}
	vs.SetMasterNode(masterNode)
	vs.store = storage.NewStore(port, ip, publicUrl, folders, maxCounts, vs.needleMapKind)

	vs.read_guard = security.NewGuard(ipReadWhiteList, rootWhiteList, "")
	vs.write_guard = security.NewGuard(ipWriteWhiteList, rootWhiteList, "")

	adminMux.HandleFunc("/ui/index.html", vs.read_guard.WhiteList(vs.uiStatusHandler))
	adminMux.HandleFunc("/status", vs.read_guard.WhiteList(vs.statusHandler))
	adminMux.HandleFunc("/admin/assign_volume", vs.write_guard.WhiteList(vs.assignVolumeHandler))
	adminMux.HandleFunc("/admin/vacuum/check", vs.write_guard.WhiteList(vs.vacuumVolumeCheckHandler))
	adminMux.HandleFunc("/admin/vacuum/compact", vs.write_guard.WhiteList(vs.vacuumVolumeCompactHandler))
	adminMux.HandleFunc("/admin/vacuum/commit", vs.write_guard.WhiteList(vs.vacuumVolumeCommitHandler))
	adminMux.HandleFunc("/admin/delete_collection", vs.write_guard.WhiteList(vs.deleteCollectionHandler))
	adminMux.HandleFunc("/admin/sync/status", vs.read_guard.WhiteList(vs.getVolumeSyncStatusHandler))
	adminMux.HandleFunc("/admin/sync/index", vs.write_guard.WhiteList(vs.getVolumeIndexContentHandler))
	adminMux.HandleFunc("/admin/sync/data", vs.write_guard.WhiteList(vs.getVolumeDataContentHandler))
	adminMux.HandleFunc("/stats/counter", vs.read_guard.WhiteList(statsCounterHandler))
	adminMux.HandleFunc("/stats/memory", vs.read_guard.WhiteList(statsMemoryHandler))
	adminMux.HandleFunc("/stats/disk", vs.read_guard.WhiteList(vs.statsDiskHandler))
	adminMux.HandleFunc("/delete", vs.write_guard.WhiteList(vs.batchDeleteHandler))
	adminMux.HandleFunc("/", vs.read_guard.WhiteList(vs.privateStoreHandler))
	if publicMux != adminMux {
		// separated admin and public port
		publicMux.HandleFunc("/favicon.ico", vs.faviconHandler)
		publicMux.HandleFunc("/", vs.publicReadOnlyHandler)
	}
	/*
	// add in profiling support
	adminMux.HandleFunc("/debug/pprof/", pprof.Index)
	adminMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	adminMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	adminMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	adminMux.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	adminMux.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	adminMux.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
	adminMux.Handle("/debug/pprof/block", pprof.Handler("block"))
	*/

	go func() {
		connected := true

		glog.V(0).Infof("Volume server bootstraps with master %s", vs.GetMasterNode())
		vs.store.SetBootstrapMaster(vs.GetMasterNode())
		vs.store.SetDataCenter(vs.dataCenter)
		vs.store.SetRack(vs.rack)
		for {
			glog.V(4).Infof("Volume server sending to master %s", vs.GetMasterNode())
			master, secretKey, err := vs.store.SendHeartbeatToMaster()
			if err == nil {
				if !connected {
					connected = true
					vs.SetMasterNode(master)
					vs.read_guard.SecretKey = secretKey
					vs.write_guard.SecretKey = secretKey
					glog.V(0).Infoln("Volume Server Connected with master at", master)
				}
			} else {
				glog.V(1).Infof("Volume Server Failed to talk with master %s: %v", vs.masterNode, err)
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
	vs.mnLock.RLock()
	defer vs.mnLock.RUnlock()
	return vs.masterNode
}

func (vs *VolumeServer) SetMasterNode(masterNode string) {
	vs.mnLock.Lock()
	defer vs.mnLock.Unlock()
	vs.masterNode = masterNode
}

func (vs *VolumeServer) Shutdown() {
	glog.V(0).Infoln("Shutting down volume server...")
	vs.store.Close()
	glog.V(0).Infoln("Shut down successfully!")
}

func (vs *VolumeServer) jwt(fileId string) security.EncodedJwt {
	return security.GenJwt(vs.read_guard.SecretKey, fileId)
}
