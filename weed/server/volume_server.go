package weed_server

import (
	"net/http"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/storage"
)

type VolumeServer struct {
	MasterNodes   []string
	currentMaster string
	pulseSeconds  int
	dataCenter    string
	rack          string
	store         *storage.Store
	guard         *security.Guard

	needleMapKind     storage.NeedleMapType
	FixJpgOrientation bool
	ReadRedirect      bool
}

func NewVolumeServer(adminMux, publicMux *http.ServeMux, ip string,
	port int, publicUrl string,
	folders []string, maxCounts []int,
	needleMapKind storage.NeedleMapType,
	masterNodes []string, pulseSeconds int,
	dataCenter string, rack string,
	whiteList []string,
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
	vs.MasterNodes = masterNodes
	vs.store = storage.NewStore(port, ip, publicUrl, folders, maxCounts, vs.needleMapKind)

	vs.guard = security.NewGuard(whiteList, "")

	handleStaticResources(adminMux)
	adminMux.HandleFunc("/ui/index.html", vs.uiStatusHandler)
	adminMux.HandleFunc("/status", vs.guard.WhiteList(vs.statusHandler))
	adminMux.HandleFunc("/admin/assign_volume", vs.guard.WhiteList(vs.assignVolumeHandler))
	adminMux.HandleFunc("/admin/vacuum/check", vs.guard.WhiteList(vs.vacuumVolumeCheckHandler))
	adminMux.HandleFunc("/admin/vacuum/compact", vs.guard.WhiteList(vs.vacuumVolumeCompactHandler))
	adminMux.HandleFunc("/admin/vacuum/commit", vs.guard.WhiteList(vs.vacuumVolumeCommitHandler))
	adminMux.HandleFunc("/admin/vacuum/cleanup", vs.guard.WhiteList(vs.vacuumVolumeCleanupHandler))
	adminMux.HandleFunc("/admin/delete_collection", vs.guard.WhiteList(vs.deleteCollectionHandler))
	adminMux.HandleFunc("/admin/sync/status", vs.guard.WhiteList(vs.getVolumeSyncStatusHandler))
	adminMux.HandleFunc("/admin/sync/index", vs.guard.WhiteList(vs.getVolumeIndexContentHandler))
	adminMux.HandleFunc("/admin/sync/data", vs.guard.WhiteList(vs.getVolumeDataContentHandler))
	adminMux.HandleFunc("/admin/volume/mount", vs.guard.WhiteList(vs.getVolumeMountHandler))
	adminMux.HandleFunc("/admin/volume/unmount", vs.guard.WhiteList(vs.getVolumeUnmountHandler))
	adminMux.HandleFunc("/admin/volume/delete", vs.guard.WhiteList(vs.getVolumeDeleteHandler))
	adminMux.HandleFunc("/stats/counter", vs.guard.WhiteList(statsCounterHandler))
	adminMux.HandleFunc("/stats/memory", vs.guard.WhiteList(statsMemoryHandler))
	adminMux.HandleFunc("/stats/disk", vs.guard.WhiteList(vs.statsDiskHandler))
	adminMux.HandleFunc("/delete", vs.guard.WhiteList(vs.batchDeleteHandler))
	adminMux.HandleFunc("/", vs.privateStoreHandler)
	if publicMux != adminMux {
		// separated admin and public port
		handleStaticResources(publicMux)
		publicMux.HandleFunc("/", vs.publicReadOnlyHandler)
	}

	go vs.heartbeat()

	return vs
}

func (vs *VolumeServer) Shutdown() {
	glog.V(0).Infoln("Shutting down volume server...")
	vs.store.Close()
	glog.V(0).Infoln("Shut down successfully!")
}

func (vs *VolumeServer) jwt(fileId string) security.EncodedJwt {
	return security.GenJwt(vs.guard.SecretKey, fileId)
}
