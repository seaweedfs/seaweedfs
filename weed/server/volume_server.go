package weed_server

import (
	"google.golang.org/grpc"
	"net/http"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/storage"
	"github.com/spf13/viper"
)

type VolumeServer struct {
	MasterNodes    []string
	currentMaster  string
	pulseSeconds   int
	dataCenter     string
	rack           string
	store          *storage.Store
	guard          *security.Guard
	grpcDialOption grpc.DialOption

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

	v := viper.GetViper()
	signingKey := v.GetString("jwt.signing.key")
	enableUiAccess := v.GetBool("access.ui")

	vs := &VolumeServer{
		pulseSeconds:      pulseSeconds,
		dataCenter:        dataCenter,
		rack:              rack,
		needleMapKind:     needleMapKind,
		FixJpgOrientation: fixJpgOrientation,
		ReadRedirect:      readRedirect,
		grpcDialOption:    security.LoadClientTLS(viper.Sub("grpc"), "volume"),
	}
	vs.MasterNodes = masterNodes
	vs.store = storage.NewStore(port, ip, publicUrl, folders, maxCounts, vs.needleMapKind)

	vs.guard = security.NewGuard(whiteList, signingKey)

	handleStaticResources(adminMux)
	if signingKey == "" || enableUiAccess {
		// only expose the volume server details for safe environments
		adminMux.HandleFunc("/ui/index.html", vs.uiStatusHandler)
		adminMux.HandleFunc("/status", vs.guard.WhiteList(vs.statusHandler))
		adminMux.HandleFunc("/stats/counter", vs.guard.WhiteList(statsCounterHandler))
		adminMux.HandleFunc("/stats/memory", vs.guard.WhiteList(statsMemoryHandler))
		adminMux.HandleFunc("/stats/disk", vs.guard.WhiteList(vs.statsDiskHandler))
	}
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
	return security.GenJwt(vs.guard.SigningKey, fileId)
}
