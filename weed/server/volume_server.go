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

	needleMapKind           storage.NeedleMapType
	FixJpgOrientation       bool
	ReadRedirect            bool
	compactionBytePerSecond int64
}

func NewVolumeServer(adminMux, publicMux *http.ServeMux, ip string,
	port int, publicUrl string,
	folders []string, maxCounts []int,
	needleMapKind storage.NeedleMapType,
	masterNodes []string, pulseSeconds int,
	dataCenter string, rack string,
	whiteList []string,
	fixJpgOrientation bool,
	readRedirect bool,
	compactionMBPerSecond int,
) *VolumeServer {

	v := viper.GetViper()
	signingKey := v.GetString("jwt.signing.key")
	v.SetDefault("jwt.signing.expires_after_seconds", 10)
	expiresAfterSec := v.GetInt("jwt.signing.expires_after_seconds")
	enableUiAccess := v.GetBool("access.ui")

	vs := &VolumeServer{
		pulseSeconds:            pulseSeconds,
		dataCenter:              dataCenter,
		rack:                    rack,
		needleMapKind:           needleMapKind,
		FixJpgOrientation:       fixJpgOrientation,
		ReadRedirect:            readRedirect,
		grpcDialOption:          security.LoadClientTLS(viper.Sub("grpc"), "volume"),
		compactionBytePerSecond: int64(compactionMBPerSecond) * 1024 * 1024,
	}
	vs.MasterNodes = masterNodes
	vs.store = storage.NewStore(port, ip, publicUrl, folders, maxCounts, vs.needleMapKind)

	vs.guard = security.NewGuard(whiteList, signingKey, expiresAfterSec)

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
