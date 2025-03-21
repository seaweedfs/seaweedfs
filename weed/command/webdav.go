package command

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/user"
	"strconv"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	weed_server "github.com/seaweedfs/seaweedfs/weed/server"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var (
	webDavStandaloneOptions WebDavOption
)

type WebDavOption struct {
	filer          *string
	ipBind         *string
	filerRootPath  *string
	port           *int
	collection     *string
	replication    *string
	disk           *string
	tlsPrivateKey  *string
	tlsCertificate *string
	cacheDir       *string
	cacheSizeMB    *int64
	maxMB          *int
}

func init() {
	cmdWebDav.Run = runWebDav // break init cycle
	webDavStandaloneOptions.filer = cmdWebDav.Flag.String("filer", "localhost:8888", "filer server address")
	webDavStandaloneOptions.ipBind = cmdWebDav.Flag.String("ip.bind", "", "ip address to bind to. Default listen to all.")
	webDavStandaloneOptions.port = cmdWebDav.Flag.Int("port", 7333, "webdav server http listen port")
	webDavStandaloneOptions.collection = cmdWebDav.Flag.String("collection", "", "collection to create the files")
	webDavStandaloneOptions.replication = cmdWebDav.Flag.String("replication", "", "replication to create the files")
	webDavStandaloneOptions.disk = cmdWebDav.Flag.String("disk", "", "[hdd|ssd|<tag>] hard drive or solid state drive or any tag")
	webDavStandaloneOptions.tlsPrivateKey = cmdWebDav.Flag.String("key.file", "", "path to the TLS private key file")
	webDavStandaloneOptions.tlsCertificate = cmdWebDav.Flag.String("cert.file", "", "path to the TLS certificate file")
	webDavStandaloneOptions.cacheDir = cmdWebDav.Flag.String("cacheDir", os.TempDir(), "local cache directory for file chunks")
	webDavStandaloneOptions.cacheSizeMB = cmdWebDav.Flag.Int64("cacheCapacityMB", 0, "local cache capacity in MB")
	webDavStandaloneOptions.maxMB = cmdWebDav.Flag.Int("maxMB", 4, "split files larger than the limit")
	webDavStandaloneOptions.filerRootPath = cmdWebDav.Flag.String("filer.path", "/", "use this remote path from filer server")
}

var cmdWebDav = &Command{
	UsageLine: "webdav -port=7333 -filer=<ip:port>",
	Short:     "start a webdav server that is backed by a filer",
	Long: `start a webdav server that is backed by a filer.

`,
}

func runWebDav(cmd *Command, args []string) bool {

	util.LoadSecurityConfiguration()

	listenAddress := fmt.Sprintf("%s:%d", *webDavStandaloneOptions.ipBind, *webDavStandaloneOptions.port)
	glog.V(0).Infof("Starting Seaweed WebDav Server %s at %s", util.Version(), listenAddress)

	return webDavStandaloneOptions.startWebDav()

}

func (wo *WebDavOption) startWebDav() bool {

	// detect current user
	uid, gid := uint32(0), uint32(0)
	if u, err := user.Current(); err == nil {
		if parsedId, pe := strconv.ParseUint(u.Uid, 10, 32); pe == nil {
			uid = uint32(parsedId)
		}
		if parsedId, pe := strconv.ParseUint(u.Gid, 10, 32); pe == nil {
			gid = uint32(parsedId)
		}
	}

	// parse filer grpc address
	filerAddress := pb.ServerAddress(*wo.filer)

	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.client")

	var cipher bool
	// connect to filer
	for {
		err := pb.WithGrpcFilerClient(false, 0, filerAddress, grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
			resp, err := client.GetFilerConfiguration(context.Background(), &filer_pb.GetFilerConfigurationRequest{})
			if err != nil {
				return fmt.Errorf("get filer %s configuration: %v", filerAddress, err)
			}
			cipher = resp.Cipher
			return nil
		})
		if err != nil {
			glog.V(0).Infof("wait to connect to filer %s grpc address %s", *wo.filer, filerAddress.ToGrpcAddress())
			time.Sleep(time.Second)
		} else {
			glog.V(0).Infof("connected to filer %s grpc address %s", *wo.filer, filerAddress.ToGrpcAddress())
			break
		}
	}

	ws, webdavServer_err := weed_server.NewWebDavServer(&weed_server.WebDavOption{
		Filer:          filerAddress,
		FilerRootPath:  *wo.filerRootPath,
		GrpcDialOption: grpcDialOption,
		Collection:     *wo.collection,
		Replication:    *wo.replication,
		DiskType:       *wo.disk,
		Uid:            uid,
		Gid:            gid,
		Cipher:         cipher,
		CacheDir:       util.ResolvePath(*wo.cacheDir),
		CacheSizeMB:    *wo.cacheSizeMB,
		MaxMB:          *wo.maxMB,
	})
	if webdavServer_err != nil {
		glog.Fatalf("WebDav Server startup error: %v", webdavServer_err)
	}

	httpS := &http.Server{Handler: ws.Handler}

	listenAddress := fmt.Sprintf("%s:%d", *wo.ipBind, *wo.port)
	webDavListener, err := util.NewListener(listenAddress, time.Duration(10)*time.Second)
	if err != nil {
		glog.Fatalf("WebDav Server listener on %s error: %v", listenAddress, err)
	}

	if *wo.tlsPrivateKey != "" {
		glog.V(0).Infof("Start Seaweed WebDav Server %s at https %s", util.Version(), listenAddress)
		if err = httpS.ServeTLS(webDavListener, *wo.tlsCertificate, *wo.tlsPrivateKey); err != nil {
			glog.Fatalf("WebDav Server Fail to serve: %v", err)
		}
	} else {
		glog.V(0).Infof("Start Seaweed WebDav Server %s at http %s", util.Version(), listenAddress)
		if err = httpS.Serve(webDavListener); err != nil {
			glog.Fatalf("WebDav Server Fail to serve: %v", err)
		}
	}

	return true

}
