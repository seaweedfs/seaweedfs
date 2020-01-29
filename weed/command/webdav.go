package command

import (
	"fmt"
	"net/http"
	"os/user"
	"strconv"
	"time"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/server"
	"github.com/chrislusf/seaweedfs/weed/util"
)

var (
	webDavStandaloneOptions WebDavOption
)

type WebDavOption struct {
	filer          *string
	port           *int
	collection     *string
	tlsPrivateKey  *string
	tlsCertificate *string
}

func init() {
	cmdWebDav.Run = runWebDav // break init cycle
	webDavStandaloneOptions.filer = cmdWebDav.Flag.String("filer", "localhost:8888", "filer server address")
	webDavStandaloneOptions.port = cmdWebDav.Flag.Int("port", 7333, "webdav server http listen port")
	webDavStandaloneOptions.collection = cmdWebDav.Flag.String("collection", "", "collection to create the files")
	webDavStandaloneOptions.tlsPrivateKey = cmdWebDav.Flag.String("key.file", "", "path to the TLS private key file")
	webDavStandaloneOptions.tlsCertificate = cmdWebDav.Flag.String("cert.file", "", "path to the TLS certificate file")
}

var cmdWebDav = &Command{
	UsageLine: "webdav -port=7333 -filer=<ip:port>",
	Short:     "<unstable> start a webdav server that is backed by a filer",
	Long: `start a webdav server that is backed by a filer.

`,
}

func runWebDav(cmd *Command, args []string) bool {

	util.LoadConfiguration("security", false)

	glog.V(0).Infof("Starting Seaweed WebDav Server %s at https port %d", util.VERSION, *webDavStandaloneOptions.port)

	return webDavStandaloneOptions.startWebDav()

}

func (wo *WebDavOption) startWebDav() bool {

	filerGrpcAddress, err := parseFilerGrpcAddress(*wo.filer)
	if err != nil {
		glog.Fatal(err)
		return false
	}

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

	ws, webdavServer_err := weed_server.NewWebDavServer(&weed_server.WebDavOption{
		Filer:            *wo.filer,
		FilerGrpcAddress: filerGrpcAddress,
		GrpcDialOption:   security.LoadClientTLS(util.GetViper(), "grpc.client"),
		Collection:       *wo.collection,
		Uid:              uid,
		Gid:              gid,
	})
	if webdavServer_err != nil {
		glog.Fatalf("WebDav Server startup error: %v", webdavServer_err)
	}

	httpS := &http.Server{Handler: ws.Handler}

	listenAddress := fmt.Sprintf(":%d", *wo.port)
	webDavListener, err := util.NewListener(listenAddress, time.Duration(10)*time.Second)
	if err != nil {
		glog.Fatalf("WebDav Server listener on %s error: %v", listenAddress, err)
	}

	if *wo.tlsPrivateKey != "" {
		glog.V(0).Infof("Start Seaweed WebDav Server %s at https port %d", util.VERSION, *wo.port)
		if err = httpS.ServeTLS(webDavListener, *wo.tlsCertificate, *wo.tlsPrivateKey); err != nil {
			glog.Fatalf("WebDav Server Fail to serve: %v", err)
		}
	} else {
		glog.V(0).Infof("Start Seaweed WebDav Server %s at http port %d", util.VERSION, *wo.port)
		if err = httpS.Serve(webDavListener); err != nil {
			glog.Fatalf("WebDav Server Fail to serve: %v", err)
		}
	}

	return true

}
