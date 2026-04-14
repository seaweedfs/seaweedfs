package command

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	weed_server_nfs "github.com/seaweedfs/seaweedfs/weed/server/nfs"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/version"
)

var (
	nfsStandaloneOptions NfsOptions
)

type NfsOptions struct {
	filer              *string
	ipBind             *string
	port               *int
	filerRootPath      *string
	readOnly           *bool
	allowedClients     *string
	volumeServerAccess *string
}

func init() {
	cmdNfs.Run = runNfs // break init cycle
	nfsStandaloneOptions.filer = cmdNfs.Flag.String("filer", "localhost:8888", "filer server address")
	nfsStandaloneOptions.ipBind = cmdNfs.Flag.String("ip.bind", "", "ip address to bind to. Default listen to all.")
	nfsStandaloneOptions.port = cmdNfs.Flag.Int("port", 2049, "NFS server listen port")
	nfsStandaloneOptions.filerRootPath = cmdNfs.Flag.String("filer.path", "/", "use this remote path from filer server")
	nfsStandaloneOptions.readOnly = cmdNfs.Flag.Bool("readOnly", false, "export the filer path as read only")
	nfsStandaloneOptions.allowedClients = cmdNfs.Flag.String("allowedClients", "", "comma-separated client IPs, hostnames, or CIDRs allowed to connect")
	nfsStandaloneOptions.volumeServerAccess = cmdNfs.Flag.String("volumeServerAccess", "direct", "access volume servers by [direct|publicUrl|filerProxy]")
}

var cmdNfs = &Command{
	UsageLine: "nfs -port=2049 -filer=<ip:port>",
	Short:     "start an experimental NFSv3 server backed by a filer",
	Long: `start an experimental NFSv3 server backed by a filer.

This command serves an experimental filer-native NFSv3 frontend with
deterministic filehandles, filer-backed metadata operations, and direct
volume-server data access for chunk reads and buffered writes.
	`,
}

func runNfs(cmd *Command, args []string) bool {
	util.LoadSecurityConfiguration()

	if *nfsStandaloneOptions.ipBind == "" {
		*nfsStandaloneOptions.ipBind = "0.0.0.0"
	}

	listenAddress := fmt.Sprintf("%s:%d", *nfsStandaloneOptions.ipBind, *nfsStandaloneOptions.port)
	glog.V(0).Infof("Starting Seaweed NFS Server %s at %s", version.Version(), listenAddress)

	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.client")

	nfsServer, err := weed_server_nfs.NewServer(&weed_server_nfs.Option{
		Filer:              pb.ServerAddress(*nfsStandaloneOptions.filer),
		BindIp:             *nfsStandaloneOptions.ipBind,
		Port:               *nfsStandaloneOptions.port,
		FilerRootPath:      *nfsStandaloneOptions.filerRootPath,
		ReadOnly:           *nfsStandaloneOptions.readOnly,
		AllowedClients:     util.StringSplit(*nfsStandaloneOptions.allowedClients, ","),
		VolumeServerAccess: *nfsStandaloneOptions.volumeServerAccess,
		GrpcDialOption:     grpcDialOption,
	})
	if err != nil {
		glog.Errorf("NFS Server startup error: %v", err)
		return false
	}

	if err := nfsServer.Start(); err != nil {
		glog.Errorf("NFS Server startup error: %v", err)
		return false
	}

	return true
}
