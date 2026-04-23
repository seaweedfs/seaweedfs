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
	portmapBind        *string
}

func init() {
	cmdNfs.Run = runNfs // break init cycle
	nfsStandaloneOptions.filer = cmdNfs.Flag.String("filer", "localhost:8888", "filer server address")
	nfsStandaloneOptions.ipBind = cmdNfs.Flag.String("ip.bind", "127.0.0.1", "ip address to bind to. Defaults to loopback; override explicitly to expose the experimental server to the network.")
	nfsStandaloneOptions.port = cmdNfs.Flag.Int("port", 2049, "NFS server listen port")
	nfsStandaloneOptions.filerRootPath = cmdNfs.Flag.String("filer.path", "", "remote path from filer server to export. Required: no default is provided so operators must opt in to exporting a namespace subtree.")
	nfsStandaloneOptions.readOnly = cmdNfs.Flag.Bool("readOnly", false, "export the filer path as read only")
	nfsStandaloneOptions.allowedClients = cmdNfs.Flag.String("allowedClients", "", "comma-separated client IPs, hostnames, or CIDRs allowed to connect")
	nfsStandaloneOptions.volumeServerAccess = cmdNfs.Flag.String("volumeServerAccess", "direct", "access volume servers by [direct|publicUrl|filerProxy]")
	nfsStandaloneOptions.portmapBind = cmdNfs.Flag.String("portmap.bind", "", "when set, bind a built-in portmap v2 responder on <ip>:111 so plain `mount -t nfs` works without client-side portmap bypass. Empty disables it. Binding port 111 requires root or CAP_NET_BIND_SERVICE and must not conflict with a system rpcbind.")
}

var cmdNfs = &Command{
	UsageLine: "nfs -port=2049 -filer=<ip:port> -filer.path=<exported subtree>",
	Short:     "start an experimental NFSv3 server backed by a filer",
	Long: `start an experimental NFSv3 server backed by a filer.

This command serves an experimental filer-native NFSv3 frontend with
deterministic filehandles, filer-backed metadata operations, and direct
volume-server data access for chunk reads and buffered writes.

Safer defaults (since export ACLs are still not implemented):

  - ip.bind defaults to 127.0.0.1, so the server is not reachable from
    other hosts unless you override it explicitly.
  - filer.path has no default; you must pick the subtree to export.

Override -ip.bind to a routable address only after you have reviewed
-allowedClients and the readiness of the rest of your deployment.

Mounting from a Linux client
----------------------------
The server does not run portmap/rpcbind by default. That means Linux
mount.nfs, which queries portmap on port 111 first, will fail with
"portmap query failed" against the plain form:

    mount -t nfs -o nfsvers=3,nolock <host>:/export /mnt

Either tell the client to bypass portmap:

    mount -t nfs -o nfsvers=3,nolock,port=2049,mountport=2049,\
        proto=tcp,mountproto=tcp <host>:/export /mnt

or enable the built-in portmap responder on the server:

    weed nfs ... -portmap.bind=0.0.0.0

Binding port 111 requires root or CAP_NET_BIND_SERVICE and must not
collide with a system rpcbind.
	`,
}

func runNfs(cmd *Command, args []string) bool {
	util.LoadSecurityConfiguration()

	if *nfsStandaloneOptions.ipBind == "" {
		*nfsStandaloneOptions.ipBind = "127.0.0.1"
	}

	if *nfsStandaloneOptions.filerRootPath == "" {
		glog.Errorf("-filer.path is required: pick an explicit subtree to export; exporting \"/\" is not a default")
		return false
	}
	if *nfsStandaloneOptions.filerRootPath == "/" {
		glog.Warningf("-filer.path=/ exports the entire filer namespace; ensure -allowedClients or -ip.bind constrains access")
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
		PortmapBind:        *nfsStandaloneOptions.portmapBind,
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
