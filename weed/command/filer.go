package command

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"google.golang.org/grpc/reflection"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/server"
	stats_collect "github.com/chrislusf/seaweedfs/weed/stats"
	"github.com/chrislusf/seaweedfs/weed/util"
)

var (
	f                  FilerOptions
	filerStartS3       *bool
	filerS3Options     S3Options
	filerStartWebDav   *bool
	filerWebDavOptions WebDavOption
	filerStartIam      *bool
	filerIamOptions    IamOptions
)

type FilerOptions struct {
	masters                 []pb.ServerAddress
	mastersString           *string
	ip                      *string
	bindIp                  *string
	port                    *int
	portGrpc                *int
	publicPort              *int
	collection              *string
	defaultReplicaPlacement *string
	disableDirListing       *bool
	maxMB                   *int
	dirListingLimit         *int
	dataCenter              *string
	rack                    *string
	enableNotification      *bool
	disableHttp             *bool
	cipher                  *bool
	metricsHttpPort         *int
	saveToFilerLimit        *int
	defaultLevelDbDirectory *string
	concurrentUploadLimitMB *int
	debug                   *bool
	debugPort               *int
	localSocket             *string
}

func init() {
	cmdFiler.Run = runFiler // break init cycle
	f.mastersString = cmdFiler.Flag.String("master", "localhost:9333", "comma-separated master servers")
	f.collection = cmdFiler.Flag.String("collection", "", "all data will be stored in this default collection")
	f.ip = cmdFiler.Flag.String("ip", util.DetectedHostAddress(), "filer server http listen ip address")
	f.bindIp = cmdFiler.Flag.String("ip.bind", "", "ip address to bind to. If empty, default to same as -ip option.")
	f.port = cmdFiler.Flag.Int("port", 8888, "filer server http listen port")
	f.portGrpc = cmdFiler.Flag.Int("port.grpc", 0, "filer server grpc listen port")
	f.publicPort = cmdFiler.Flag.Int("port.readonly", 0, "readonly port opened to public")
	f.defaultReplicaPlacement = cmdFiler.Flag.String("defaultReplicaPlacement", "", "default replication type. If not specified, use master setting.")
	f.disableDirListing = cmdFiler.Flag.Bool("disableDirListing", false, "turn off directory listing")
	f.maxMB = cmdFiler.Flag.Int("maxMB", 4, "split files larger than the limit")
	f.dirListingLimit = cmdFiler.Flag.Int("dirListLimit", 100000, "limit sub dir listing size")
	f.dataCenter = cmdFiler.Flag.String("dataCenter", "", "prefer to read and write to volumes in this data center")
	f.rack = cmdFiler.Flag.String("rack", "", "prefer to write to volumes in this rack")
	f.disableHttp = cmdFiler.Flag.Bool("disableHttp", false, "disable http request, only gRpc operations are allowed")
	f.cipher = cmdFiler.Flag.Bool("encryptVolumeData", false, "encrypt data on volume servers")
	f.metricsHttpPort = cmdFiler.Flag.Int("metricsPort", 0, "Prometheus metrics listen port")
	f.saveToFilerLimit = cmdFiler.Flag.Int("saveToFilerLimit", 0, "files smaller than this limit will be saved in filer store")
	f.defaultLevelDbDirectory = cmdFiler.Flag.String("defaultStoreDir", ".", "if filer.toml is empty, use an embedded filer store in the directory")
	f.concurrentUploadLimitMB = cmdFiler.Flag.Int("concurrentUploadLimitMB", 128, "limit total concurrent upload size")
	f.debug = cmdFiler.Flag.Bool("debug", false, "serves runtime profiling data, e.g., http://localhost:<debug.port>/debug/pprof/goroutine?debug=2")
	f.debugPort = cmdFiler.Flag.Int("debug.port", 6060, "http port for debugging")
	f.localSocket = cmdFiler.Flag.String("localSocket", "", "default to /tmp/seaweedfs-filer-<port>.sock")

	// start s3 on filer
	filerStartS3 = cmdFiler.Flag.Bool("s3", false, "whether to start S3 gateway")
	filerS3Options.port = cmdFiler.Flag.Int("s3.port", 8333, "s3 server http listen port")
	filerS3Options.domainName = cmdFiler.Flag.String("s3.domainName", "", "suffix of the host name in comma separated list, {bucket}.{domainName}")
	filerS3Options.tlsPrivateKey = cmdFiler.Flag.String("s3.key.file", "", "path to the TLS private key file")
	filerS3Options.tlsCertificate = cmdFiler.Flag.String("s3.cert.file", "", "path to the TLS certificate file")
	filerS3Options.config = cmdFiler.Flag.String("s3.config", "", "path to the config file")
	filerS3Options.auditLogConfig = cmdFiler.Flag.String("s3.auditLogConfig", "", "path to the audit log config file")
	filerS3Options.allowEmptyFolder = cmdFiler.Flag.Bool("s3.allowEmptyFolder", true, "allow empty folders")

	// start webdav on filer
	filerStartWebDav = cmdFiler.Flag.Bool("webdav", false, "whether to start webdav gateway")
	filerWebDavOptions.port = cmdFiler.Flag.Int("webdav.port", 7333, "webdav server http listen port")
	filerWebDavOptions.collection = cmdFiler.Flag.String("webdav.collection", "", "collection to create the files")
	filerWebDavOptions.replication = cmdFiler.Flag.String("webdav.replication", "", "replication to create the files")
	filerWebDavOptions.disk = cmdFiler.Flag.String("webdav.disk", "", "[hdd|ssd|<tag>] hard drive or solid state drive or any tag")
	filerWebDavOptions.tlsPrivateKey = cmdFiler.Flag.String("webdav.key.file", "", "path to the TLS private key file")
	filerWebDavOptions.tlsCertificate = cmdFiler.Flag.String("webdav.cert.file", "", "path to the TLS certificate file")
	filerWebDavOptions.cacheDir = cmdFiler.Flag.String("webdav.cacheDir", os.TempDir(), "local cache directory for file chunks")
	filerWebDavOptions.cacheSizeMB = cmdFiler.Flag.Int64("webdav.cacheCapacityMB", 0, "local cache capacity in MB")

	// start iam on filer
	filerStartIam = cmdFiler.Flag.Bool("iam", false, "whether to start IAM service")
	filerIamOptions.port = cmdFiler.Flag.Int("iam.port", 8111, "iam server http listen port")
}

var cmdFiler = &Command{
	UsageLine: "filer -port=8888 -master=<ip:port>[,<ip:port>]*",
	Short:     "start a file server that points to a master server, or a list of master servers",
	Long: `start a file server which accepts REST operation for any files.

	//create or overwrite the file, the directories /path/to will be automatically created
	POST /path/to/file
	//get the file content
	GET /path/to/file
	//create or overwrite the file, the filename in the multipart request will be used
	POST /path/to/
	//return a json format subdirectory and files listing
	GET /path/to/

	The configuration file "filer.toml" is read from ".", "$HOME/.seaweedfs/", "/usr/local/etc/seaweedfs/", or "/etc/seaweedfs/", in that order.
	If the "filer.toml" is not found, an embedded filer store will be created under "-defaultStoreDir".

	The example filer.toml configuration file can be generated by "weed scaffold -config=filer"

`,
}

func runFiler(cmd *Command, args []string) bool {
	if *f.debug {
		go http.ListenAndServe(fmt.Sprintf(":%d", *f.debugPort), nil)
	}

	util.LoadConfiguration("security", false)

	go stats_collect.StartMetricsServer(*f.metricsHttpPort)

	filerAddress := util.JoinHostPort(*f.ip, *f.port)
	startDelay := time.Duration(2)
	if *filerStartS3 {
		filerS3Options.filer = &filerAddress
		filerS3Options.bindIp = f.bindIp
		filerS3Options.localFilerSocket = f.localSocket
		go func() {
			time.Sleep(startDelay * time.Second)
			filerS3Options.startS3Server()
		}()
		startDelay++
	} else {
		*f.localSocket = ""
	}

	if *filerStartWebDav {
		filerWebDavOptions.filer = &filerAddress
		go func() {
			time.Sleep(startDelay * time.Second)
			filerWebDavOptions.startWebDav()
		}()
		startDelay++
	}

	if *filerStartIam {
		filerIamOptions.filer = &filerAddress
		filerIamOptions.masters = f.mastersString
		go func() {
			time.Sleep(startDelay * time.Second)
			filerIamOptions.startIamServer()
		}()
	}

	f.masters = pb.ServerAddresses(*f.mastersString).ToAddresses()

	f.startFiler()

	return true
}

func (fo *FilerOptions) startFiler() {

	defaultMux := http.NewServeMux()
	publicVolumeMux := defaultMux

	if *fo.publicPort != 0 {
		publicVolumeMux = http.NewServeMux()
	}
	if *fo.portGrpc == 0 {
		*fo.portGrpc = 10000 + *fo.port
	}
	if *fo.bindIp == "" {
		*fo.bindIp = *fo.ip
	}

	defaultLevelDbDirectory := util.ResolvePath(*fo.defaultLevelDbDirectory + "/filerldb2")

	filerAddress := pb.NewServerAddress(*fo.ip, *fo.port, *fo.portGrpc)

	fs, nfs_err := weed_server.NewFilerServer(defaultMux, publicVolumeMux, &weed_server.FilerOption{
		Masters:               fo.masters,
		Collection:            *fo.collection,
		DefaultReplication:    *fo.defaultReplicaPlacement,
		DisableDirListing:     *fo.disableDirListing,
		MaxMB:                 *fo.maxMB,
		DirListingLimit:       *fo.dirListingLimit,
		DataCenter:            *fo.dataCenter,
		Rack:                  *fo.rack,
		DefaultLevelDbDir:     defaultLevelDbDirectory,
		DisableHttp:           *fo.disableHttp,
		Host:                  filerAddress,
		Cipher:                *fo.cipher,
		SaveToFilerLimit:      int64(*fo.saveToFilerLimit),
		ConcurrentUploadLimit: int64(*fo.concurrentUploadLimitMB) * 1024 * 1024,
	})
	if nfs_err != nil {
		glog.Fatalf("Filer startup error: %v", nfs_err)
	}

	if *fo.publicPort != 0 {
		publicListeningAddress := util.JoinHostPort(*fo.bindIp, *fo.publicPort)
		glog.V(0).Infoln("Start Seaweed filer server", util.Version(), "public at", publicListeningAddress)
		publicListener, e := util.NewListener(publicListeningAddress, 0)
		if e != nil {
			glog.Fatalf("Filer server public listener error on port %d:%v", *fo.publicPort, e)
		}
		go func() {
			if e := http.Serve(publicListener, publicVolumeMux); e != nil {
				glog.Fatalf("Volume server fail to serve public: %v", e)
			}
		}()
	}

	glog.V(0).Infof("Start Seaweed Filer %s at %s:%d", util.Version(), *fo.ip, *fo.port)
	filerListener, e := util.NewListener(
		util.JoinHostPort(*fo.bindIp, *fo.port),
		time.Duration(10)*time.Second,
	)
	if e != nil {
		glog.Fatalf("Filer listener error: %v", e)
	}

	// start on local unix socket
	if *fo.localSocket == "" {
		*fo.localSocket = fmt.Sprintf("/tmp/seaweefs-filer-%d.sock", *fo.port)
		if err := os.Remove(*fo.localSocket); err != nil && !os.IsNotExist(err) {
			glog.Fatalf("Failed to remove %s, error: %s", *fo.localSocket, err.Error())
		}
	}
	filerSocketListener, err := net.Listen("unix", *fo.localSocket)
	if err != nil {
		glog.Fatalf("Failed to listen on %s: %v", *fo.localSocket, err)
	}

	// starting grpc server
	grpcPort := *fo.portGrpc
	grpcL, err := util.NewListener(util.JoinHostPort(*fo.bindIp, grpcPort), 0)
	if err != nil {
		glog.Fatalf("failed to listen on grpc port %d: %v", grpcPort, err)
	}
	grpcS := pb.NewGrpcServer(security.LoadServerTLS(util.GetViper(), "grpc.filer"))
	filer_pb.RegisterSeaweedFilerServer(grpcS, fs)
	reflection.Register(grpcS)
	go grpcS.Serve(grpcL)

	httpS := &http.Server{Handler: defaultMux}
	go func() {
		httpS.Serve(filerSocketListener)
	}()
	if err := httpS.Serve(filerListener); err != nil {
		glog.Fatalf("Filer Fail to serve: %v", e)
	}

}
