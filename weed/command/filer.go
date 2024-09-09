package command

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/filer"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	weed_server "github.com/seaweedfs/seaweedfs/weed/server"
	stats_collect "github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/spf13/viper"
	"google.golang.org/grpc/credentials/tls/certprovider"
	"google.golang.org/grpc/credentials/tls/certprovider/pemfile"
	"google.golang.org/grpc/reflection"
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
	masters                 *pb.ServerDiscovery
	mastersString           *string
	ip                      *string
	bindIp                  *string
	port                    *int
	portGrpc                *int
	publicPort              *int
	filerGroup              *string
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
	metricsHttpIp           *string
	saveToFilerLimit        *int
	defaultLevelDbDirectory *string
	concurrentUploadLimitMB *int
	debug                   *bool
	debugPort               *int
	localSocket             *string
	showUIDirectoryDelete   *bool
	downloadMaxMBps         *int
	diskType                *string
	allowedOrigins          *string
	exposeDirectoryData     *bool
	certProvider            certprovider.Provider
}

func init() {
	cmdFiler.Run = runFiler // break init cycle
	f.mastersString = cmdFiler.Flag.String("master", "localhost:9333", "comma-separated master servers or a single DNS SRV record of at least 1 master server, prepended with dnssrv+")
	f.filerGroup = cmdFiler.Flag.String("filerGroup", "", "share metadata with other filers in the same filerGroup")
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
	f.metricsHttpIp = cmdFiler.Flag.String("metricsIp", "", "metrics listen ip. If empty, default to same as -ip.bind option.")
	f.saveToFilerLimit = cmdFiler.Flag.Int("saveToFilerLimit", 0, "files smaller than this limit will be saved in filer store")
	f.defaultLevelDbDirectory = cmdFiler.Flag.String("defaultStoreDir", ".", "if filer.toml is empty, use an embedded filer store in the directory")
	f.concurrentUploadLimitMB = cmdFiler.Flag.Int("concurrentUploadLimitMB", 128, "limit total concurrent upload size")
	f.debug = cmdFiler.Flag.Bool("debug", false, "serves runtime profiling data, e.g., http://localhost:<debug.port>/debug/pprof/goroutine?debug=2")
	f.debugPort = cmdFiler.Flag.Int("debug.port", 6060, "http port for debugging")
	f.localSocket = cmdFiler.Flag.String("localSocket", "", "default to /tmp/seaweedfs-filer-<port>.sock")
	f.showUIDirectoryDelete = cmdFiler.Flag.Bool("ui.deleteDir", true, "enable filer UI show delete directory button")
	f.downloadMaxMBps = cmdFiler.Flag.Int("downloadMaxMBps", 0, "download max speed for each download request, in MB per second")
	f.diskType = cmdFiler.Flag.String("disk", "", "[hdd|ssd|<tag>] hard drive or solid state drive or any tag")
	f.allowedOrigins = cmdFiler.Flag.String("allowedOrigins", "*", "comma separated list of allowed origins")
	f.exposeDirectoryData = cmdFiler.Flag.Bool("exposeDirectoryData", true, "whether to return directory metadata and content in Filer UI")

	// start s3 on filer
	filerStartS3 = cmdFiler.Flag.Bool("s3", false, "whether to start S3 gateway")
	filerS3Options.port = cmdFiler.Flag.Int("s3.port", 8333, "s3 server http listen port")
	filerS3Options.portHttps = cmdFiler.Flag.Int("s3.port.https", 0, "s3 server https listen port")
	filerS3Options.portGrpc = cmdFiler.Flag.Int("s3.port.grpc", 0, "s3 server grpc listen port")
	filerS3Options.domainName = cmdFiler.Flag.String("s3.domainName", "", "suffix of the host name in comma separated list, {bucket}.{domainName}")
	filerS3Options.allowedOrigins = cmdFiler.Flag.String("s3.allowedOrigins", "*", "comma separated list of allowed origins")
	filerS3Options.dataCenter = cmdFiler.Flag.String("s3.dataCenter", "", "prefer to read and write to volumes in this data center")
	filerS3Options.tlsPrivateKey = cmdFiler.Flag.String("s3.key.file", "", "path to the TLS private key file")
	filerS3Options.tlsCertificate = cmdFiler.Flag.String("s3.cert.file", "", "path to the TLS certificate file")
	filerS3Options.config = cmdFiler.Flag.String("s3.config", "", "path to the config file")
	filerS3Options.auditLogConfig = cmdFiler.Flag.String("s3.auditLogConfig", "", "path to the audit log config file")
	filerS3Options.allowEmptyFolder = cmdFiler.Flag.Bool("s3.allowEmptyFolder", true, "allow empty folders")
	filerS3Options.allowDeleteBucketNotEmpty = cmdFiler.Flag.Bool("s3.allowDeleteBucketNotEmpty", true, "allow recursive deleting all entries along with bucket")
	filerS3Options.localSocket = cmdFiler.Flag.String("s3.localSocket", "", "default to /tmp/seaweedfs-s3-<port>.sock")

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
	filerWebDavOptions.maxMB = cmdFiler.Flag.Int("webdav.maxMB", 4, "split files larger than the limit")
	filerWebDavOptions.filerRootPath = cmdFiler.Flag.String("webdav.filer.path", "/", "use this remote path from filer server")

	// start iam on filer
	filerStartIam = cmdFiler.Flag.Bool("iam", false, "whether to start IAM service")
	filerIamOptions.ip = cmdFiler.Flag.String("iam.ip", *f.ip, "iam server http listen ip address")
	filerIamOptions.port = cmdFiler.Flag.Int("iam.port", 8111, "iam server http listen port")
}

func filerLongDesc() string {
	desc := `start a file server which accepts REST operation for any files.

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

Supported Filer Stores:
`

	storeNames := make([]string, len(filer.Stores))
	for i, store := range filer.Stores {
		storeNames[i] = "\t" + store.GetName()
	}
	sort.Strings(storeNames)
	storeList := strings.Join(storeNames, "\n")
	return desc + storeList
}

var cmdFiler = &Command{
	UsageLine: "filer -port=8888 -master=<ip:port>[,<ip:port>]*",
	Short:     "start a file server that points to a master server, or a list of master servers",
	Long:      filerLongDesc(),
}

func runFiler(cmd *Command, args []string) bool {
	if *f.debug {
		go http.ListenAndServe(fmt.Sprintf(":%d", *f.debugPort), nil)
	}

	util.LoadSecurityConfiguration()

	switch {
	case *f.metricsHttpIp != "":
		// noting to do, use f.metricsHttpIp
	case *f.bindIp != "":
		*f.metricsHttpIp = *f.bindIp
	case *f.ip != "":
		*f.metricsHttpIp = *f.ip
	}
	go stats_collect.StartMetricsServer(*f.metricsHttpIp, *f.metricsHttpPort)

	filerAddress := pb.NewServerAddress(*f.ip, *f.port, *f.portGrpc).String()
	startDelay := time.Duration(2)
	if *filerStartS3 {
		filerS3Options.filer = &filerAddress
		filerS3Options.bindIp = f.bindIp
		filerS3Options.localFilerSocket = f.localSocket
		if *f.dataCenter != "" && *filerS3Options.dataCenter == "" {
			filerS3Options.dataCenter = f.dataCenter
		}
		go func(delay time.Duration) {
			time.Sleep(delay * time.Second)
			filerS3Options.startS3Server()
		}(startDelay)
		startDelay++
	}

	if *filerStartWebDav {
		filerWebDavOptions.filer = &filerAddress

		if *filerWebDavOptions.disk == "" {
			filerWebDavOptions.disk = f.diskType
		}

		go func(delay time.Duration) {
			time.Sleep(delay * time.Second)
			filerWebDavOptions.startWebDav()
		}(startDelay)
		startDelay++
	}

	if *filerStartIam {
		filerIamOptions.filer = &filerAddress
		filerIamOptions.masters = f.mastersString
		go func(delay time.Duration) {
			time.Sleep(delay * time.Second)
			filerIamOptions.startIamServer()
		}(startDelay)
	}

	f.masters = pb.ServerAddresses(*f.mastersString).ToServiceDiscovery()

	f.startFiler()

	return true
}

// GetCertificateWithUpdate Auto refreshing TSL certificate
func (fo *FilerOptions) GetCertificateWithUpdate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	certs, err := fo.certProvider.KeyMaterial(context.Background())
	if certs == nil {
		return nil, err
	}
	return &certs.Certs[0], err
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
	if *fo.allowedOrigins == "" {
		*fo.allowedOrigins = "*"
	}

	defaultLevelDbDirectory := util.ResolvePath(*fo.defaultLevelDbDirectory + "/filerldb2")

	filerAddress := pb.NewServerAddress(*fo.ip, *fo.port, *fo.portGrpc)

	fs, nfs_err := weed_server.NewFilerServer(defaultMux, publicVolumeMux, &weed_server.FilerOption{
		Masters:               fo.masters,
		FilerGroup:            *fo.filerGroup,
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
		ShowUIDirectoryDelete: *fo.showUIDirectoryDelete,
		DownloadMaxBytesPs:    int64(*fo.downloadMaxMBps) * 1024 * 1024,
		DiskType:              *fo.diskType,
		AllowedOrigins:        strings.Split(*fo.allowedOrigins, ","),
	})
	if nfs_err != nil {
		glog.Fatalf("Filer startup error: %v", nfs_err)
	}

	if *fo.publicPort != 0 {
		publicListeningAddress := util.JoinHostPort(*fo.bindIp, *fo.publicPort)
		glog.V(0).Infoln("Start Seaweed filer server", util.Version(), "public at", publicListeningAddress)
		publicListener, localPublicListener, e := util.NewIpAndLocalListeners(*fo.bindIp, *fo.publicPort, 0)
		if e != nil {
			glog.Fatalf("Filer server public listener error on port %d:%v", *fo.publicPort, e)
		}
		go func() {
			if e := http.Serve(publicListener, publicVolumeMux); e != nil {
				glog.Fatalf("Volume server fail to serve public: %v", e)
			}
		}()
		if localPublicListener != nil {
			go func() {
				if e := http.Serve(localPublicListener, publicVolumeMux); e != nil {
					glog.Errorf("Volume server fail to serve public: %v", e)
				}
			}()
		}
	}

	glog.V(0).Infof("Start Seaweed Filer %s at %s:%d", util.Version(), *fo.ip, *fo.port)
	filerListener, filerLocalListener, e := util.NewIpAndLocalListeners(
		*fo.bindIp, *fo.port,
		time.Duration(10)*time.Second,
	)
	if e != nil {
		glog.Fatalf("Filer listener error: %v", e)
	}

	// starting grpc server
	grpcPort := *fo.portGrpc
	grpcL, grpcLocalL, err := util.NewIpAndLocalListeners(*fo.bindIp, grpcPort, 0)
	if err != nil {
		glog.Fatalf("failed to listen on grpc port %d: %v", grpcPort, err)
	}
	grpcS := pb.NewGrpcServer(security.LoadServerTLS(util.GetViper(), "grpc.filer"))
	filer_pb.RegisterSeaweedFilerServer(grpcS, fs)
	reflection.Register(grpcS)
	if grpcLocalL != nil {
		go grpcS.Serve(grpcLocalL)
	}
	go grpcS.Serve(grpcL)

	httpS := &http.Server{Handler: defaultMux}
	if runtime.GOOS != "windows" {
		localSocket := *fo.localSocket
		if localSocket == "" {
			localSocket = fmt.Sprintf("/tmp/seaweedfs-filer-%d.sock", *fo.port)
		}
		if err := os.Remove(localSocket); err != nil && !os.IsNotExist(err) {
			glog.Fatalf("Failed to remove %s, error: %s", localSocket, err.Error())
		}
		go func() {
			// start on local unix socket
			filerSocketListener, err := net.Listen("unix", localSocket)
			if err != nil {
				glog.Fatalf("Failed to listen on %s: %v", localSocket, err)
			}
			httpS.Serve(filerSocketListener)
		}()
	}

	if viper.GetString("https.filer.key") != "" {
		certFile := viper.GetString("https.filer.cert")
		keyFile := viper.GetString("https.filer.key")
		caCertFile := viper.GetString("https.filer.ca")
		disbaleTlsVerifyClientCert := viper.GetBool("https.filer.disable_tls_verify_client_cert")

		pemfileOptions := pemfile.Options{
			CertFile:        certFile,
			KeyFile:         keyFile,
			RefreshDuration: security.CredRefreshingInterval,
		}
		if fo.certProvider, err = pemfile.NewProvider(pemfileOptions); err != nil {
			glog.Fatalf("pemfile.NewProvider(%v) failed: %v", pemfileOptions, err)
		}

		caCertPool := x509.NewCertPool()
		if caCertFile != "" {
			caCertFile, err := os.ReadFile(caCertFile)
			if err != nil {
				glog.Fatalf("error reading CA certificate: %v", err)
			}
			caCertPool.AppendCertsFromPEM(caCertFile)
		}

		clientAuth := tls.NoClientCert
		if !disbaleTlsVerifyClientCert {
			clientAuth = tls.RequireAndVerifyClientCert
		}

		httpS.TLSConfig = &tls.Config{
			GetCertificate: fo.GetCertificateWithUpdate,
			ClientAuth:     clientAuth,
			ClientCAs:      caCertPool,
		}

		if filerLocalListener != nil {
			go func() {
				if err := httpS.ServeTLS(filerLocalListener, "", ""); err != nil {
					glog.Errorf("Filer Fail to serve: %v", e)
				}
			}()
		}
		if err := httpS.ServeTLS(filerListener, "", ""); err != nil {
			glog.Fatalf("Filer Fail to serve: %v", e)
		}
	} else {
		if filerLocalListener != nil {
			go func() {
				if err := httpS.Serve(filerLocalListener); err != nil {
					glog.Errorf("Filer Fail to serve: %v", e)
				}
			}()
		}
		if err := httpS.Serve(filerListener); err != nil {
			glog.Fatalf("Filer Fail to serve: %v", e)
		}
	}
}
