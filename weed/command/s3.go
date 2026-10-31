package command

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/mux"
	"google.golang.org/grpc/reflection"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/s3_lifecycle_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/s3_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/vector_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api"
	"github.com/seaweedfs/seaweedfs/weed/s3api/iceberg"
	"github.com/seaweedfs/seaweedfs/weed/s3api/lance"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3tables"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3vectors"
	"github.com/seaweedfs/seaweedfs/weed/security"
	stats_collect "github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/grace"
	util_http "github.com/seaweedfs/seaweedfs/weed/util/http"
	"github.com/seaweedfs/seaweedfs/weed/util/version"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	s3StandaloneOptions S3Options
)

// S3Options holds CLI flags for the S3 gateway.
// Flags are registered in multiple commands: s3.go (standalone), server.go, filer.go, and mini.go.
// When adding a new field, update all four flag registration sites.
type S3Options struct {
	filer                     *string
	ip                        *string
	bindIp                    *string
	listenOnLocalhost         *bool
	port                      *int
	portHttps                 *int
	portGrpc                  *int
	portIceberg               *int
	portLance                 *int
	icebergCredentialRole     *string
	icebergCredentialDuration *int
	config                    *string
	iamConfig                 *string
	domainName                *string
	allowedOrigins            *string
	tlsPrivateKey             *string
	tlsCertificate            *string
	tlsCACertificate          *string
	tlsVerifyClientCert       *bool
	metricsHttpPort           *int
	metricsHttpIp             *string
	allowDeleteBucketNotEmpty *bool
	autoCreateBucket          *bool
	auditLogConfig            *string
	localFilerSocket          *string
	dataCenter                *string
	localSocket               *string
	idleTimeout               *int
	concurrentUploadLimitMB   *int
	concurrentFileUploadLimit *int
	enableIam                 *bool
	iamReadOnly               *bool
	debug                     *bool
	debugPort                 *int
	cipher                    *bool
	externalUrl               *string
	defaultFileMode           *string
	cacheSizeMB               *int64
	readerCacheSizeMB         *int64

	allowUntrustedRemoteEndpoints *bool
	allowBackslashInKey           *bool
	iamSelfServiceRequiresGrant   *bool
	// shutdownCtx, when non-nil, tells startS3Server to gracefully shut down
	// its HTTP/gRPC servers once the ctx is cancelled, in addition to on
	// interrupt. Used by weed mini to orchestrate an ordered shutdown.
	shutdownCtx context.Context

	// icebergBound/lanceBound record whether the auxiliary catalog listeners
	// bound their ports. The aux binds happen before the S3 listener serves,
	// so once the S3 port answers these are final; mini reads them to probe
	// and advertise only catalogs that are actually running instead of
	// whatever process may hold the configured port.
	icebergBound atomic.Bool
	lanceBound   atomic.Bool
}

func init() {
	cmdS3.Run = runS3 // break init cycle
	s3StandaloneOptions.filer = cmdS3.Flag.String("filer", "localhost:8888", "comma-separated filer server addresses for high availability")
	s3StandaloneOptions.ip = cmdS3.Flag.String("ip", "", "ip address advertised to the cluster. If empty, default to -ip.bind, or the auto-detected address.")
	s3StandaloneOptions.bindIp = cmdS3.Flag.String("ip.bind", "", "ip address to bind to. If empty, default to 0.0.0.0.")
	s3StandaloneOptions.port = cmdS3.Flag.Int("port", 8333, "s3 server http listen port")
	s3StandaloneOptions.portHttps = cmdS3.Flag.Int("port.https", 0, "s3 server https listen port")
	s3StandaloneOptions.portGrpc = cmdS3.Flag.Int("port.grpc", 0, "s3 server grpc listen port")
	s3StandaloneOptions.portIceberg = cmdS3.Flag.Int("port.iceberg", 8181, "Iceberg REST Catalog server listen port (0 to disable)")
	s3StandaloneOptions.portLance = cmdS3.Flag.Int("port.lance", 9101, "Lance Namespace server listen port (0 to disable); credential vending uses -iceberg.credentialRole")
	s3StandaloneOptions.icebergCredentialRole = cmdS3.Flag.String("iceberg.credentialRole", "", "IAM role ARN the Iceberg catalog assumes to vend table-scoped credentials (empty disables vending)")
	s3StandaloneOptions.icebergCredentialDuration = cmdS3.Flag.Int("iceberg.credentialDurationSeconds", 3600, "lifetime of credentials vended by the Iceberg catalog")
	s3StandaloneOptions.domainName = cmdS3.Flag.String("domainName", "", "suffix of the host name in comma separated list, {bucket}.{domainName}")
	s3StandaloneOptions.allowedOrigins = cmdS3.Flag.String("allowedOrigins", "*", "comma separated list of allowed origins")
	s3StandaloneOptions.dataCenter = cmdS3.Flag.String("dataCenter", "", "prefer to read and write to volumes in this data center")
	s3StandaloneOptions.config = cmdS3.Flag.String("config", "", "path to the config file")
	s3StandaloneOptions.iamConfig = cmdS3.Flag.String("iam.config", "", "path to the advanced IAM config file")
	s3StandaloneOptions.auditLogConfig = cmdS3.Flag.String("auditLogConfig", "", "path to the audit log config file")
	s3StandaloneOptions.tlsPrivateKey = cmdS3.Flag.String("key.file", "", "path to the TLS private key file")
	s3StandaloneOptions.tlsCertificate = cmdS3.Flag.String("cert.file", "", "path to the TLS certificate file")
	s3StandaloneOptions.tlsCACertificate = cmdS3.Flag.String("cacert.file", "", "path to the TLS CA certificate file")
	s3StandaloneOptions.tlsVerifyClientCert = cmdS3.Flag.Bool("tlsVerifyClientCert", false, "whether to verify the client's certificate")
	s3StandaloneOptions.metricsHttpPort = cmdS3.Flag.Int("metricsPort", 0, "Prometheus metrics listen port")
	s3StandaloneOptions.metricsHttpIp = cmdS3.Flag.String("metricsIp", "", "metrics listen ip. If empty, default to same as -ip.bind option.")
	cmdS3.Flag.Bool("allowEmptyFolder", true, "deprecated, ignored. Empty folder cleanup is now automatic.")
	s3StandaloneOptions.allowDeleteBucketNotEmpty = cmdS3.Flag.Bool("allowDeleteBucketNotEmpty", false, "allow recursive deleting all entries along with bucket")
	s3StandaloneOptions.autoCreateBucket = cmdS3.Flag.Bool("autoCreateBucket", true, "create the bucket on upload if it does not exist, for admin identities only")
	s3StandaloneOptions.localFilerSocket = cmdS3.Flag.String("localFilerSocket", "", "local filer socket path")
	s3StandaloneOptions.localSocket = cmdS3.Flag.String("localSocket", "", "default to /tmp/seaweedfs-s3-<port>.sock")
	s3StandaloneOptions.idleTimeout = cmdS3.Flag.Int("idleTimeout", 120, "connection idle seconds")
	s3StandaloneOptions.concurrentUploadLimitMB = cmdS3.Flag.Int("concurrentUploadLimitMB", 0, "limit total concurrent upload size, 0 means unlimited")
	s3StandaloneOptions.concurrentFileUploadLimit = cmdS3.Flag.Int("concurrentFileUploadLimit", 0, "limit number of concurrent file uploads, 0 means unlimited")
	s3StandaloneOptions.enableIam = cmdS3.Flag.Bool("iam", true, "enable embedded IAM API on the same port")
	s3StandaloneOptions.iamReadOnly = cmdS3.Flag.Bool("iam.readOnly", true, "disable IAM write operations on this server")
	s3StandaloneOptions.debug = cmdS3.Flag.Bool("debug", false, "serves runtime profiling data via pprof on the port specified by -debug.port")
	s3StandaloneOptions.debugPort = cmdS3.Flag.Int("debug.port", 6060, "http port for debugging")
	s3StandaloneOptions.cipher = cmdS3.Flag.Bool("encryptVolumeData", false, "encrypt data on volume servers")
	s3StandaloneOptions.externalUrl = cmdS3.Flag.String("externalUrl", "", "the external URL clients use to connect (e.g. https://api.example.com:9000). Advertised to Iceberg and Lance clients, and tried first when verifying S3 signatures behind a reverse proxy. Falls back to S3_EXTERNAL_URL env var.")
	s3StandaloneOptions.defaultFileMode = cmdS3.Flag.String("defaultFileMode", "", "default file mode for S3 uploaded objects, e.g. 0660, 0644, 0666")
	s3StandaloneOptions.cacheSizeMB = cmdS3.Flag.Int64("cacheCapacityMB", 0, "in-memory chunk cache capacity in MB for S3 GETs shared across requests (0 disables)")
	s3StandaloneOptions.readerCacheSizeMB = cmdS3.Flag.Int64("readerCacheSizeMB", 0, "memory budget in MiB for downloaded and in-flight reader buffers across all S3 GETs (0 means unlimited)")
	s3StandaloneOptions.allowUntrustedRemoteEndpoints = cmdS3.Flag.Bool("allowUntrustedRemoteEndpoints", false, allowUntrustedRemoteEndpointsUsage)
	s3StandaloneOptions.allowBackslashInKey = cmdS3.Flag.Bool("allowBackslashInKey", false, allowBackslashInKeyUsage)
	s3StandaloneOptions.iamSelfServiceRequiresGrant = cmdS3.Flag.Bool("iam.selfServiceRequiresGrant", false, iamSelfServiceRequiresGrantUsage)
}

var cmdS3 = &Command{
	UsageLine: "s3 [-port=8333] [-filer=<ip:port>[,<ip:port>]...] [-config=</path/to/config.json>]",
	Short:     "start a s3 API compatible server that is backed by filer(s)",
	Long: `start a s3 API compatible server that is backed by filer(s).

	Multiple filer addresses can be specified for high availability, separated by commas.
	The S3 server will automatically failover between filers if one becomes unavailable.

	By default, you can use any access key and secret key to access the S3 APIs.
	To enable credential based access, create a config.json file similar to this:

{
  "identities": [
    {
      "name": "anonymous",
      "actions": [
        "Read"
      ]
    },
    {
      "name": "some_admin_user",
      "credentials": [
        {
          "accessKey": "some_access_key1",
          "secretKey": "some_secret_key1"
        }
      ],
      "actions": [
        "Admin",
        "Read",
        "List",
        "Tagging",
        "Write"
      ]
    },
    {
      "name": "some_read_only_user",
      "credentials": [
        {
          "accessKey": "some_access_key2",
          "secretKey": "some_secret_key2"
        }
      ],
      "actions": [
        "Read"
      ]
    },
    {
      "name": "some_normal_user",
      "credentials": [
        {
          "accessKey": "some_access_key3",
          "secretKey": "some_secret_key3"
        }
      ],
      "actions": [
        "Read",
        "List",
        "Tagging",
        "Write"
      ]
    },
    {
      "name": "user_limited_to_bucket1",
      "credentials": [
        {
          "accessKey": "some_access_key4",
          "secretKey": "some_secret_key4"
        }
      ],
      "actions": [
        "Read:bucket1",
        "List:bucket1",
        "Tagging:bucket1",
        "Write:bucket1"
      ]
    }
  ]
}

	Alternatively, you can use environment variables as fallback admin credentials:

	AWS_ACCESS_KEY_ID=your_access_key AWS_SECRET_ACCESS_KEY=your_secret_key weed s3

	Environment variables are only used when no S3 configuration file is provided
	and no configuration is available from the filer. This provides a simple way
	to get started without requiring configuration files.

`,
}

func runS3(cmd *Command, args []string) bool {
	if *s3StandaloneOptions.debug {
		grace.StartDebugServer(*s3StandaloneOptions.debugPort)
	}

	s3StandaloneOptions.resolvePaths()
	util.LoadSecurityConfiguration()

	switch {
	case *s3StandaloneOptions.metricsHttpIp != "":
		// noting to do, use s3StandaloneOptions.metricsHttpIp
	case *s3StandaloneOptions.bindIp != "":
		*s3StandaloneOptions.metricsHttpIp = *s3StandaloneOptions.bindIp
	}
	go stats_collect.StartMetricsServer(*s3StandaloneOptions.metricsHttpIp, *s3StandaloneOptions.metricsHttpPort)

	return s3StandaloneOptions.startS3Server()

}

// resolveExternalUrl returns the external URL from the flag or falls back to the S3_EXTERNAL_URL env var.
func (s3opt *S3Options) resolveExternalUrl() string {
	if s3opt.externalUrl != nil && *s3opt.externalUrl != "" {
		return *s3opt.externalUrl
	}
	return os.Getenv("S3_EXTERNAL_URL")
}

func (s3opt *S3Options) parseDefaultFileMode() (uint32, error) {
	if s3opt.defaultFileMode == nil || *s3opt.defaultFileMode == "" {
		return 0, nil
	}
	mode, err := strconv.ParseUint(*s3opt.defaultFileMode, 8, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid defaultFileMode %q: %v", *s3opt.defaultFileMode, err)
	}
	return uint32(mode), nil
}

// resolvePaths expands "~" in every user-supplied path flag so callers
// that share these pointers (e.g. server.go propagating s3Options.config
// to filerOptions.s3ConfigFile before startS3Server runs) see resolved
// values. Idempotent — safe to call from any entry point.
func (s3opt *S3Options) resolvePaths() {
	*s3opt.config = util.ResolvePath(*s3opt.config)
	*s3opt.iamConfig = util.ResolvePath(*s3opt.iamConfig)
	*s3opt.tlsCertificate = util.ResolvePath(*s3opt.tlsCertificate)
	*s3opt.tlsPrivateKey = util.ResolvePath(*s3opt.tlsPrivateKey)
	*s3opt.tlsCACertificate = util.ResolvePath(*s3opt.tlsCACertificate)
	*s3opt.auditLogConfig = util.ResolvePath(*s3opt.auditLogConfig)
}

func (s3opt *S3Options) startS3Server() bool {

	// Before the first filer dial below; gRPC caches conns and binds at dial time.
	util.SetOutboundLocalIP(*s3opt.bindIp)

	filerAddresses := pb.ServerAddresses(*s3opt.filer).ToAddresses()

	filerBucketsPath := "/buckets"
	filerGroup := ""
	var masterAddresses []pb.ServerAddress

	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.client")

	// metrics read from the filer
	var metricsAddress string
	var metricsIntervalSec int

	// chunk size for S3 uploads, read from the filer's -maxMB
	var filerMaxMB int32

	for {
		err := pb.WithOneOfGrpcFilerClients(false, filerAddresses, grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
			resp, err := client.GetFilerConfiguration(context.Background(), &filer_pb.GetFilerConfigurationRequest{})
			if err != nil {
				return fmt.Errorf("get filer configuration: %v", err)
			}
			filerBucketsPath = resp.DirBuckets
			filerGroup = resp.FilerGroup
			// Get master addresses for filer discovery
			masterAddresses = pb.ServerAddresses(strings.Join(resp.Masters, ",")).ToAddresses()
			metricsAddress, metricsIntervalSec = resp.MetricsAddress, int(resp.MetricsIntervalSec)
			filerMaxMB = int32(resp.MaxMb)
			glog.V(0).Infof("S3 read filer buckets dir: %s", filerBucketsPath)
			glog.V(0).Infof("S3 read filer maxMB: %d", filerMaxMB)
			if len(masterAddresses) > 0 {
				glog.V(0).Infof("S3 read master addresses for discovery: %v", masterAddresses)
			}
			return nil
		})
		if err != nil {
			glog.V(2).Infof("wait to connect to filers %v grpc address", filerAddresses)
			time.Sleep(time.Second)
		} else {
			glog.V(0).Infof("connected to filers %v", filerAddresses)
			break
		}
	}

	go stats_collect.LoopPushingMetric("s3", stats_collect.SourceName(uint32(*s3opt.port)), metricsAddress, metricsIntervalSec)

	router := mux.NewRouter().SkipClean(true)
	var localFilerSocket string
	// The filer only listens on its local socket outside Windows.
	if s3opt.localFilerSocket != nil && runtime.GOOS != "windows" {
		localFilerSocket = *s3opt.localFilerSocket
	}
	var s3ApiServer *s3api.S3ApiServer
	var s3ApiServer_err error

	// Create S3 server with optional advanced IAM integration
	var iamConfigPath string
	if s3opt.iamConfig != nil && *s3opt.iamConfig != "" {
		iamConfigPath = *s3opt.iamConfig
		glog.V(0).Infof("Starting S3 API Server with advanced IAM integration")
	} else {
		glog.V(0).Infof("Starting S3 API Server with standard IAM")
	}

	if *s3opt.portGrpc == 0 {
		*s3opt.portGrpc = 10000 + *s3opt.port
	}
	// The loopback companion listener is only kept while -ip.bind is unset.
	// An explicit -ip.bind pins each port to exactly that address.
	listenOnLocalhost := *s3opt.bindIp == ""
	if s3opt.listenOnLocalhost != nil {
		listenOnLocalhost = *s3opt.listenOnLocalhost
	}
	if *s3opt.bindIp == "" {
		*s3opt.bindIp = "0.0.0.0"
	}

	defaultFileMode, fileModeErr := s3opt.parseDefaultFileMode()
	if fileModeErr != nil {
		glog.Fatalf("S3 API Server startup error: %v", fileModeErr)
	}

	var readerCacheSizeMB int64
	if s3opt.readerCacheSizeMB != nil {
		readerCacheSizeMB = *s3opt.readerCacheSizeMB
	}

	s3ApiServer, s3ApiServer_err = s3api.NewS3ApiServer(router, &s3api.S3ApiServerOption{
		Filers:                    filerAddresses,
		Masters:                   masterAddresses,
		Port:                      *s3opt.port,
		Config:                    *s3opt.config,
		DomainName:                *s3opt.domainName,
		AllowedOrigins:            strings.Split(*s3opt.allowedOrigins, ","),
		BucketsPath:               filerBucketsPath,
		GrpcDialOption:            grpcDialOption,
		AllowDeleteBucketNotEmpty: *s3opt.allowDeleteBucketNotEmpty,
		AutoCreateBucket:          *s3opt.autoCreateBucket,
		LocalFilerSocket:          localFilerSocket,
		DataCenter:                *s3opt.dataCenter,
		FilerGroup:                filerGroup,
		IamConfig:                 iamConfigPath, // Advanced IAM config (optional)
		ConcurrentUploadLimit:     int64(*s3opt.concurrentUploadLimitMB) * 1024 * 1024,
		ConcurrentFileUploadLimit: int64(*s3opt.concurrentFileUploadLimit),
		EnableIam:                 *s3opt.enableIam, // Embedded IAM API (enabled by default)
		IamReadOnly:               *s3opt.iamReadOnly,
		Cipher:                    *s3opt.cipher, // encrypt data on volume servers
		Ip:                        *s3opt.ip,
		BindIp:                    *s3opt.bindIp,
		GrpcPort:                  *s3opt.portGrpc,
		ExternalUrl:               s3opt.resolveExternalUrl(),
		DefaultFileMode:           defaultFileMode,
		CacheSizeMB:               *s3opt.cacheSizeMB,
		ReaderCacheSizeMB:         readerCacheSizeMB,
		MaxMB:                     filerMaxMB,

		AllowUntrustedRemoteEndpoints: *s3opt.allowUntrustedRemoteEndpoints,
		AllowBackslashInKey:           *s3opt.allowBackslashInKey,
		IamSelfServiceRequiresGrant:   *s3opt.iamSelfServiceRequiresGrant,
	})
	if s3ApiServer_err != nil {
		glog.Fatalf("S3 API Server startup error: %v", s3ApiServer_err)
	}

	var httpServers []*http.Server

	// Start Iceberg REST Catalog server if enabled
	if *s3opt.portIceberg > 0 {
		if httpS := s3opt.startIcebergServer(s3ApiServer, listenOnLocalhost); httpS != nil {
			httpServers = append(httpServers, httpS)
		}
	}

	// Start Lance Namespace server if enabled
	if s3opt.portLance != nil && *s3opt.portLance > 0 {
		if httpS := s3opt.startLanceServer(s3ApiServer, listenOnLocalhost); httpS != nil {
			httpServers = append(httpServers, httpS)
		}
	}

	// The Vectors API rides the main -port listener whenever a weed-vector
	// backend is reachable — /s3vectors/ in the SigV4 credential scope picks
	// the vectors router.
	vectorsHandler := http.Handler(router)
	if vectorsRouter := s3opt.vectorsRouter(s3ApiServer, grpcDialOption, masterAddresses); vectorsRouter != nil {
		vr, mainRouter := vectorsRouter, router
		vectorsHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if isVectorsRequest(r) {
				vr.ServeHTTP(w, r)
				return
			}
			mainRouter.ServeHTTP(w, r)
		})
	}

	if runtime.GOOS != "windows" {
		localSocket := *s3opt.localSocket
		explicit := localSocket != ""
		if !explicit {
			localSocket = fmt.Sprintf("/tmp/seaweedfs-s3-%d.sock", *s3opt.port)
		}
		if s3SocketListener := pb.ListenOnLocalSocket(localSocket); s3SocketListener != nil {
			s3SocketListener = &util.Listener{Listener: s3SocketListener, Timeout: idleTimeoutSeconds(s3opt.idleTimeout)}
			socketServer := newHttpServer(vectorsHandler, nil)
			httpServers = append(httpServers, socketServer)
			go func() {
				if err := socketServer.Serve(s3SocketListener); err != nil && err != http.ErrServerClosed {
					glog.Fatalf("Failed to start S3 http server: %v", err)
				}
			}()
		}
	}

	listenAddress := fmt.Sprintf("%s:%d", *s3opt.bindIp, *s3opt.port)
	s3ApiListener, s3ApiLocalListener, err := util.NewIpAndLocalListeners(
		*s3opt.bindIp, *s3opt.port, idleTimeoutSeconds(s3opt.idleTimeout), listenOnLocalhost)
	if err != nil {
		glog.Fatalf("S3 API Server listener on %s error: %v", listenAddress, err)
	}

	if len(*s3opt.auditLogConfig) > 0 {
		s3err.InitAuditLog(*s3opt.auditLogConfig)
		if s3err.Logger != nil {
			defer s3err.Logger.Close()
		}
	}

	// starting grpc server
	grpcPort := *s3opt.portGrpc
	grpcL, grpcLocalL, err := util.NewIpAndLocalListeners(*s3opt.bindIp, grpcPort, 0, listenOnLocalhost)
	if err != nil {
		glog.Fatalf("s3 failed to listen on grpc port %d: %v", grpcPort, err)
	}
	grpcS := pb.NewGrpcServer(security.LoadServerTLS(util.GetViper(), "grpc.s3"))
	s3_pb.RegisterSeaweedS3IamCacheServer(grpcS, s3ApiServer)
	s3_lifecycle_pb.RegisterSeaweedS3LifecycleInternalServer(grpcS, s3ApiServer)
	reflection.Register(grpcS)
	if grpcLocalL != nil {
		go grpcS.Serve(grpcLocalL)
	}
	go grpcS.Serve(grpcL)
	pb.ServeGrpcOnLocalSocket(grpcS, grpcPort)
	stopGrpcServer := func() { gracefulStopGrpc(grpcS, 15*time.Second) }

	if *s3opt.tlsPrivateKey != "" {
		// Check for port conflict when both HTTP and HTTPS are enabled on the same port
		if *s3opt.portHttps > 0 && *s3opt.portHttps == *s3opt.port {
			glog.Fatalf("S3 API Server error: -s3.port.https (%d) cannot be the same as -s3.port (%d)", *s3opt.portHttps, *s3opt.port)
		}

		getCert, certProvider, err := security.NewReloadingServerCertificate(*s3opt.tlsCertificate, *s3opt.tlsPrivateKey)
		if err != nil {
			glog.Fatalf("S3 API Server failed to load HTTPS certificate: %v", err)
		}
		grace.OnInterrupt(certProvider.Close)

		caCertPool, err := loadClientCAPool(*s3opt.tlsCACertificate)
		if err != nil {
			glog.Fatalf("%v", err)
		}

		clientAuth := tls.NoClientCert
		if *s3opt.tlsVerifyClientCert {
			clientAuth = tls.RequireAndVerifyClientCert
		}

		tlsConfig := &tls.Config{
			GetCertificate: getCert,
			ClientAuth:     clientAuth,
			ClientCAs:      caCertPool,
		}
		err = security.FixTlsConfig(util.GetViper(), tlsConfig)
		if err != nil {
			glog.Fatalf("error with tls config: %v", err)
		}
		if *s3opt.portHttps == 0 {
			glog.V(0).Infof("Start Seaweed S3 API Server %s at https port %d", version.Version(), *s3opt.port)
			if s3ApiLocalListener != nil {
				localServer := newHttpServer(vectorsHandler, tlsConfig)
				httpServers = append(httpServers, localServer)
				go func() {
					if err := localServer.ServeTLS(s3ApiLocalListener, "", ""); err != nil && err != http.ErrServerClosed {
						glog.Fatalf("S3 API Server Fail to serve: %v", err)
					}
				}()
			}
			httpS := newHttpServer(vectorsHandler, tlsConfig)
			httpServers = append(httpServers, httpS)
			shutdown := s3opt.newShutdown(stopGrpcServer, s3ApiServer, httpServers)
			if err = httpS.ServeTLS(s3ApiListener, "", ""); err != nil && err != http.ErrServerClosed {
				glog.Fatalf("S3 API Server Fail to serve: %v", err)
			}
			// Serve returns when listeners close, before active requests finish.
			shutdown()
		} else {
			glog.V(0).Infof("Start Seaweed S3 API Server %s at https port %d", version.Version(), *s3opt.portHttps)
			s3ApiListenerHttps, s3ApiLocalListenerHttps, err := util.NewIpAndLocalListeners(
				*s3opt.bindIp, *s3opt.portHttps, idleTimeoutSeconds(s3opt.idleTimeout), listenOnLocalhost)
			if err != nil {
				glog.Fatalf("S3 API HTTPS listener on %s:%d error: %v", *s3opt.bindIp, *s3opt.portHttps, err)
			}
			if s3ApiLocalListenerHttps != nil {
				localHttpsServer := newHttpServer(vectorsHandler, tlsConfig)
				httpServers = append(httpServers, localHttpsServer)
				go func() {
					if err := localHttpsServer.ServeTLS(s3ApiLocalListenerHttps, "", ""); err != nil && err != http.ErrServerClosed {
						glog.Fatalf("S3 API Server Fail to serve: %v", err)
					}
				}()
			}
			httpsServer := newHttpServer(vectorsHandler, tlsConfig)
			httpServers = append(httpServers, httpsServer)
			go func() {
				if err := httpsServer.ServeTLS(s3ApiListenerHttps, "", ""); err != nil && err != http.ErrServerClosed {
					glog.Fatalf("S3 API Server Fail to serve: %v", err)
				}
			}()
		}
	}
	if *s3opt.tlsPrivateKey == "" || *s3opt.portHttps > 0 {
		glog.V(0).Infof("Start Seaweed S3 API Server %s at http port %d", version.Version(), *s3opt.port)
		if s3ApiLocalListener != nil {
			localServer := newHttpServer(vectorsHandler, nil)
			httpServers = append(httpServers, localServer)
			go func() {
				if err := localServer.Serve(s3ApiLocalListener); err != nil && err != http.ErrServerClosed {
					glog.Fatalf("S3 API Server Fail to serve: %v", err)
				}
			}()
		}
		httpS := newHttpServer(vectorsHandler, nil)
		httpServers = append(httpServers, httpS)
		shutdown := s3opt.newShutdown(stopGrpcServer, s3ApiServer, httpServers)
		if err = httpS.Serve(s3ApiListener); err != nil && err != http.ErrServerClosed {
			glog.Fatalf("S3 API Server Fail to serve: %v", err)
		}
		// Serve returns when listeners close, before active requests finish.
		shutdown()
	}

	return true

}

func (s3opt *S3Options) newShutdown(stopGrpc func(), s3ApiServer *s3api.S3ApiServer, httpServers []*http.Server) func() {
	shutdown := newGracefulShutdown(stopGrpc, s3ApiServer.Shutdown, httpServers...)
	grace.OnInterrupt(shutdown)
	if s3opt.shutdownCtx != nil {
		go func() {
			<-s3opt.shutdownCtx.Done()
			shutdown()
		}()
	}
	return shutdown
}

// startIcebergServer starts the Iceberg REST Catalog server on a separate port.
func (s3opt *S3Options) startIcebergServer(s3ApiServer *s3api.S3ApiServer, listenOnLocalhost bool) *http.Server {
	icebergRouter := mux.NewRouter().SkipClean(true)
	// warehouse/parent query values may legally contain ';', which Go's
	// url.ParseQuery would otherwise drop
	icebergRouter.Use(util_http.EscapeSemicolonsInQuery)

	// Create Iceberg server using the S3ApiServer as filer client
	icebergServer := iceberg.NewServer(s3ApiServer, s3ApiServer)
	icebergServer.SetCredentialValidator(s3ApiServer)
	if s3opt.icebergCredentialRole != nil && *s3opt.icebergCredentialRole != "" {
		duration := int64(0)
		if s3opt.icebergCredentialDuration != nil {
			duration = int64(*s3opt.icebergCredentialDuration)
		}
		s3ApiServer.SetIcebergCredentialRole(*s3opt.icebergCredentialRole, duration)
		icebergServer.SetCredentialVendor(icebergCredentialVendor{s3ApiServer})
	}
	icebergServer.SetS3Endpoint(s3opt.deriveS3AdvertisedEndpoint())
	icebergServer.RegisterRoutes(icebergRouter)

	listenAddress := fmt.Sprintf("%s:%d", *s3opt.bindIp, *s3opt.portIceberg)
	icebergListener, icebergLocalListener, err := util.NewIpAndLocalListeners(
		*s3opt.bindIp, *s3opt.portIceberg, idleTimeoutSeconds(s3opt.idleTimeout), listenOnLocalhost)
	if err != nil {
		// The catalog is auxiliary to the S3 gateway; a port conflict must not
		// take the whole server down.
		glog.Errorf("Iceberg REST Catalog listener on %s error: %v", listenAddress, err)
		return nil
	}
	s3opt.icebergBound.Store(true)

	glog.V(0).Infof("Start Iceberg REST Catalog Server at http://%s", listenAddress)

	httpS := newHttpServer(icebergRouter, nil)
	// Serve on localhost as well if we're bound to a different interface
	if icebergLocalListener != nil {
		go func() {
			if err := httpS.Serve(icebergLocalListener); err != nil && err != http.ErrServerClosed {
				glog.V(0).Infof("Iceberg localhost listener error: %v", err)
			}
		}()
	}
	go func() {
		if err := httpS.Serve(icebergListener); err != nil && err != http.ErrServerClosed {
			glog.Errorf("Iceberg REST Catalog Server Fail to serve: %v", err)
		}
	}()
	return httpS
}

// startLanceServer starts the Lance Namespace server on a separate port. It
// shares the Iceberg catalog's credential role: one deployment vends table
// credentials one way, whichever catalog the client speaks to.
func (s3opt *S3Options) startLanceServer(s3ApiServer *s3api.S3ApiServer, listenOnLocalhost bool) *http.Server {
	lanceRouter := mux.NewRouter().SkipClean(true)
	lanceRouter.Use(util_http.EscapeSemicolonsInQuery)

	lanceServer := lance.NewServer(s3ApiServer, s3ApiServer)
	lanceServer.SetCredentialValidator(s3ApiServer)
	if s3opt.icebergCredentialRole != nil && *s3opt.icebergCredentialRole != "" {
		lanceServer.SetCredentialVendor(lanceCredentialVendor{s3ApiServer})
	}
	lanceServer.SetS3Endpoint(s3opt.deriveLanceStorageEndpoint())
	lanceServer.SetS3Region(s3tables.DefaultRegion)
	lanceServer.RegisterRoutes(lanceRouter)

	listenAddress := fmt.Sprintf("%s:%d", *s3opt.bindIp, *s3opt.portLance)
	lanceListener, lanceLocalListener, err := util.NewIpAndLocalListeners(
		*s3opt.bindIp, *s3opt.portLance, idleTimeoutSeconds(s3opt.idleTimeout), listenOnLocalhost)
	if err != nil {
		glog.Errorf("Lance Namespace listener on %s error: %v", listenAddress, err)
		return nil
	}
	s3opt.lanceBound.Store(true)

	glog.V(0).Infof("Start Lance Namespace Server at http://%s", listenAddress)

	httpS := newHttpServer(lanceRouter, nil)
	if lanceLocalListener != nil {
		go func() {
			if err := httpS.Serve(lanceLocalListener); err != nil && err != http.ErrServerClosed {
				glog.V(0).Infof("Lance localhost listener error: %v", err)
			}
		}()
	}
	go func() {
		if err := httpS.Serve(lanceListener); err != nil && err != http.ErrServerClosed {
			glog.Errorf("Lance Namespace Server Fail to serve: %v", err)
		}
	}()
	return httpS
}

// vectorsRouter builds the S3 Vectors API router — workers register to the
// master and the backend resolves them lazily, so a worker that comes up
// after the gateway still gets picked up on first query.
func (s3opt *S3Options) vectorsRouter(s3ApiServer *s3api.S3ApiServer, grpcDialOption grpc.DialOption, masterAddresses []pb.ServerAddress) *mux.Router {
	if len(masterAddresses) == 0 {
		glog.Warningf("S3 Vectors API disabled: no weed-vector registered on master")
		return nil
	}
	backend := &vectorDiscovery{masters: masterAddresses, grpcDialOption: grpcDialOption, discover: discoverVectorWorkers}
	vectorsRouter := mux.NewRouter().SkipClean(true)

	vectorsServer := s3vectors.NewServer(s3ApiServer, s3ApiServer, backend)
	vectorsServer.SetRegion(s3tables.DefaultRegion)
	vectorsServer.SetAccountID(s3tables.DefaultAccountID)
	vectorsServer.RegisterRoutes(vectorsRouter)
	return vectorsRouter
}

// vectorDiscovery resolves the currently registered weed-vector through the
// master and caches the connection; workers joining after gateway startup
// are picked up on first query.
type vectorDiscovery struct {
	masters        []pb.ServerAddress
	grpcDialOption grpc.DialOption
	discover       func([]pb.ServerAddress, grpc.DialOption) []string
	mu             sync.Mutex
	worker         string
	conn           *grpc.ClientConn
	client         vector_pb.VectorServiceClient
	nextResolve    time.Time
}

// reset drops the cached worker so the next call rediscovers — invoked on
// transport errors only, and only when the failed client is still the
// cached one (a concurrent request may have already re-resolved).
func (d *vectorDiscovery) reset(c vector_pb.VectorServiceClient) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if c != d.client {
		return
	}
	if d.conn != nil {
		d.conn.Close()
	}
	d.worker, d.conn, d.client = "", nil, nil
	d.nextResolve = time.Time{}
}

// Only Unavailable proves a transport failure — a request deadline or an
// application-level error can fire on a healthy connection.
func isTransportError(err error) bool {
	return status.Code(err) == codes.Unavailable
}

func (d *vectorDiscovery) resolve() (vector_pb.VectorServiceClient, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	// Measured from lookup end so a slow lookup cannot leave the next one already due.
	defer func() {
		interval := vectorRecheckInterval
		if d.client != nil {
			interval = vectorResolveInterval
		}
		d.nextResolve = time.Now().Add(interval)
	}()
	workers := d.discover(d.masters, d.grpcDialOption)
	if len(workers) == 0 {
		return nil, errNoVectorWorker
	}
	worker := workers[0]
	for _, w := range workers {
		if w == d.worker {
			worker = w
			break
		}
	}
	if worker != d.worker {
		if d.conn != nil {
			d.conn.Close()
		}
		conn, err := pb.GrpcDial(context.Background(), worker, false, d.grpcDialOption)
		if err != nil {
			return nil, err
		}
		d.worker, d.conn = worker, conn
		d.client = vector_pb.NewVectorServiceClient(conn)
	}
	return d.client, nil
}

// A live client refreshes membership at most every vectorResolveInterval.
// With no usable backend the recheck is much more frequent: a worker that
// registers a moment after a lookup must surface inside a caller's retry
// budget, while an absent one still cannot flood the master with lookups.
const (
	vectorResolveInterval = 5 * time.Second
	vectorRecheckInterval = 500 * time.Millisecond
)

var errNoVectorWorker = status.Error(codes.Unavailable, "no weed-vector registered on master")

func (d *vectorDiscovery) cli() (vector_pb.VectorServiceClient, error) {
	d.mu.Lock()
	cli := d.client
	if !time.Now().Before(d.nextResolve) {
		interval := vectorRecheckInterval
		if cli != nil {
			interval = vectorResolveInterval
		}
		d.nextResolve = time.Now().Add(interval)
		d.mu.Unlock()
		return d.resolve()
	}
	d.mu.Unlock()
	if cli != nil {
		return cli, nil
	}
	return nil, errNoVectorWorker
}

func (d *vectorDiscovery) CreateIndex(ctx context.Context, in *vector_pb.CreateIndexRequest, opts ...grpc.CallOption) (resp *vector_pb.CreateIndexResponse, err error) {
	for attempt := 0; attempt < 2; attempt++ {
		var c vector_pb.VectorServiceClient
		if c, err = d.cli(); err != nil {
			return nil, err
		}
		if resp, err = c.CreateIndex(ctx, in, opts...); err == nil {
			return resp, nil
		}
		if !isTransportError(err) {
			return nil, err
		}
		d.reset(c)
	}
	return nil, err
}
func (d *vectorDiscovery) DropIndex(ctx context.Context, in *vector_pb.DropIndexRequest, opts ...grpc.CallOption) (resp *vector_pb.DropIndexResponse, err error) {
	for attempt := 0; attempt < 2; attempt++ {
		var c vector_pb.VectorServiceClient
		if c, err = d.cli(); err != nil {
			return nil, err
		}
		if resp, err = c.DropIndex(ctx, in, opts...); err == nil {
			return resp, nil
		}
		if !isTransportError(err) {
			return nil, err
		}
		d.reset(c)
	}
	return nil, err
}
func (d *vectorDiscovery) PutVectors(ctx context.Context, in *vector_pb.PutVectorsRequest, opts ...grpc.CallOption) (resp *vector_pb.PutVectorsResponse, err error) {
	for attempt := 0; attempt < 2; attempt++ {
		var c vector_pb.VectorServiceClient
		if c, err = d.cli(); err != nil {
			return nil, err
		}
		if resp, err = c.PutVectors(ctx, in, opts...); err == nil {
			return resp, nil
		}
		if !isTransportError(err) {
			return nil, err
		}
		d.reset(c)
	}
	return nil, err
}
func (d *vectorDiscovery) GetVectors(ctx context.Context, in *vector_pb.GetVectorsRequest, opts ...grpc.CallOption) (resp *vector_pb.GetVectorsResponse, err error) {
	for attempt := 0; attempt < 2; attempt++ {
		var c vector_pb.VectorServiceClient
		if c, err = d.cli(); err != nil {
			return nil, err
		}
		if resp, err = c.GetVectors(ctx, in, opts...); err == nil {
			return resp, nil
		}
		if !isTransportError(err) {
			return nil, err
		}
		d.reset(c)
	}
	return nil, err
}
func (d *vectorDiscovery) DeleteVectors(ctx context.Context, in *vector_pb.DeleteVectorsRequest, opts ...grpc.CallOption) (resp *vector_pb.DeleteVectorsResponse, err error) {
	for attempt := 0; attempt < 2; attempt++ {
		var c vector_pb.VectorServiceClient
		if c, err = d.cli(); err != nil {
			return nil, err
		}
		if resp, err = c.DeleteVectors(ctx, in, opts...); err == nil {
			return resp, nil
		}
		if !isTransportError(err) {
			return nil, err
		}
		d.reset(c)
	}
	return nil, err
}
func (d *vectorDiscovery) ListVectors(ctx context.Context, in *vector_pb.ListVectorsRequest, opts ...grpc.CallOption) (resp *vector_pb.ListVectorsResponse, err error) {
	for attempt := 0; attempt < 2; attempt++ {
		var c vector_pb.VectorServiceClient
		if c, err = d.cli(); err != nil {
			return nil, err
		}
		if resp, err = c.ListVectors(ctx, in, opts...); err == nil {
			return resp, nil
		}
		if !isTransportError(err) {
			return nil, err
		}
		d.reset(c)
	}
	return nil, err
}
func (d *vectorDiscovery) QueryVectors(ctx context.Context, in *vector_pb.QueryVectorsRequest, opts ...grpc.CallOption) (resp *vector_pb.QueryVectorsResponse, err error) {
	for attempt := 0; attempt < 2; attempt++ {
		var c vector_pb.VectorServiceClient
		if c, err = d.cli(); err != nil {
			return nil, err
		}
		if resp, err = c.QueryVectors(ctx, in, opts...); err == nil {
			return resp, nil
		}
		if !isTransportError(err) {
			return nil, err
		}
		d.reset(c)
	}
	return nil, err
}

// isVectorsRequest routes by the SigV4 credential scope — the s3vectors
// service name lives there, not in the path.
func isVectorsRequest(r *http.Request) bool {
	if v := r.Header.Get("Authorization"); v != "" {
		return strings.Contains(v, "/s3vectors/")
	}
	return strings.Contains(r.URL.Query().Get("X-Amz-Credential"), "/s3vectors/")
}

func loadClientCAPool(caCertFile string) (*x509.CertPool, error) {
	pool := x509.NewCertPool()
	if caCertFile == "" {
		return pool, nil
	}
	pem, err := os.ReadFile(caCertFile)
	if err != nil {
		return nil, fmt.Errorf("error reading CA certificate: %w", err)
	}
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("no valid certificates found in %s", caCertFile)
	}
	return pool, nil
}

// deriveLanceStorageEndpoint picks the endpoint the Lance namespace puts in
// storage_options. It falls back to the advertised -ip where the Iceberg
// derivation gives up, because the two clients are not in the same position: a
// Spark or Trino Iceberg client brings its own s3.endpoint and advertising the
// wrong one hijacks it, whereas storage_options is the only place a Lance
// client learns where the store is. Without one, object_store quietly falls
// back to real AWS S3 and the failure reads like a credentials problem.
func (s3opt *S3Options) deriveLanceStorageEndpoint() string {
	if endpoint := s3opt.deriveS3AdvertisedEndpoint(); endpoint != "" {
		return endpoint
	}
	host := ""
	if s3opt.ip != nil {
		host = *s3opt.ip
	}
	switch host {
	case "", "0.0.0.0", "::", "[::]":
		return ""
	}
	scheme := "http"
	port := 0
	if s3opt.port != nil {
		port = *s3opt.port
	}
	if s3opt.tlsPrivateKey != nil && *s3opt.tlsPrivateKey != "" {
		scheme = "https"
		if s3opt.portHttps != nil && *s3opt.portHttps > 0 {
			port = *s3opt.portHttps
		}
	}
	return fmt.Sprintf("%s://%s", scheme, util.JoinHostPort(host, port))
}

// deriveS3AdvertisedEndpoint builds the S3 endpoint URL to advertise to
// Iceberg catalog clients as part of LoadTable FileIO config. To avoid
// hijacking correctly-configured clients (Spark/Trino/PyIceberg all bring
// their own s3.endpoint), advertising is strictly opt-in and returns ""
// whenever no reliable value is available:
//   - -s3.externalUrl / S3_EXTERNAL_URL wins and supports reverse-proxy
//     deployments.
//   - Otherwise the bind IP is used only when it is explicit and not a
//     wildcard (0.0.0.0 / ::), with the scheme picked from TLS config and
//     IPv6 literals bracketed via util.JoinHostPort.
//
// See issue #9103.
func (s3opt *S3Options) deriveS3AdvertisedEndpoint() string {
	if ext := strings.TrimRight(s3opt.resolveExternalUrl(), "/"); ext != "" {
		return ext
	}

	host := ""
	if s3opt.bindIp != nil {
		host = *s3opt.bindIp
	}
	switch host {
	case "", "0.0.0.0", "::", "[::]":
		return ""
	}

	scheme := "http"
	port := 0
	if s3opt.port != nil {
		port = *s3opt.port
	}
	if s3opt.tlsPrivateKey != nil && *s3opt.tlsPrivateKey != "" {
		scheme = "https"
		if s3opt.portHttps != nil && *s3opt.portHttps > 0 {
			port = *s3opt.portHttps
		}
	}
	return fmt.Sprintf("%s://%s", scheme, util.JoinHostPort(host, port))
}

// icebergCredentialVendor adapts the S3 gateway's STS-backed vending to the
// catalog's interface, keeping the two packages independent of each other.
type icebergCredentialVendor struct {
	server *s3api.S3ApiServer
}

func (v icebergCredentialVendor) VendTableCredentials(ctx context.Context, principal, bucket, prefix string, readOnly bool) (*iceberg.VendedCredentials, error) {
	credentials, err := v.server.VendTableCredentials(ctx, principal, bucket, prefix, readOnly)
	if err != nil || credentials == nil {
		return nil, err
	}
	return &iceberg.VendedCredentials{
		AccessKeyID:     credentials.AccessKeyID,
		SecretAccessKey: credentials.SecretAccessKey,
		SessionToken:    credentials.SessionToken,
		Expiration:      credentials.Expiration,
	}, nil
}

// lanceCredentialVendor adapts the S3 gateway's STS-backed vending to the Lance
// namespace's interface, keeping the two packages independent of each other.
type lanceCredentialVendor struct {
	server *s3api.S3ApiServer
}

func (v lanceCredentialVendor) VendTableCredentials(ctx context.Context, principal, bucket, prefix string, readOnly bool) (*lance.VendedCredentials, error) {
	credentials, err := v.server.VendTableCredentials(ctx, principal, bucket, prefix, readOnly)
	if err != nil || credentials == nil {
		return nil, err
	}
	return &lance.VendedCredentials{
		AccessKeyID:     credentials.AccessKeyID,
		SecretAccessKey: credentials.SecretAccessKey,
		SessionToken:    credentials.SessionToken,
		Expiration:      credentials.Expiration,
	}, nil
}

// discoverVectorWorker resolves one registered weed-vector server through the
// master's cluster-nodes list; OSS fans every vector call to that single
// backend.
// discoverVectorWorkers returns every registered weed-vector address —
// callers keep their current worker when it is still listed so
// membership-order changes cannot churn live connections.
func discoverVectorWorkers(masters []pb.ServerAddress, grpcDialOption grpc.DialOption) []string {
	for _, master := range masters {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		conn, err := pb.GrpcDial(ctx, master.ToGrpcAddress(), false, grpcDialOption)
		if err != nil {
			cancel()
			continue
		}
		resp, err := master_pb.NewSeaweedClient(conn).ListClusterNodes(ctx, &master_pb.ListClusterNodesRequest{
			ClientType: "vector",
		})
		conn.Close()
		cancel()
		if err != nil {
			continue
		}
		workers := make([]string, 0, len(resp.ClusterNodes))
		for _, node := range resp.ClusterNodes {
			glog.V(0).Infof("S3 Vectors: discovered weed-vector at %s via master %s", node.Address, master)
			workers = append(workers, node.Address)
		}
		// A follower can answer with an empty list — workers register with
		// the leader, so keep looking rather than accept "none."
		if len(workers) > 0 {
			return workers
		}
	}
	return nil
}
