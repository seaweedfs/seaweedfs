package command

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"google.golang.org/grpc/credentials/tls/certprovider"
	"google.golang.org/grpc/credentials/tls/certprovider/pemfile"
	"google.golang.org/grpc/reflection"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/s3_pb"
	"github.com/seaweedfs/seaweedfs/weed/s3api"
	"github.com/seaweedfs/seaweedfs/weed/s3api/s3err"
	"github.com/seaweedfs/seaweedfs/weed/security"
	stats_collect "github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/version"
)

var (
	s3StandaloneOptions S3Options
)

type S3Options struct {
	filer                     *string
	bindIp                    *string
	port                      *int
	portHttps                 *int
	portGrpc                  *int
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
	auditLogConfig            *string
	localFilerSocket          *string
	dataCenter                *string
	localSocket               *string
	certProvider              certprovider.Provider
	idleTimeout               *int
	concurrentUploadLimitMB   *int
	concurrentFileUploadLimit *int
	enableIam                 *bool
}

func init() {
	cmdS3.Run = runS3 // break init cycle
	s3StandaloneOptions.filer = cmdS3.Flag.String("filer", "localhost:8888", "comma-separated filer server addresses for high availability")
	s3StandaloneOptions.bindIp = cmdS3.Flag.String("ip.bind", "", "ip address to bind to. Default to localhost.")
	s3StandaloneOptions.port = cmdS3.Flag.Int("port", 8333, "s3 server http listen port")
	s3StandaloneOptions.portHttps = cmdS3.Flag.Int("port.https", 0, "s3 server https listen port")
	s3StandaloneOptions.portGrpc = cmdS3.Flag.Int("port.grpc", 0, "s3 server grpc listen port")
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
	s3StandaloneOptions.allowDeleteBucketNotEmpty = cmdS3.Flag.Bool("allowDeleteBucketNotEmpty", true, "allow recursive deleting all entries along with bucket")
	s3StandaloneOptions.localFilerSocket = cmdS3.Flag.String("localFilerSocket", "", "local filer socket path")
	s3StandaloneOptions.localSocket = cmdS3.Flag.String("localSocket", "", "default to /tmp/seaweedfs-s3-<port>.sock")
	s3StandaloneOptions.idleTimeout = cmdS3.Flag.Int("idleTimeout", 120, "connection idle seconds")
	s3StandaloneOptions.concurrentUploadLimitMB = cmdS3.Flag.Int("concurrentUploadLimitMB", 0, "limit total concurrent upload size, 0 means unlimited")
	s3StandaloneOptions.concurrentFileUploadLimit = cmdS3.Flag.Int("concurrentFileUploadLimit", 0, "limit number of concurrent file uploads, 0 means unlimited")
	s3StandaloneOptions.enableIam = cmdS3.Flag.Bool("iam", true, "enable embedded IAM API on the same port")
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

// GetCertificateWithUpdate Auto refreshing TSL certificate
func (s3opt *S3Options) GetCertificateWithUpdate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	certs, err := s3opt.certProvider.KeyMaterial(context.Background())
	if certs == nil {
		return nil, err
	}
	return &certs.Certs[0], err
}

func (s3opt *S3Options) startS3Server() bool {

	filerAddresses := pb.ServerAddresses(*s3opt.filer).ToAddresses()

	filerBucketsPath := "/buckets"
	filerGroup := ""
	var masterAddresses []pb.ServerAddress

	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.client")

	// metrics read from the filer
	var metricsAddress string
	var metricsIntervalSec int

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
			glog.V(0).Infof("S3 read filer buckets dir: %s", filerBucketsPath)
			if len(masterAddresses) > 0 {
				glog.V(0).Infof("S3 read master addresses for discovery: %v", masterAddresses)
			}
			return nil
		})
		if err != nil {
			glog.V(0).Infof("wait to connect to filers %v grpc address", filerAddresses)
			time.Sleep(time.Second)
		} else {
			glog.V(0).Infof("connected to filers %v", filerAddresses)
			break
		}
	}

	go stats_collect.LoopPushingMetric("s3", stats_collect.SourceName(uint32(*s3opt.port)), metricsAddress, metricsIntervalSec)

	router := mux.NewRouter().SkipClean(true)
	var localFilerSocket string
	if s3opt.localFilerSocket != nil {
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
		LocalFilerSocket:          localFilerSocket,
		DataCenter:                *s3opt.dataCenter,
		FilerGroup:                filerGroup,
		IamConfig:                 iamConfigPath, // Advanced IAM config (optional)
		ConcurrentUploadLimit:     int64(*s3opt.concurrentUploadLimitMB) * 1024 * 1024,
		ConcurrentFileUploadLimit: int64(*s3opt.concurrentFileUploadLimit),
		EnableIam:                 *s3opt.enableIam, // Embedded IAM API (enabled by default)
	})
	if s3ApiServer_err != nil {
		glog.Fatalf("S3 API Server startup error: %v", s3ApiServer_err)
	}

	if *s3opt.portGrpc == 0 {
		*s3opt.portGrpc = 10000 + *s3opt.port
	}
	if *s3opt.bindIp == "" {
		*s3opt.bindIp = "localhost"
	}

	if runtime.GOOS != "windows" {
		localSocket := *s3opt.localSocket
		if localSocket == "" {
			localSocket = fmt.Sprintf("/tmp/seaweedfs-s3-%d.sock", *s3opt.port)
		}
		if err := os.Remove(localSocket); err != nil && !os.IsNotExist(err) {
			glog.Fatalf("Failed to remove %s, error: %s", localSocket, err.Error())
		}
		go func() {
			// start on local unix socket
			s3SocketListener, err := net.Listen("unix", localSocket)
			if err != nil {
				glog.Fatalf("Failed to listen on %s: %v", localSocket, err)
			}
			newHttpServer(router, nil).Serve(s3SocketListener)
		}()
	}

	listenAddress := fmt.Sprintf("%s:%d", *s3opt.bindIp, *s3opt.port)
	s3ApiListener, s3ApiLocalListener, err := util.NewIpAndLocalListeners(
		*s3opt.bindIp, *s3opt.port, time.Duration(*s3opt.idleTimeout)*time.Second)
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
	grpcL, grpcLocalL, err := util.NewIpAndLocalListeners(*s3opt.bindIp, grpcPort, 0)
	if err != nil {
		glog.Fatalf("s3 failed to listen on grpc port %d: %v", grpcPort, err)
	}
	grpcS := pb.NewGrpcServer(security.LoadServerTLS(util.GetViper(), "grpc.s3"))
	s3_pb.RegisterSeaweedS3Server(grpcS, s3ApiServer)
	reflection.Register(grpcS)
	if grpcLocalL != nil {
		go grpcS.Serve(grpcLocalL)
	}
	go grpcS.Serve(grpcL)

	if *s3opt.tlsPrivateKey != "" {
		pemfileOptions := pemfile.Options{
			CertFile:        *s3opt.tlsCertificate,
			KeyFile:         *s3opt.tlsPrivateKey,
			RefreshDuration: security.CredRefreshingInterval,
		}
		if s3opt.certProvider, err = pemfile.NewProvider(pemfileOptions); err != nil {
			glog.Fatalf("pemfile.NewProvider(%v) failed: %v", pemfileOptions, err)
		}

		caCertPool := x509.NewCertPool()
		if *s3opt.tlsCACertificate != "" {
			// load CA certificate file and add it to list of client CAs
			caCertFile, err := ioutil.ReadFile(*s3opt.tlsCACertificate)
			if err != nil {
				glog.Fatalf("error reading CA certificate: %v", err)
			}
			caCertPool.AppendCertsFromPEM(caCertFile)
		}

		clientAuth := tls.NoClientCert
		if *s3opt.tlsVerifyClientCert {
			clientAuth = tls.RequireAndVerifyClientCert
		}

		tlsConfig := &tls.Config{
			GetCertificate: s3opt.GetCertificateWithUpdate,
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
				go func() {
					if err = newHttpServer(router, tlsConfig).ServeTLS(s3ApiLocalListener, "", ""); err != nil {
						glog.Fatalf("S3 API Server Fail to serve: %v", err)
					}
				}()
			}
			if err = newHttpServer(router, tlsConfig).ServeTLS(s3ApiListener, "", ""); err != nil {
				glog.Fatalf("S3 API Server Fail to serve: %v", err)
			}
		} else {
			glog.V(0).Infof("Start Seaweed S3 API Server %s at https port %d", version.Version(), *s3opt.portHttps)
			s3ApiListenerHttps, s3ApiLocalListenerHttps, _ := util.NewIpAndLocalListeners(
				*s3opt.bindIp, *s3opt.portHttps, time.Duration(*s3opt.idleTimeout)*time.Second)
			if s3ApiLocalListenerHttps != nil {
				go func() {
					if err = newHttpServer(router, tlsConfig).ServeTLS(s3ApiLocalListenerHttps, "", ""); err != nil {
						glog.Fatalf("S3 API Server Fail to serve: %v", err)
					}
				}()
			}
			go func() {
				if err = newHttpServer(router, tlsConfig).ServeTLS(s3ApiListenerHttps, "", ""); err != nil {
					glog.Fatalf("S3 API Server Fail to serve: %v", err)
				}
			}()
		}
	}
	if *s3opt.tlsPrivateKey == "" || *s3opt.portHttps > 0 {
		glog.V(0).Infof("Start Seaweed S3 API Server %s at http port %d", version.Version(), *s3opt.port)
		if s3ApiLocalListener != nil {
			go func() {
				if err = newHttpServer(router, nil).Serve(s3ApiLocalListener); err != nil {
					glog.Fatalf("S3 API Server Fail to serve: %v", err)
				}
			}()
		}
		if err = newHttpServer(router, nil).Serve(s3ApiListener); err != nil {
			glog.Fatalf("S3 API Server Fail to serve: %v", err)
		}
	}

	return true

}
