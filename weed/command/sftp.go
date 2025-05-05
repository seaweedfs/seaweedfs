package command

import (
	"context"
	"fmt"
	"net"
	"os"
	"runtime"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	filer_pb "github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/sftpd"
	stats_collect "github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var (
	sftpOptionsStandalone SftpOptions
)

// SftpOptions holds configuration options for the SFTP server.
type SftpOptions struct {
	filer               *string
	bindIp              *string
	port                *int
	sshPrivateKey       *string
	hostKeysFolder      *string
	authMethods         *string
	maxAuthTries        *int
	bannerMessage       *string
	loginGraceTime      *time.Duration
	clientAliveInterval *time.Duration
	clientAliveCountMax *int
	userStoreFile       *string
	dataCenter          *string
	metricsHttpPort     *int
	metricsHttpIp       *string
	localSocket         *string
}

// cmdSftp defines the SFTP command similar to the S3 command.
var cmdSftp = &Command{
	UsageLine: "sftp [-port=2022] [-filer=<ip:port>] [-sshPrivateKey=</path/to/private_key>]",
	Short:     "start an SFTP server that is backed by a SeaweedFS filer",
	Long: `Start an SFTP server that leverages the SeaweedFS filer service to handle file operations.

Instead of reading from or writing to a local filesystem, all file operations
are routed through the filer (filer_pb) gRPC API. This allows you to centralize
your file management in SeaweedFS.
	`,
}

func init() {
	// Register the command to avoid cyclic dependencies.
	cmdSftp.Run = runSftp

	sftpOptionsStandalone.filer = cmdSftp.Flag.String("filer", "localhost:8888", "filer server address (ip:port)")
	sftpOptionsStandalone.bindIp = cmdSftp.Flag.String("ip.bind", "0.0.0.0", "ip address to bind SFTP server")
	sftpOptionsStandalone.port = cmdSftp.Flag.Int("port", 2022, "SFTP server listen port")
	sftpOptionsStandalone.sshPrivateKey = cmdSftp.Flag.String("sshPrivateKey", "", "path to the SSH private key file for host authentication")
	sftpOptionsStandalone.hostKeysFolder = cmdSftp.Flag.String("hostKeysFolder", "", "path to folder containing SSH private key files for host authentication")
	sftpOptionsStandalone.authMethods = cmdSftp.Flag.String("authMethods", "password,publickey", "comma-separated list of allowed auth methods: password, publickey, keyboard-interactive")
	sftpOptionsStandalone.maxAuthTries = cmdSftp.Flag.Int("maxAuthTries", 6, "maximum number of authentication attempts per connection")
	sftpOptionsStandalone.bannerMessage = cmdSftp.Flag.String("bannerMessage", "SeaweedFS SFTP Server - Unauthorized access is prohibited", "message displayed before authentication")
	sftpOptionsStandalone.loginGraceTime = cmdSftp.Flag.Duration("loginGraceTime", 2*time.Minute, "timeout for authentication")
	sftpOptionsStandalone.clientAliveInterval = cmdSftp.Flag.Duration("clientAliveInterval", 5*time.Second, "interval for sending keep-alive messages")
	sftpOptionsStandalone.clientAliveCountMax = cmdSftp.Flag.Int("clientAliveCountMax", 3, "maximum number of missed keep-alive messages before disconnecting")
	sftpOptionsStandalone.userStoreFile = cmdSftp.Flag.String("userStoreFile", "", "path to JSON file containing user credentials and permissions")
	sftpOptionsStandalone.dataCenter = cmdSftp.Flag.String("dataCenter", "", "prefer to read and write to volumes in this data center")
	sftpOptionsStandalone.metricsHttpPort = cmdSftp.Flag.Int("metricsPort", 0, "Prometheus metrics listen port")
	sftpOptionsStandalone.metricsHttpIp = cmdSftp.Flag.String("metricsIp", "", "metrics listen ip. If empty, default to same as -ip.bind option.")
	sftpOptionsStandalone.localSocket = cmdSftp.Flag.String("localSocket", "", "default to /tmp/seaweedfs-sftp-<port>.sock")
}

// runSftp is the command entry point.
func runSftp(cmd *Command, args []string) bool {
	// Load security configuration as done in other SeaweedFS services.
	util.LoadSecurityConfiguration()

	// Configure metrics
	switch {
	case *sftpOptionsStandalone.metricsHttpIp != "":
		// nothing to do, use sftpOptionsStandalone.metricsHttpIp
	case *sftpOptionsStandalone.bindIp != "":
		*sftpOptionsStandalone.metricsHttpIp = *sftpOptionsStandalone.bindIp
	}
	go stats_collect.StartMetricsServer(*sftpOptionsStandalone.metricsHttpIp, *sftpOptionsStandalone.metricsHttpPort)

	return sftpOptionsStandalone.startSftpServer()
}

func (sftpOpt *SftpOptions) startSftpServer() bool {
	filerAddress := pb.ServerAddress(*sftpOpt.filer)
	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.client")

	// metrics read from the filer
	var metricsAddress string
	var metricsIntervalSec int
	var filerGroup string

	// Connect to the filer service and try to retrieve basic configuration.
	for {
		err := pb.WithGrpcFilerClient(false, 0, filerAddress, grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
			resp, err := client.GetFilerConfiguration(context.Background(), &filer_pb.GetFilerConfigurationRequest{})
			if err != nil {
				return fmt.Errorf("get filer %s configuration: %v", filerAddress, err)
			}
			metricsAddress, metricsIntervalSec = resp.MetricsAddress, int(resp.MetricsIntervalSec)
			filerGroup = resp.FilerGroup
			glog.V(0).Infof("SFTP read filer configuration, using filer at: %s", filerAddress)
			return nil
		})
		if err != nil {
			glog.V(0).Infof("Waiting to connect to filer %s grpc address %s...", *sftpOpt.filer, filerAddress.ToGrpcAddress())
			time.Sleep(time.Second)
		} else {
			glog.V(0).Infof("Connected to filer %s grpc address %s", *sftpOpt.filer, filerAddress.ToGrpcAddress())
			break
		}
	}

	go stats_collect.LoopPushingMetric("sftp", stats_collect.SourceName(uint32(*sftpOpt.port)), metricsAddress, metricsIntervalSec)

	// Parse auth methods
	var authMethods []string
	if *sftpOpt.authMethods != "" {
		authMethods = util.StringSplit(*sftpOpt.authMethods, ",")
	}

	// Create a new SFTP service instance with all options
	service := sftpd.NewSFTPService(&sftpd.SFTPServiceOptions{
		GrpcDialOption:      grpcDialOption,
		DataCenter:          *sftpOpt.dataCenter,
		FilerGroup:          filerGroup,
		Filer:               filerAddress,
		SshPrivateKey:       *sftpOpt.sshPrivateKey,
		HostKeysFolder:      *sftpOpt.hostKeysFolder,
		AuthMethods:         authMethods,
		MaxAuthTries:        *sftpOpt.maxAuthTries,
		BannerMessage:       *sftpOpt.bannerMessage,
		LoginGraceTime:      *sftpOpt.loginGraceTime,
		ClientAliveInterval: *sftpOpt.clientAliveInterval,
		ClientAliveCountMax: *sftpOpt.clientAliveCountMax,
		UserStoreFile:       *sftpOpt.userStoreFile,
	})

	// Set up Unix socket if on non-Windows platforms
	if runtime.GOOS != "windows" {
		localSocket := *sftpOpt.localSocket
		if localSocket == "" {
			localSocket = fmt.Sprintf("/tmp/seaweedfs-sftp-%d.sock", *sftpOpt.port)
		}
		if err := os.Remove(localSocket); err != nil && !os.IsNotExist(err) {
			glog.Fatalf("Failed to remove %s, error: %s", localSocket, err.Error())
		}
		go func() {
			// start on local unix socket
			sftpSocketListener, err := net.Listen("unix", localSocket)
			if err != nil {
				glog.Fatalf("Failed to listen on %s: %v", localSocket, err)
			}
			if err := service.Serve(sftpSocketListener); err != nil {
				glog.Fatalf("Failed to serve SFTP on socket %s: %v", localSocket, err)
			}
		}()
	}

	// Start the SFTP service on TCP
	listenAddress := fmt.Sprintf("%s:%d", *sftpOpt.bindIp, *sftpOpt.port)
	sftpListener, sftpLocalListener, err := util.NewIpAndLocalListeners(*sftpOpt.bindIp, *sftpOpt.port, time.Duration(10)*time.Second)
	if err != nil {
		glog.Fatalf("SFTP server listener on %s error: %v", listenAddress, err)
	}

	glog.V(0).Infof("Start Seaweed SFTP Server %s at %s", util.Version(), listenAddress)

	if sftpLocalListener != nil {
		go func() {
			if err := service.Serve(sftpLocalListener); err != nil {
				glog.Fatalf("SFTP Server failed to serve on local listener: %v", err)
			}
		}()
	}

	if err := service.Serve(sftpListener); err != nil {
		glog.Fatalf("SFTP Server failed to serve: %v", err)
	}

	return true
}
