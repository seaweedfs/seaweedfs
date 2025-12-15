package command

import (
	"context"
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/util/version"

	"time"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/iamapi"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"github.com/seaweedfs/seaweedfs/weed/util/grace"

	// Import credential stores to register them
	_ "github.com/seaweedfs/seaweedfs/weed/credential/filer_etc"
	_ "github.com/seaweedfs/seaweedfs/weed/credential/memory"
	_ "github.com/seaweedfs/seaweedfs/weed/credential/postgres"
)

var (
	iamStandaloneOptions IamOptions
)

type IamOptions struct {
	filer   *string
	masters *string
	ip      *string
	port    *int
}

func init() {
	cmdIam.Run = runIam // break init cycle
	iamStandaloneOptions.filer = cmdIam.Flag.String("filer", "localhost:8888", "comma-separated filer server addresses for high availability")
	iamStandaloneOptions.masters = cmdIam.Flag.String("master", "localhost:9333", "comma-separated master servers")
	iamStandaloneOptions.ip = cmdIam.Flag.String("ip", util.DetectedHostAddress(), "iam server http listen ip address")
	iamStandaloneOptions.port = cmdIam.Flag.Int("port", 8111, "iam server http listen port")
}

var cmdIam = &Command{
	UsageLine: "iam [-port=8111] [-filer=<ip:port>[,<ip:port>]...] [-master=<ip:port>,<ip:port>]",
	Short:     "[DEPRECATED] start a standalone iam API compatible server",
	Long: `[DEPRECATED] start a standalone iam API compatible server.

	DEPRECATION NOTICE:
	The standalone 'weed iam' command is deprecated and will be removed in a future release.
	
	The IAM API is now embedded in the S3 server by default. Simply use 'weed s3' instead,
	which provides both S3 and IAM APIs on the same port (enabled by default with -iam=true).
	
	This simplifies deployment by running a single server instead of two separate servers,
	following the pattern used by MinIO and Ceph RGW.
	
	To use the embedded IAM API:
	  weed s3 -port=8333          # IAM API is available on the same port
	
	To disable the embedded IAM API (if you prefer the old behavior):
	  weed s3 -iam=false          # Run S3 without IAM
	  weed iam -port=8111         # Run IAM separately (deprecated)

	Multiple filer addresses can be specified for high availability, separated by commas.`,
}

func runIam(cmd *Command, args []string) bool {
	glog.Warningf("================================================================================")
	glog.Warningf("DEPRECATION WARNING: 'weed iam' is deprecated and will be removed in a future release.")
	glog.Warningf("The IAM API is now embedded in 'weed s3' by default (use -iam=true, which is the default).")
	glog.Warningf("Please migrate to using 'weed s3' which provides both S3 and IAM APIs on the same port.")
	glog.Warningf("================================================================================")
	return iamStandaloneOptions.startIamServer()
}

func (iamopt *IamOptions) startIamServer() bool {
	filerAddresses := pb.ServerAddresses(*iamopt.filer).ToAddresses()

	util.LoadSecurityConfiguration()
	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.client")
	for {
		err := pb.WithOneOfGrpcFilerClients(false, filerAddresses, grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
			resp, err := client.GetFilerConfiguration(context.Background(), &filer_pb.GetFilerConfigurationRequest{})
			if err != nil {
				return fmt.Errorf("get filer configuration: %v", err)
			}
			glog.V(0).Infof("IAM read filer configuration: %s", resp)
			return nil
		})
		if err != nil {
			glog.V(0).Infof("wait to connect to filers %v", filerAddresses)
			time.Sleep(time.Second)
		} else {
			glog.V(0).Infof("connected to filers %v", filerAddresses)
			break
		}
	}

	masters := pb.ServerAddresses(*iamopt.masters).ToAddressMap()
	router := mux.NewRouter().SkipClean(true)
	iamApiServer, iamApiServer_err := iamapi.NewIamApiServer(router, &iamapi.IamServerOption{
		Masters:        masters,
		Filers:         filerAddresses,
		Port:           *iamopt.port,
		GrpcDialOption: grpcDialOption,
	})
	glog.V(0).Info("NewIamApiServer created")
	if iamApiServer_err != nil {
		glog.Fatalf("IAM API Server startup error: %v", iamApiServer_err)
	}

	// Register shutdown handler to prevent goroutine leak
	grace.OnInterrupt(func() {
		iamApiServer.Shutdown()
	})

	listenAddress := fmt.Sprintf(":%d", *iamopt.port)
	iamApiListener, iamApiLocalListener, err := util.NewIpAndLocalListeners(*iamopt.ip, *iamopt.port, time.Duration(10)*time.Second)
	if err != nil {
		glog.Fatalf("IAM API Server listener on %s error: %v", listenAddress, err)
	}

	glog.V(0).Infof("Start Seaweed IAM API Server %s at http port %d", version.Version(), *iamopt.port)
	if iamApiLocalListener != nil {
		go func() {
			if err = newHttpServer(router, nil).Serve(iamApiLocalListener); err != nil {
				glog.Errorf("IAM API Server Fail to serve: %v", err)
			}
		}()
	}
	if err = newHttpServer(router, nil).Serve(iamApiListener); err != nil {
		glog.Fatalf("IAM API Server Fail to serve: %v", err)
	}

	return true
}
