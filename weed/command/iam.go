package command

import (
	"context"
	"fmt"
	"net/http"

	"time"

	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/iamapi"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
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
	iamStandaloneOptions.filer = cmdIam.Flag.String("filer", "localhost:8888", "filer server address")
	iamStandaloneOptions.masters = cmdIam.Flag.String("master", "localhost:9333", "comma-separated master servers")
	iamStandaloneOptions.ip = cmdIam.Flag.String("ip", util.DetectedHostAddress(), "iam server http listen ip address")
	iamStandaloneOptions.port = cmdIam.Flag.Int("port", 8111, "iam server http listen port")
}

var cmdIam = &Command{
	UsageLine: "iam [-port=8111] [-filer=<ip:port>] [-master=<ip:port>,<ip:port>]",
	Short:     "start a iam API compatible server",
	Long:      "start a iam API compatible server.",
}

func runIam(cmd *Command, args []string) bool {
	return iamStandaloneOptions.startIamServer()
}

func (iamopt *IamOptions) startIamServer() bool {
	filerAddress := pb.ServerAddress(*iamopt.filer)

	util.LoadSecurityConfiguration()
	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.client")
	for {
		err := pb.WithGrpcFilerClient(false, 0, filerAddress, grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
			resp, err := client.GetFilerConfiguration(context.Background(), &filer_pb.GetFilerConfigurationRequest{})
			if err != nil {
				return fmt.Errorf("get filer %s configuration: %v", filerAddress, err)
			}
			glog.V(0).Infof("IAM read filer configuration: %s", resp)
			return nil
		})
		if err != nil {
			glog.V(0).Infof("wait to connect to filer %s grpc address %s", *iamopt.filer, filerAddress.ToGrpcAddress())
			time.Sleep(time.Second)
		} else {
			glog.V(0).Infof("connected to filer %s grpc address %s", *iamopt.filer, filerAddress.ToGrpcAddress())
			break
		}
	}

	masters := pb.ServerAddresses(*iamopt.masters).ToAddressMap()
	router := mux.NewRouter().SkipClean(true)
	_, iamApiServer_err := iamapi.NewIamApiServer(router, &iamapi.IamServerOption{
		Masters:        masters,
		Filer:          filerAddress,
		Port:           *iamopt.port,
		GrpcDialOption: grpcDialOption,
	})
	glog.V(0).Info("NewIamApiServer created")
	if iamApiServer_err != nil {
		glog.Fatalf("IAM API Server startup error: %v", iamApiServer_err)
	}

	httpS := &http.Server{Handler: router}

	listenAddress := fmt.Sprintf(":%d", *iamopt.port)
	iamApiListener, iamApiLocalListener, err := util.NewIpAndLocalListeners(*iamopt.ip, *iamopt.port, time.Duration(10)*time.Second)
	if err != nil {
		glog.Fatalf("IAM API Server listener on %s error: %v", listenAddress, err)
	}

	glog.V(0).Infof("Start Seaweed IAM API Server %s at http port %d", util.Version(), *iamopt.port)
	if iamApiLocalListener != nil {
		go func() {
			if err = httpS.Serve(iamApiLocalListener); err != nil {
				glog.Errorf("IAM API Server Fail to serve: %v", err)
			}
		}()
	}
	if err = httpS.Serve(iamApiListener); err != nil {
		glog.Fatalf("IAM API Server Fail to serve: %v", err)
	}

	return true
}
