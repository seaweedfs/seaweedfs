package command

import (
	"context"
	"fmt"
	"net/http"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/iamapi"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/gorilla/mux"
	"time"
)

var (
	iamStandaloneOptions IamOptions
)

type IamOptions struct {
	filer   *string
	masters *string
	port    *int
}

func init() {
	cmdIam.Run = runIam // break init cycle
	iamStandaloneOptions.filer = cmdIam.Flag.String("filer", "localhost:8888", "filer server address")
	iamStandaloneOptions.masters = cmdIam.Flag.String("master", "localhost:9333", "comma-separated master servers")
	iamStandaloneOptions.port = cmdIam.Flag.Int("port", 8111, "iam server http listen port")
}

var cmdIam = &Command{
	UsageLine: "iam [-port=8111] [-filer=<ip:port>] [-masters=<ip:port>,<ip:port>]",
	Short:     "start a iam API compatible server",
	Long:      "start a iam API compatible server.",
}

func runIam(cmd *Command, args []string) bool {
	return iamStandaloneOptions.startIamServer()
}

func (iamopt *IamOptions) startIamServer() bool {
	filerGrpcAddress, err := pb.ParseServerToGrpcAddress(*iamopt.filer)
	if err != nil {
		glog.Fatal(err)
		return false
	}

	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.client")
	for {
		err = pb.WithGrpcFilerClient(filerGrpcAddress, grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
			resp, err := client.GetFilerConfiguration(context.Background(), &filer_pb.GetFilerConfigurationRequest{})
			if err != nil {
				return fmt.Errorf("get filer %s configuration: %v", filerGrpcAddress, err)
			}
			glog.V(0).Infof("IAM read filer configuration: %s", resp)
			return nil
		})
		if err != nil {
			glog.V(0).Infof("wait to connect to filer %s grpc address %s", *iamopt.filer, filerGrpcAddress)
			time.Sleep(time.Second)
		} else {
			glog.V(0).Infof("connected to filer %s grpc address %s", *iamopt.filer, filerGrpcAddress)
			break
		}
	}

	router := mux.NewRouter().SkipClean(true)
	_, iamApiServer_err := iamapi.NewIamApiServer(router, &iamapi.IamServerOption{
		Filer:            *iamopt.filer,
		Port:             *iamopt.port,
		FilerGrpcAddress: filerGrpcAddress,
		GrpcDialOption:   grpcDialOption,
	})
	glog.V(0).Info("NewIamApiServer created")
	if iamApiServer_err != nil {
		glog.Fatalf("IAM API Server startup error: %v", iamApiServer_err)
	}

	httpS := &http.Server{Handler: router}

	listenAddress := fmt.Sprintf(":%d", *iamopt.port)
	iamApiListener, err := util.NewListener(listenAddress, time.Duration(10)*time.Second)
	if err != nil {
		glog.Fatalf("IAM API Server listener on %s error: %v", listenAddress, err)
	}

	glog.V(0).Infof("Start Seaweed IAM API Server %s at http port %d", util.Version(), *iamopt.port)
	if err = httpS.Serve(iamApiListener); err != nil {
		glog.Fatalf("IAM API Server Fail to serve: %v", err)
	}

	return true
}
