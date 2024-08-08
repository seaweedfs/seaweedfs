package command

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/gorilla/mux"
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	weed_server "github.com/seaweedfs/seaweedfs/weed/server"
	"github.com/seaweedfs/seaweedfs/weed/util"
	"google.golang.org/grpc/reflection"
)

var (
	mf MasterOptions
)

func init() {
	cmdMasterFollower.Run = runMasterFollower // break init cycle
	mf.port = cmdMasterFollower.Flag.Int("port", 9334, "http listen port")
	mf.portGrpc = cmdMasterFollower.Flag.Int("port.grpc", 0, "grpc listen port")
	mf.ipBind = cmdMasterFollower.Flag.String("ip.bind", "", "ip address to bind to. Default to localhost.")
	mf.peers = cmdMasterFollower.Flag.String("masters", "localhost:9333", "all master nodes in comma separated ip:port list, example: 127.0.0.1:9093,127.0.0.1:9094,127.0.0.1:9095")

	mf.ip = aws.String(util.DetectedHostAddress())
	mf.metaFolder = aws.String("")
	mf.volumeSizeLimitMB = nil
	mf.volumePreallocate = nil
	mf.defaultReplication = nil
	mf.garbageThreshold = aws.Float64(0.1)
	mf.whiteList = nil
	mf.disableHttp = aws.Bool(false)
	mf.metricsAddress = aws.String("")
	mf.metricsIntervalSec = aws.Int(0)
	mf.raftResumeState = aws.Bool(false)
}

var cmdMasterFollower = &Command{
	UsageLine: "master.follower -port=9333 -masters=<master1Host>:<master1Port>",
	Short:     "start a master follower",
	Long: `start a master follower to provide volume=>location mapping service

	The master follower does not participate in master election.
	It just follow the existing masters, and listen for any volume location changes.

	In most cases, the master follower is not needed. In big data centers with thousands of volume
	servers. In theory, the master may have trouble to keep up with the write requests and read requests.

	The master follower can relieve the master from read requests, which only needs to
	lookup a fileId or volumeId.

	The master follower currently can handle fileId lookup requests:
		/dir/lookup?volumeId=4
		/dir/lookup?fileId=4,49c50924569199
	And gRPC API
		rpc LookupVolume (LookupVolumeRequest) returns (LookupVolumeResponse) {}

	This master follower is stateless and can run from any place.

  `,
}

func runMasterFollower(cmd *Command, args []string) bool {

	util.LoadSecurityConfiguration()
	util.LoadConfiguration("master", false)

	if *mf.portGrpc == 0 {
		*mf.portGrpc = 10000 + *mf.port
	}

	startMasterFollower(mf)

	return true
}

func startMasterFollower(masterOptions MasterOptions) {

	// collect settings from main masters
	masters := pb.ServerAddresses(*mf.peers).ToAddressMap()

	var err error
	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.master")
	for i := 0; i < 10; i++ {
		err = pb.WithOneOfGrpcMasterClients(false, masters, grpcDialOption, func(client master_pb.SeaweedClient) error {
			resp, err := client.GetMasterConfiguration(context.Background(), &master_pb.GetMasterConfigurationRequest{})
			if err != nil {
				return fmt.Errorf("get master grpc address %v configuration: %v", masters, err)
			}
			masterOptions.defaultReplication = &resp.DefaultReplication
			masterOptions.volumeSizeLimitMB = aws.Uint(uint(resp.VolumeSizeLimitMB))
			masterOptions.volumePreallocate = &resp.VolumePreallocate
			return nil
		})
		if err != nil {
			glog.V(0).Infof("failed to talk to filer %v: %v", masters, err)
			glog.V(0).Infof("wait for %d seconds ...", i+1)
			time.Sleep(time.Duration(i+1) * time.Second)
		}
	}
	if err != nil {
		glog.Errorf("failed to talk to filer %v: %v", masters, err)
		return
	}

	option := masterOptions.toMasterOption(nil)
	option.IsFollower = true

	if *masterOptions.ipBind == "" {
		*masterOptions.ipBind = *masterOptions.ip
	}

	r := mux.NewRouter()
	ms := weed_server.NewMasterServer(r, option, masters)
	listeningAddress := util.JoinHostPort(*masterOptions.ipBind, *masterOptions.port)
	glog.V(0).Infof("Start Seaweed Master %s at %s", util.Version(), listeningAddress)
	masterListener, masterLocalListener, e := util.NewIpAndLocalListeners(*masterOptions.ipBind, *masterOptions.port, 0)
	if e != nil {
		glog.Fatalf("Master startup error: %v", e)
	}

	// starting grpc server
	grpcPort := *masterOptions.portGrpc
	grpcL, grpcLocalL, err := util.NewIpAndLocalListeners(*masterOptions.ipBind, grpcPort, 0)
	if err != nil {
		glog.Fatalf("master failed to listen on grpc port %d: %v", grpcPort, err)
	}
	grpcS := pb.NewGrpcServer(security.LoadServerTLS(util.GetViper(), "grpc.master"))
	master_pb.RegisterSeaweedServer(grpcS, ms)
	reflection.Register(grpcS)
	glog.V(0).Infof("Start Seaweed Master %s grpc server at %s:%d", util.Version(), *masterOptions.ip, grpcPort)
	if grpcLocalL != nil {
		go grpcS.Serve(grpcLocalL)
	}
	go grpcS.Serve(grpcL)

	go ms.MasterClient.KeepConnectedToMaster(context.Background())

	// start http server
	httpS := &http.Server{Handler: r}
	if masterLocalListener != nil {
		go httpS.Serve(masterLocalListener)
	}
	go httpS.Serve(masterListener)

	select {}
}
