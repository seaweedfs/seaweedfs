package command

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc/reflection"

	"github.com/chrislusf/seaweedfs/weed/util/grace"

	"github.com/chrislusf/seaweedfs/weed/glog"
	"github.com/chrislusf/seaweedfs/weed/messaging/broker"
	"github.com/chrislusf/seaweedfs/weed/pb"
	"github.com/chrislusf/seaweedfs/weed/pb/filer_pb"
	"github.com/chrislusf/seaweedfs/weed/pb/messaging_pb"
	"github.com/chrislusf/seaweedfs/weed/security"
	"github.com/chrislusf/seaweedfs/weed/util"
)

var (
	messageBrokerStandaloneOptions MessageBrokerOptions
)

type MessageBrokerOptions struct {
	filer      *string
	ip         *string
	port       *int
	cpuprofile *string
	memprofile *string
}

func init() {
	cmdMsgBroker.Run = runMsgBroker // break init cycle
	messageBrokerStandaloneOptions.filer = cmdMsgBroker.Flag.String("filer", "localhost:8888", "filer server address")
	messageBrokerStandaloneOptions.ip = cmdMsgBroker.Flag.String("ip", util.DetectedHostAddress(), "broker host address")
	messageBrokerStandaloneOptions.port = cmdMsgBroker.Flag.Int("port", 17777, "broker gRPC listen port")
	messageBrokerStandaloneOptions.cpuprofile = cmdMsgBroker.Flag.String("cpuprofile", "", "cpu profile output file")
	messageBrokerStandaloneOptions.memprofile = cmdMsgBroker.Flag.String("memprofile", "", "memory profile output file")
}

var cmdMsgBroker = &Command{
	UsageLine: "msgBroker [-port=17777] [-filer=<ip:port>]",
	Short:     "start a message queue broker",
	Long: `start a message queue broker

	The broker can accept gRPC calls to write or read messages. The messages are stored via filer.
	The brokers are stateless. To scale up, just add more brokers.

`,
}

func runMsgBroker(cmd *Command, args []string) bool {

	util.LoadConfiguration("security", false)

	return messageBrokerStandaloneOptions.startQueueServer()

}

func (msgBrokerOpt *MessageBrokerOptions) startQueueServer() bool {

	grace.SetupProfiling(*messageBrokerStandaloneOptions.cpuprofile, *messageBrokerStandaloneOptions.memprofile)

	filerAddress := pb.ServerAddress(*msgBrokerOpt.filer)

	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.msg_broker")
	cipher := false

	for {
		err := pb.WithGrpcFilerClient(false, filerAddress, grpcDialOption, func(client filer_pb.SeaweedFilerClient) error {
			resp, err := client.GetFilerConfiguration(context.Background(), &filer_pb.GetFilerConfigurationRequest{})
			if err != nil {
				return fmt.Errorf("get filer %s configuration: %v", filerAddress, err)
			}
			cipher = resp.Cipher
			return nil
		})
		if err != nil {
			glog.V(0).Infof("wait to connect to filer %s grpc address %s", *msgBrokerOpt.filer, filerAddress.ToGrpcAddress())
			time.Sleep(time.Second)
		} else {
			glog.V(0).Infof("connected to filer %s grpc address %s", *msgBrokerOpt.filer, filerAddress.ToGrpcAddress())
			break
		}
	}

	qs, err := broker.NewMessageBroker(&broker.MessageBrokerOption{
		Filers:             []pb.ServerAddress{filerAddress},
		DefaultReplication: "",
		MaxMB:              0,
		Ip:                 *msgBrokerOpt.ip,
		Port:               *msgBrokerOpt.port,
		Cipher:             cipher,
	}, grpcDialOption)

	// start grpc listener
	grpcL, _, err := util.NewIpAndLocalListeners("", *msgBrokerOpt.port, 0)
	if err != nil {
		glog.Fatalf("failed to listen on grpc port %d: %v", *msgBrokerOpt.port, err)
	}
	grpcS := pb.NewGrpcServer(security.LoadServerTLS(util.GetViper(), "grpc.msg_broker"))
	messaging_pb.RegisterSeaweedMessagingServer(grpcS, qs)
	reflection.Register(grpcS)
	grpcS.Serve(grpcL)

	return true

}
