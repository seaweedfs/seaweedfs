package command

import (
	"google.golang.org/grpc/reflection"

	"github.com/seaweedfs/seaweedfs/weed/util/grace"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/broker"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var (
	mqBrokerStandaloneOptions MessageQueueBrokerOptions
)

type MessageQueueBrokerOptions struct {
	masters       map[string]pb.ServerAddress
	mastersString *string
	filerGroup    *string
	ip            *string
	port          *int
	dataCenter    *string
	rack          *string
	cpuprofile    *string
	memprofile    *string
}

func init() {
	cmdMqBroker.Run = runMqBroker // break init cycle
	mqBrokerStandaloneOptions.mastersString = cmdMqBroker.Flag.String("master", "localhost:9333", "comma-separated master servers")
	mqBrokerStandaloneOptions.filerGroup = cmdMqBroker.Flag.String("filerGroup", "", "share metadata with other filers in the same filerGroup")
	mqBrokerStandaloneOptions.ip = cmdMqBroker.Flag.String("ip", util.DetectedHostAddress(), "broker host address")
	mqBrokerStandaloneOptions.port = cmdMqBroker.Flag.Int("port", 17777, "broker gRPC listen port")
	mqBrokerStandaloneOptions.dataCenter = cmdMqBroker.Flag.String("dataCenter", "", "prefer to read and write to volumes in this data center")
	mqBrokerStandaloneOptions.rack = cmdMqBroker.Flag.String("rack", "", "prefer to write to volumes in this rack")
	mqBrokerStandaloneOptions.cpuprofile = cmdMqBroker.Flag.String("cpuprofile", "", "cpu profile output file")
	mqBrokerStandaloneOptions.memprofile = cmdMqBroker.Flag.String("memprofile", "", "memory profile output file")
}

var cmdMqBroker = &Command{
	UsageLine: "mq.broker [-port=17777] [-master=<ip:port>]",
	Short:     "<WIP> start a message queue broker",
	Long: `start a message queue broker

	The broker can accept gRPC calls to write or read messages. The messages are stored via filer.
	The brokers are stateless. To scale up, just add more brokers.

`,
}

func runMqBroker(cmd *Command, args []string) bool {

	util.LoadSecurityConfiguration()

	mqBrokerStandaloneOptions.masters = pb.ServerAddresses(*mqBrokerStandaloneOptions.mastersString).ToAddressMap()

	return mqBrokerStandaloneOptions.startQueueServer()

}

func (mqBrokerOpt *MessageQueueBrokerOptions) startQueueServer() bool {

	grace.SetupProfiling(*mqBrokerStandaloneOptions.cpuprofile, *mqBrokerStandaloneOptions.memprofile)

	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.msg_broker")

	qs, err := broker.NewMessageBroker(&broker.MessageQueueBrokerOption{
		Masters:            mqBrokerOpt.masters,
		FilerGroup:         *mqBrokerOpt.filerGroup,
		DataCenter:         *mqBrokerOpt.dataCenter,
		Rack:               *mqBrokerOpt.rack,
		DefaultReplication: "",
		MaxMB:              0,
		Ip:                 *mqBrokerOpt.ip,
		Port:               *mqBrokerOpt.port,
	}, grpcDialOption)
	if err != nil {
		glog.Fatalf("failed to create new message broker for queue server: %v", err)
	}

	// start grpc listener
	grpcL, _, err := util.NewIpAndLocalListeners("", *mqBrokerOpt.port, 0)
	if err != nil {
		glog.Fatalf("failed to listen on grpc port %d: %v", *mqBrokerOpt.port, err)
	}
	grpcS := pb.NewGrpcServer(security.LoadServerTLS(util.GetViper(), "grpc.msg_broker"))
	mq_pb.RegisterSeaweedMessagingServer(grpcS, qs)
	reflection.Register(grpcS)
	grpcS.Serve(grpcL)

	return true

}
