package command

import (
	"github.com/seaweedfs/seaweedfs/weed/mq/agent"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_agent_pb"
	"google.golang.org/grpc/reflection"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/security"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var (
	mqAgentOptions MessageQueueAgentOptions
)

type MessageQueueAgentOptions struct {
	brokers       []pb.ServerAddress
	brokersString *string
	filerGroup    *string
	ip            *string
	port          *int
}

func init() {
	cmdMqAgent.Run = runMqAgent // break init cycle
	mqAgentOptions.brokersString = cmdMqAgent.Flag.String("broker", "localhost:17777", "comma-separated message queue brokers")
	mqAgentOptions.ip = cmdMqAgent.Flag.String("ip", "", "message queue agent host address")
	mqAgentOptions.port = cmdMqAgent.Flag.Int("port", 16777, "message queue agent gRPC server port")
}

var cmdMqAgent = &Command{
	UsageLine: "mq.agent [-port=16777] [-broker=<ip:port>]",
	Short:     "<WIP> start a message queue agent",
	Long: `start a message queue agent

	The agent runs on local server to accept gRPC calls to write or read messages. 
	The messages are sent to message queue brokers.

`,
}

func runMqAgent(cmd *Command, args []string) bool {

	util.LoadSecurityConfiguration()

	mqAgentOptions.brokers = pb.ServerAddresses(*mqAgentOptions.brokersString).ToAddresses()

	return mqAgentOptions.startQueueAgent()

}

func (mqAgentOpt *MessageQueueAgentOptions) startQueueAgent() bool {

	grpcDialOption := security.LoadClientTLS(util.GetViper(), "grpc.msg_agent")

	agentServer := agent.NewMessageQueueAgent(&agent.MessageQueueAgentOptions{
		SeedBrokers: mqAgentOpt.brokers,
	}, grpcDialOption)

	// start grpc listener
	grpcL, _, err := util.NewIpAndLocalListeners(*mqAgentOpt.ip, *mqAgentOpt.port, 0)
	if err != nil {
		glog.Fatalf("failed to listen on grpc port %d: %v", *mqAgentOpt.port, err)
	}
	glog.Infof("Start Seaweed Message Queue Agent on %s:%d", *mqAgentOpt.ip, *mqAgentOpt.port)
	grpcS := pb.NewGrpcServer()
	mq_agent_pb.RegisterSeaweedMessagingAgentServer(grpcS, agentServer)
	reflection.Register(grpcS)
	grpcS.Serve(grpcL)

	return true

}
