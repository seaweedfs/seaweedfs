package command

import (
	"github.com/seaweedfs/seaweedfs/weed/mq/agent"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_agent_pb"
	"google.golang.org/grpc"
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
	brokers           []pb.ServerAddress
	brokersString     *string
	filerGroup        *string
	ip                *string
	ipBind            *string
	listenOnLocalhost *bool
	port              *int
}

func init() {
	cmdMqAgent.Run = runMqAgent // break init cycle
	mqAgentOptions.brokersString = cmdMqAgent.Flag.String("broker", "localhost:17777", "comma-separated message queue brokers")
	mqAgentOptions.ip = cmdMqAgent.Flag.String("ip", "", "message queue agent host address")
	mqAgentOptions.ipBind = cmdMqAgent.Flag.String("ip.bind", "", "ip address to bind to. If empty, default to same as -ip option.")
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

	// The loopback companion listener is only kept while -ip.bind is unset and
	// the bind address falls back to -ip. An explicit -ip.bind pins the port to
	// exactly that address.
	bindIp := *mqAgentOpt.ip
	listenOnLocalhost := mqAgentOpt.ipBind == nil || *mqAgentOpt.ipBind == ""
	if mqAgentOpt.ipBind != nil && *mqAgentOpt.ipBind != "" {
		bindIp = *mqAgentOpt.ipBind
	}
	if mqAgentOpt.listenOnLocalhost != nil {
		listenOnLocalhost = *mqAgentOpt.listenOnLocalhost
	}

	// start grpc listener
	grpcL, localL, err := util.NewIpAndLocalListeners(bindIp, *mqAgentOpt.port, 0, listenOnLocalhost)
	if err != nil {
		glog.Fatalf("failed to listen on grpc port %d: %v", *mqAgentOpt.port, err)
	}

	// Create main gRPC server
	grpcS := pb.NewGrpcServer()
	mq_agent_pb.RegisterSeaweedMessagingAgentServer(grpcS, agentServer)
	reflection.Register(grpcS)

	// Start localhost listener if available
	if localL != nil {
		localGrpcS := pb.NewGrpcServer()
		mq_agent_pb.RegisterSeaweedMessagingAgentServer(localGrpcS, agentServer)
		reflection.Register(localGrpcS)
		go func() {
			glog.V(0).Infof("MQ Agent listening on localhost:%d", *mqAgentOpt.port)
			if err := localGrpcS.Serve(localL); err != nil {
				glog.Errorf("MQ Agent localhost listener error: %v", err)
			}
		}()
	}

	glog.Infof("Start Seaweed Message Queue Agent on %s:%d", bindIp, *mqAgentOpt.port)
	if err := grpcS.Serve(grpcL); err != nil && err != grpc.ErrServerStopped {
		glog.Errorf("MQ Agent failed to start: %v", err)
	}

	return true

}
