package command

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/gateway"
)

var (
	mqKafkaGatewayOptions mqKafkaGatewayOpts
)

type mqKafkaGatewayOpts struct {
	listen       *string
	agentAddress *string
}

func init() {
	cmdMqKafkaGateway.Run = runMqKafkaGateway
	mqKafkaGatewayOptions.listen = cmdMqKafkaGateway.Flag.String("listen", ":9092", "Kafka gateway listen address")
	mqKafkaGatewayOptions.agentAddress = cmdMqKafkaGateway.Flag.String("agent", "localhost:17777", "SeaweedMQ Agent address (required)")
}

var cmdMqKafkaGateway = &Command{
	UsageLine: "mq.kafka.gateway [-listen=:9092] [-agent=localhost:17777]",
	Short:     "start a Kafka wire-protocol gateway for SeaweedMQ",
	Long: `Start a Kafka wire-protocol gateway translating Kafka client requests to SeaweedMQ.

Requires a running SeaweedMQ Agent. Use -agent=<address> to specify the agent location.

This is experimental and currently supports a minimal subset for development.
`,
}

func runMqKafkaGateway(cmd *Command, args []string) bool {
	// Validate options - agent address is now required
	if *mqKafkaGatewayOptions.agentAddress == "" {
		glog.Fatalf("SeaweedMQ Agent address is required (-agent)")
		return false
	}

	srv := gateway.NewServer(gateway.Options{
		Listen:       *mqKafkaGatewayOptions.listen,
		AgentAddress: *mqKafkaGatewayOptions.agentAddress,
	})

	glog.V(0).Infof("Starting MQ Kafka Gateway on %s with SeaweedMQ backend (%s)", *mqKafkaGatewayOptions.listen, *mqKafkaGatewayOptions.agentAddress)
	if err := srv.Start(); err != nil {
		glog.Fatalf("mq kafka gateway start: %v", err)
		return false
	}

	// Set up graceful shutdown
	defer func() {
		glog.V(0).Infof("Shutting down MQ Kafka Gateway...")
		if err := srv.Close(); err != nil {
			glog.Errorf("mq kafka gateway close: %v", err)
		}
	}()

	// Serve blocks until closed
	if err := srv.Wait(); err != nil {
		glog.Errorf("mq kafka gateway wait: %v", err)
		return false
	}
	return true
}
