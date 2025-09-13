package command

import (
	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/gateway"
)

var (
	mqKafkaGatewayOptions mqKafkaGatewayOpts
)

type mqKafkaGatewayOpts struct {
	listen     *string
	masters    *string
	filerGroup *string
}

func init() {
	cmdMqKafkaGateway.Run = runMqKafkaGateway
	mqKafkaGatewayOptions.listen = cmdMqKafkaGateway.Flag.String("listen", ":9092", "Kafka gateway listen address")
	mqKafkaGatewayOptions.masters = cmdMqKafkaGateway.Flag.String("masters", "localhost:9333", "SeaweedFS master servers")
	mqKafkaGatewayOptions.filerGroup = cmdMqKafkaGateway.Flag.String("filerGroup", "", "filer group name")
}

var cmdMqKafkaGateway = &Command{
	UsageLine: "mq.kafka.gateway [-listen=:9092] [-masters=localhost:9333] [-filerGroup=]",
	Short:     "start a Kafka wire-protocol gateway for SeaweedMQ",
	Long: `Start a Kafka wire-protocol gateway translating Kafka client requests to SeaweedMQ.

Connects to SeaweedFS master servers to discover available brokers. Use -masters=<addresses> 
to specify comma-separated master locations.

This is experimental and currently supports a minimal subset for development.
`,
}

func runMqKafkaGateway(cmd *Command, args []string) bool {
	// Validate options - masters address is now required
	if *mqKafkaGatewayOptions.masters == "" {
		glog.Fatalf("SeaweedFS masters address is required (-masters)")
		return false
	}

	srv := gateway.NewServer(gateway.Options{
		Listen:     *mqKafkaGatewayOptions.listen,
		Masters:    *mqKafkaGatewayOptions.masters,
		FilerGroup: *mqKafkaGatewayOptions.filerGroup,
	})

	glog.V(0).Infof("Starting MQ Kafka Gateway on %s with SeaweedMQ brokers from masters (%s)", *mqKafkaGatewayOptions.listen, *mqKafkaGatewayOptions.masters)
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
