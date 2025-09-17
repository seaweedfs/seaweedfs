package command

import (
	"fmt"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/gateway"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var (
	mqKafkaGatewayOptions mqKafkaGatewayOpts
)

type mqKafkaGatewayOpts struct {
	ip         *string
	port       *int
	master     *string
	filerGroup *string
}

func init() {
	cmdMqKafkaGateway.Run = runMqKafkaGateway
	mqKafkaGatewayOptions.ip = cmdMqKafkaGateway.Flag.String("ip", util.DetectedHostAddress(), "Kafka gateway host address")
	mqKafkaGatewayOptions.port = cmdMqKafkaGateway.Flag.Int("port", 9092, "Kafka gateway listen port")
	mqKafkaGatewayOptions.master = cmdMqKafkaGateway.Flag.String("master", "localhost:9333", "comma-separated SeaweedFS master servers")
	mqKafkaGatewayOptions.filerGroup = cmdMqKafkaGateway.Flag.String("filerGroup", "", "filer group name")
}

var cmdMqKafkaGateway = &Command{
	UsageLine: "mq.kafka.gateway [-ip=<host>] [-port=9092] [-master=<master_servers>] [-filerGroup=<group>]",
	Short:     "start a Kafka wire-protocol gateway for SeaweedMQ",
	Long: `Start a Kafka wire-protocol gateway translating Kafka client requests to SeaweedMQ.

Connects to SeaweedFS master servers to discover available brokers. Use -master=<addresses> 
to specify comma-separated master locations.

Examples:
  weed mq.kafka.gateway -ip=gateway1 -port=9092 -master=master1:9333,master2:9333
  weed mq.kafka.gateway -port=9092 -master=localhost:9333

This is experimental and currently supports a minimal subset for development.
`,
}

func runMqKafkaGateway(cmd *Command, args []string) bool {
	// Validate options - master address is now required
	if *mqKafkaGatewayOptions.master == "" {
		glog.Fatalf("SeaweedFS master address is required (-master)")
		return false
	}

	// Construct listen address from ip and port
	listenAddr := fmt.Sprintf("%s:%d", *mqKafkaGatewayOptions.ip, *mqKafkaGatewayOptions.port)

	srv := gateway.NewServer(gateway.Options{
		Listen:     listenAddr,
		Masters:    *mqKafkaGatewayOptions.master,
		FilerGroup: *mqKafkaGatewayOptions.filerGroup,
	})

	glog.V(0).Infof("Starting MQ Kafka Gateway on %s with SeaweedMQ brokers from masters (%s)", listenAddr, *mqKafkaGatewayOptions.master)
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
