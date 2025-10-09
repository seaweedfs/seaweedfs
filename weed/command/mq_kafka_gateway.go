package command

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"

	"github.com/seaweedfs/seaweedfs/weed/glog"
	"github.com/seaweedfs/seaweedfs/weed/mq/kafka/gateway"
	"github.com/seaweedfs/seaweedfs/weed/util"
)

var (
	mqKafkaGatewayOptions mqKafkaGatewayOpts
)

type mqKafkaGatewayOpts struct {
	ip                *string
	ipBind            *string
	port              *int
	pprofPort         *int
	master            *string
	filerGroup        *string
	schemaRegistryURL *string
	defaultPartitions *int
}

func init() {
	cmdMqKafkaGateway.Run = runMqKafkaGateway
	mqKafkaGatewayOptions.ip = cmdMqKafkaGateway.Flag.String("ip", util.DetectedHostAddress(), "Kafka gateway advertised host address")
	mqKafkaGatewayOptions.ipBind = cmdMqKafkaGateway.Flag.String("ip.bind", "", "Kafka gateway bind address (default: same as -ip)")
	mqKafkaGatewayOptions.port = cmdMqKafkaGateway.Flag.Int("port", 9092, "Kafka gateway listen port")
	mqKafkaGatewayOptions.pprofPort = cmdMqKafkaGateway.Flag.Int("port.pprof", 0, "HTTP profiling port (0 to disable)")
	mqKafkaGatewayOptions.master = cmdMqKafkaGateway.Flag.String("master", "localhost:9333", "comma-separated SeaweedFS master servers")
	mqKafkaGatewayOptions.filerGroup = cmdMqKafkaGateway.Flag.String("filerGroup", "", "filer group name")
	mqKafkaGatewayOptions.schemaRegistryURL = cmdMqKafkaGateway.Flag.String("schema-registry-url", "", "Schema Registry URL (required for schema management)")
	mqKafkaGatewayOptions.defaultPartitions = cmdMqKafkaGateway.Flag.Int("default-partitions", 4, "Default number of partitions for auto-created topics")
}

var cmdMqKafkaGateway = &Command{
	UsageLine: "mq.kafka.gateway [-ip=<host>] [-ip.bind=<bind_addr>] [-port=9092] [-master=<master_servers>] [-filerGroup=<group>] [-default-partitions=4] -schema-registry-url=<url>",
	Short:     "start a Kafka wire-protocol gateway for SeaweedMQ with schema management",
	Long: `Start a Kafka wire-protocol gateway translating Kafka client requests to SeaweedMQ.

Connects to SeaweedFS master servers to discover available brokers and integrates with
Schema Registry for schema-aware topic management.

Options:
  -ip                  Advertised host address that clients should connect to (default: auto-detected)
  -ip.bind             Bind address for the gateway to listen on (default: same as -ip)
                       Use 0.0.0.0 to bind to all interfaces while advertising specific IP
  -port                Listen port (default: 9092)
  -default-partitions  Default number of partitions for auto-created topics (default: 4)
  -schema-registry-url Schema Registry URL (REQUIRED for schema management)

Examples:
  weed mq.kafka.gateway -port=9092 -master=localhost:9333 -schema-registry-url=http://localhost:8081
  weed mq.kafka.gateway -ip=gateway1 -port=9092 -master=master1:9333,master2:9333 -schema-registry-url=http://schema-registry:8081
  weed mq.kafka.gateway -ip=external.host.com -ip.bind=0.0.0.0 -master=localhost:9333 -schema-registry-url=http://schema-registry:8081

This is experimental and currently supports a minimal subset for development.
`,
}

func runMqKafkaGateway(cmd *Command, args []string) bool {
	// Validate required options
	if *mqKafkaGatewayOptions.master == "" {
		glog.Fatalf("SeaweedFS master address is required (-master)")
		return false
	}

	// Schema Registry URL is required for schema management
	if *mqKafkaGatewayOptions.schemaRegistryURL == "" {
		glog.Fatalf("Schema Registry URL is required (-schema-registry-url)")
		return false
	}

	// Determine bind address - default to advertised IP if not specified
	bindIP := *mqKafkaGatewayOptions.ipBind
	if bindIP == "" {
		bindIP = *mqKafkaGatewayOptions.ip
	}

	// Construct listen address from bind IP and port
	listenAddr := fmt.Sprintf("%s:%d", bindIP, *mqKafkaGatewayOptions.port)

	// Set advertised host for Kafka protocol handler
	if err := os.Setenv("KAFKA_ADVERTISED_HOST", *mqKafkaGatewayOptions.ip); err != nil {
		glog.Warningf("Failed to set KAFKA_ADVERTISED_HOST environment variable: %v", err)
	}

	srv := gateway.NewServer(gateway.Options{
		Listen:            listenAddr,
		Masters:           *mqKafkaGatewayOptions.master,
		FilerGroup:        *mqKafkaGatewayOptions.filerGroup,
		SchemaRegistryURL: *mqKafkaGatewayOptions.schemaRegistryURL,
		DefaultPartitions: int32(*mqKafkaGatewayOptions.defaultPartitions),
	})

	glog.Warningf("EXPERIMENTAL FEATURE: MQ Kafka Gateway is experimental and should NOT be used in production environments. It currently supports only a minimal subset of Kafka protocol for development purposes.")

	// Show bind vs advertised addresses for clarity
	if bindIP != *mqKafkaGatewayOptions.ip {
		glog.V(0).Infof("Starting MQ Kafka Gateway: binding to %s, advertising %s:%d to clients",
			listenAddr, *mqKafkaGatewayOptions.ip, *mqKafkaGatewayOptions.port)
	} else {
		glog.V(0).Infof("Starting MQ Kafka Gateway on %s", listenAddr)
	}
	glog.V(0).Infof("Using SeaweedMQ brokers from masters: %s", *mqKafkaGatewayOptions.master)

	// Start HTTP profiling server if enabled
	if *mqKafkaGatewayOptions.pprofPort > 0 {
		go func() {
			pprofAddr := fmt.Sprintf(":%d", *mqKafkaGatewayOptions.pprofPort)
			glog.V(0).Infof("Kafka Gateway pprof server listening on %s", pprofAddr)
			glog.V(0).Infof("Access profiling at: http://localhost:%d/debug/pprof/", *mqKafkaGatewayOptions.pprofPort)
			if err := http.ListenAndServe(pprofAddr, nil); err != nil {
				glog.Errorf("pprof server error: %v", err)
			}
		}()
	}

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
