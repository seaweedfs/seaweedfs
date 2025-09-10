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
    seaweedMode  *bool
}

func init() {
    cmdMqKafkaGateway.Run = runMqKafkaGateway
    mqKafkaGatewayOptions.listen = cmdMqKafkaGateway.Flag.String("listen", ":9092", "Kafka gateway listen address")
    mqKafkaGatewayOptions.agentAddress = cmdMqKafkaGateway.Flag.String("agent", "", "SeaweedMQ Agent address (e.g., localhost:17777)")
    mqKafkaGatewayOptions.seaweedMode = cmdMqKafkaGateway.Flag.Bool("seaweedmq", false, "Use SeaweedMQ backend instead of in-memory stub")
}

var cmdMqKafkaGateway = &Command{
    UsageLine: "mq.kafka.gateway [-listen=:9092] [-agent=localhost:17777] [-seaweedmq]",
    Short:     "start a Kafka wire-protocol gateway for SeaweedMQ",
    Long: `Start a Kafka wire-protocol gateway translating Kafka client requests to SeaweedMQ.

By default, uses an in-memory stub for development and testing.
Use -seaweedmq -agent=<address> to connect to a real SeaweedMQ Agent for production.

This is experimental and currently supports a minimal subset for development.
`,
}

func runMqKafkaGateway(cmd *Command, args []string) bool {
    // Validate options
    if *mqKafkaGatewayOptions.seaweedMode && *mqKafkaGatewayOptions.agentAddress == "" {
        glog.Fatalf("SeaweedMQ mode requires -agent address")
        return false
    }

    srv := gateway.NewServer(gateway.Options{
        Listen:       *mqKafkaGatewayOptions.listen,
        AgentAddress: *mqKafkaGatewayOptions.agentAddress,
        UseSeaweedMQ: *mqKafkaGatewayOptions.seaweedMode,
    })

    mode := "in-memory"
    if *mqKafkaGatewayOptions.seaweedMode {
        mode = "SeaweedMQ (" + *mqKafkaGatewayOptions.agentAddress + ")"
    }
    glog.V(0).Infof("Starting MQ Kafka Gateway on %s with %s backend", *mqKafkaGatewayOptions.listen, mode)
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


