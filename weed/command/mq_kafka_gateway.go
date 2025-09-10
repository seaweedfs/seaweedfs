package command

import (
    "github.com/seaweedfs/seaweedfs/weed/glog"
    "github.com/seaweedfs/seaweedfs/weed/mq/kafka/gateway"
)

var (
    mqKafkaGatewayOptions mqKafkaGatewayOpts
)

type mqKafkaGatewayOpts struct {
    listen *string
}

func init() {
    cmdMqKafkaGateway.Run = runMqKafkaGateway
    mqKafkaGatewayOptions.listen = cmdMqKafkaGateway.Flag.String("listen", ":9092", "Kafka gateway listen address")
}

var cmdMqKafkaGateway = &Command{
    UsageLine: "mq.kafka.gateway [-listen=:9092]",
    Short:     "start a Kafka wire-protocol gateway for SeaweedMQ",
    Long: `Start a Kafka wire-protocol gateway translating Kafka client requests to SeaweedMQ.

This is experimental and currently supports a minimal subset for development.
`,
}

func runMqKafkaGateway(cmd *Command, args []string) bool {
    srv := gateway.NewServer(gateway.Options{
        Listen: *mqKafkaGatewayOptions.listen,
    })

    glog.V(0).Infof("Starting MQ Kafka Gateway on %s", *mqKafkaGatewayOptions.listen)
    if err := srv.Start(); err != nil {
        glog.Fatalf("mq kafka gateway start: %v", err)
        return false
    }
    // Serve blocks until closed
    if err := srv.Wait(); err != nil {
        glog.Errorf("mq kafka gateway wait: %v", err)
        return false
    }
    return true
}


