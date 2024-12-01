package shell

import (
	"context"
	"flag"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"io"
)

func init() {
	Commands = append(Commands, &commandMqTopicDescribe{})
}

type commandMqTopicDescribe struct {
}

func (c *commandMqTopicDescribe) Name() string {
	return "mq.topic.describe"
}

func (c *commandMqTopicDescribe) Help() string {
	return `describe a topic`
}

func (c *commandMqTopicDescribe) HasTag(CommandTag) bool {
	return false
}

func (c *commandMqTopicDescribe) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {
	// parse parameters
	mqCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	namespace := mqCommand.String("namespace", "", "namespace name")
	topicName := mqCommand.String("topic", "", "topic name")
	if err := mqCommand.Parse(args); err != nil {
		return err
	}

	// find the broker balancer
	brokerBalancer, err := findBrokerBalancer(commandEnv)
	if err != nil {
		return err
	}
	fmt.Fprintf(writer, "current balancer: %s\n", brokerBalancer)

	return pb.WithBrokerGrpcClient(false, brokerBalancer, commandEnv.option.GrpcDialOption, func(client mq_pb.SeaweedMessagingClient) error {
		resp, err := client.LookupTopicBrokers(context.Background(), &mq_pb.LookupTopicBrokersRequest{
			Topic: &schema_pb.Topic{
				Namespace: *namespace,
				Name:      *topicName,
			},
		})
		if err != nil {
			return err
		}
		for _, assignment := range resp.BrokerPartitionAssignments {
			fmt.Fprintf(writer, "  %+v\n", assignment)
		}
		return nil
	})
}
