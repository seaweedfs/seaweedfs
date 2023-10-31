package shell

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"io"
)

func init() {
	Commands = append(Commands, &commandMqTopicCreate{})
}

type commandMqTopicCreate struct {
}

func (c *commandMqTopicCreate) Name() string {
	return "mq.topic.create"
}

func (c *commandMqTopicCreate) Help() string {
	return `create a topic with a given name

	Example:
		mq.topic.create -namespace <namespace> -topic <topic_name> -partition_count <partition_count>
`
}

func (c *commandMqTopicCreate) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {

	// parse parameters
	mqCommand := flag.NewFlagSet(c.Name(), flag.ContinueOnError)
	namespace := mqCommand.String("namespace", "", "namespace name")
	topicName := mqCommand.String("topic", "", "topic name")
	partitionCount := mqCommand.Int("partitionCount", 6, "partition count")
	if err := mqCommand.Parse(args); err != nil {
		return err
	}

	// find the broker balancer
	brokerBalancer, err := findBrokerBalancer(commandEnv)
	if err != nil {
		return err
	}
	fmt.Fprintf(writer, "current balancer: %s\n", brokerBalancer)

	// create topic
	return pb.WithBrokerGrpcClient(false, brokerBalancer, commandEnv.option.GrpcDialOption, func(client mq_pb.SeaweedMessagingClient) error {
		resp, err := client.ConfigureTopic(context.Background(), &mq_pb.ConfigureTopicRequest{
			Topic: &mq_pb.Topic{
				Namespace: *namespace,
				Name:      *topicName,
			},
			PartitionCount: int32(*partitionCount),
		})
		if err != nil {
			return err
		}
		output, _ := json.MarshalIndent(resp, "", "  ")
		fmt.Fprintf(writer, "response:\n%+v\n", string(output))
		return nil
	})

}
