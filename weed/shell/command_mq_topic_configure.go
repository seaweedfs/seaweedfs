package shell

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/schema_pb"
	"io"
)

func init() {
	Commands = append(Commands, &commandMqTopicConfigure{})
}

type commandMqTopicConfigure struct {
}

func (c *commandMqTopicConfigure) Name() string {
	return "mq.topic.configure"
}

func (c *commandMqTopicConfigure) Help() string {
	return `configure a topic with a given name

	Example:
		mq.topic.configure -namespace <namespace> -topic <topic_name> -partition_count <partition_count>
`
}

func (c *commandMqTopicConfigure) HasTag(CommandTag) bool {
	return false
}

func (c *commandMqTopicConfigure) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {

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
			Topic: &schema_pb.Topic{
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
