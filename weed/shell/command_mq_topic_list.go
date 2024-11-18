package shell

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/mq/pub_balancer"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/filer_pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/mq_pb"
	"io"
)

func init() {
	Commands = append(Commands, &commandMqTopicList{})
}

type commandMqTopicList struct {
}

func (c *commandMqTopicList) Name() string {
	return "mq.topic.list"
}

func (c *commandMqTopicList) Help() string {
	return `print out all topics`
}

func (c *commandMqTopicList) HasTag(CommandTag) bool {
	return false
}

func (c *commandMqTopicList) Do(args []string, commandEnv *CommandEnv, writer io.Writer) error {

	brokerBalancer, err := findBrokerBalancer(commandEnv)
	if err != nil {
		return err
	}
	fmt.Fprintf(writer, "current balancer: %s\n", brokerBalancer)

	return pb.WithBrokerGrpcClient(false, brokerBalancer, commandEnv.option.GrpcDialOption, func(client mq_pb.SeaweedMessagingClient) error {
		resp, err := client.ListTopics(context.Background(), &mq_pb.ListTopicsRequest{})
		if err != nil {
			return err
		}
		if len(resp.Topics) == 0 {
			fmt.Fprintf(writer, "no topics found\n")
			return nil
		}
		for _, topic := range resp.Topics {
			fmt.Fprintf(writer, "  %+v\n", topic)
		}
		return nil
	})
}

func findBrokerBalancer(commandEnv *CommandEnv) (brokerBalancer string, err error) {
	err = commandEnv.WithFilerClient(false, func(client filer_pb.SeaweedFilerClient) error {
		resp, err := client.FindLockOwner(context.Background(), &filer_pb.FindLockOwnerRequest{
			Name: pub_balancer.LockBrokerBalancer,
		})
		if err != nil {
			return fmt.Errorf("FindLockOwner: %v", err)
		}
		brokerBalancer = resp.Owner
		return nil
	})
	return
}
